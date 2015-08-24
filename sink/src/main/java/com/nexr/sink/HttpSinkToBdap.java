package com.nexr.sink;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.security.UserGroupInformation;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.security.PrivilegedExceptionAction;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HttpSinkToBdap extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(HttpSinkToBdap.class);

    private String[] sourceDirs;
    private String agentServer;
    private String uploadPath;
    private String targetUrl;
    private String sinkUrl;
    private int errorCheckInterval;
    private int errorBackoffTries;
    private String uploaderStartCommand;
    private String uploaderStopCommand;
    public static String DEFAULT_URL = "http://localhost:19191/collector";
    public static int RETRY_DEFAULT = 3;
    public static String COMPLETED = ".COMPLETED";

    //minutes
    private static int SINK_ROLL_TIME = 10;

    private static final String HDFS_EVENT_PATTERN = "(.*(Creating hdfs|Closing hdfs|Closing idle|Renaming hdfs).*)";

    private ScheduledExecutorService timedRollerPool;

    private CollectInforWriter collectInforWriter;
    private Map<CollectInfoBean, Integer> retryMap = new HashMap<CollectInfoBean, Integer>();

    private Map<String, List<String>> hdfsErrors = new ConcurrentHashMap<String, List<String>>();

    @Override
    public void configure(Context context) {
        targetUrl = context.getString("target.url", DEFAULT_URL);
        sinkUrl = targetUrl + "/sink";
        agentServer = context.getString("uploader.server", "localhost");
        sourceDirs = context.getString("uploader.spooldir", "").split(",");
        uploadPath = context.getString("uploader.uploadpath", "");
        errorCheckInterval = context.getInteger("hdfserror.checkInterval", 30 * 60); // seconds, 30 minutes
        errorBackoffTries = context.getInteger("hdfserror.backofftries", 10 * 60); // seconds, 10 minutes
        uploaderStartCommand = context.getString("uploader.start", "");
        uploaderStopCommand = context.getString("uploader.stop", "");
    }

    @Override
    public void start() {
        logger.info("Starting {} ....", this);

        try {
            logger.info("targetUrl = {}", targetUrl);
            logger.info("agentServer = {}", agentServer);
            for (String dir: sourceDirs) {
                logger.info("source dir = " + dir);
            }
            logger.info("uploadPath = {}", uploadPath);

            collectInforWriter = new CollectInforWriter();

            String rollerName = "httpCollectInfo-" + getName() + "-roll-timer-%d";
            logger.info("sink roll period = {} ", SINK_ROLL_TIME);
            timedRollerPool = Executors.newScheduledThreadPool(5, new ThreadFactoryBuilder().setNameFormat(rollerName).build());


            timedRollerPool.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        updateCollectInfoStatus();
                    } catch (Throwable t) {
                        logger.error("Fail to roll the Collect info", t);
                    }
                }
            }, getInitalDelay(), SINK_ROLL_TIME * 60, TimeUnit.SECONDS);


            validateTargetURL(sinkUrl);

            timedRollerPool.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    checkErrors();
                }
            }, 20, errorCheckInterval, TimeUnit.SECONDS);

            recovery();

        } catch (Exception e) {
            logger.info("TargetURL is not valid, http request will not happen.");
        }
        logger.info("HttpSink {} started. !!!!!! ", getName());
    }

    @Override
    public void stop() {
        collectInforWriter.stop();
        collectInforWriter = null;

        timedRollerPool.shutdown();

        logger.debug("Stop method. Closed connection");

        return;
    }
    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        // Start transaction
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        try {
            Event event = ch.take();
            //String message = URLEncoder.encode(new String(event.getBody()), "UTF-8");
            String eventBody = new String(event.getBody());  //URLEncoder

            String collectServer = event.getHeaders().get("hostname");

            processEvent(collectServer, eventBody);

            txn.commit();
            status = Status.READY;
        } catch (Throwable t) {
            txn.rollback();

            logger.debug("Sending Fail, BACKOFF the event : " + t.getMessage());

            status = Status.BACKOFF;

            // re-throw isUploded Errors
            if (t instanceof Error) {
                throw (Error) t;
            }
        } finally {
            txn.close();
        }
        return status;
    }

    private void processEvent(String collectorServer, String message) throws Exception {
        logger.info("MESSAGE BODY {} " + message);
        if (Pattern.compile(HDFS_EVENT_PATTERN).matcher(message).find()) {
            addFileEvent(collectorServer, message);
        } else if (getSpoolDirFatal(message) != null) {
            handleSpooldirFatal(message);
        } else if (getHDFSIOErrorPath(message) != null) {
            handleHDFSIOError(message);
        } else if (getFileFailedToCloseFinal(message) != null) {
            handleFailClose(message);
        } else if (passEvent(message)) {
            // nothing to do
        } else {
            logger.info("Unknown pattern message, {}", message);
        }
    }

    private void handleSpooldirFatal(String message) {
        String spoolDir = getSpoolDirFatal(message);

        logger.info("handle Spool Directory Fatal [{}]", spoolDir);
        restartAgents();
    }

    private void handleHDFSIOError(String message) {
        // handle Backoff process
        final String tmpFilePath = getHDFSIOErrorPath(message);

        logger.info("handle HDFSIOError [{}], \npath : [{}] \n", message, tmpFilePath);
        if (tmpFilePath == null || !tmpFilePath.endsWith(".tmp")) {
            logger.warn("FilePath should be tmp file");
            return;
        }
        addError(tmpFilePath, message);
    }

    private void handleFailClose(String message) {
        String filePath = getFileFailedToCloseFinal(message);

        logger.info("handle FailClose [{}], \n {} \n", filePath, message);
        if (filePath == null || !filePath.endsWith(".tmp")) {
            logger.warn("FilePath should be tmp file");
            return;
        }

        Path path = new Path(filePath);
        Configuration conf = new Configuration();
        try {
            // delete and re-upload
            List<String> paths = cleanupHDFS(path);
            if (paths.size() == 1) {
                reupload(paths);
                removeFileEvent(filePath);
            } else {
                logger.error("Cleanup file count should be <<1>> but <<{}>>, check manually", paths.size());
                addError(filePath, message);
            }

        } catch (Throwable e) {
            logger.error("Fail to handle FailClose " + filePath + ". Check the HDFS and delete", e);
        }
    }

    private void addError(String uploadPath, String message) {
        logger.warn("Add Error {}, {}", uploadPath, message);
        if (!uploadPath.endsWith(".tmp")) {
            logger.warn("UploadPath in error is not tmp");
            return;
        }
        List<String> errors = hdfsErrors.get(uploadPath);
        if (errors == null) {
            errors = new ArrayList<String>();
            hdfsErrors.put(uploadPath, errors);
        }
        errors.add(message);
        logger.warn("error count, {}", errors.size());
    }

    private void checkErrors() {

        logger.info("hdfs errors, {}", hdfsErrors.size());

        Set<String> paths = new HashSet<String>();
        paths.addAll(hdfsErrors.keySet());

        boolean restart = false;
        for(String path: paths) {
            List<String> messages = hdfsErrors.get(path);
            logger.info("error path {}, tries {} ", path, messages.size());
            if (messages.size() <= errorBackoffTries) {
                try {
                    ScheduledFuture<Boolean> recoved =
                            timedRollerPool.schedule(createErrorHandleCallable(path), 100, TimeUnit.MILLISECONDS);

                    logger.warn("recoverd {}", recoved.get().toString());
                    if (recoved.get().booleanValue()) {
                        hdfsErrors.remove(path);
                    }
                } catch (Exception e) {
                    logger.error("Fail to checkError", e);
                }
            } else {
                logger.error("BackOff may fail. Need to check manually and restart uploader.");
                hdfsErrors.remove(path);
                restart = true;
            }
        }
        if (restart) {
            restartAgents();
        }
    }

    private void restartAgents() {
        logger.warn("!! Restart agents ......");
        try {
            logger.info("Stop uploader [{}] ......", uploaderStopCommand);
            Process process = Runtime.getRuntime().exec(uploaderStopCommand);

            StreamDump sg = new StreamDump(process.getInputStream(), "uploader-stop");
            sg.start();

            int exitValue = process.waitFor();
            logger.info("uploader stop process exit : " + exitValue);

            logger.info("Restart umonitor in 15 seconds ......");
            Thread.sleep(15000);
            hdfsErrors.clear();
            collectInforWriter.clear();
            recovery();
            Thread.sleep(15000);

            logger.info("Start uploader [{}] ......", uploaderStartCommand);

            process = Runtime.getRuntime().exec(uploaderStartCommand);

            sg = new StreamDump(process.getInputStream(), "uploader-start");
            sg.start();

            exitValue = process.waitFor();
            logger.info("uploader start process exit : " + exitValue);

        } catch (Exception e) {
            logger.error("Fail to restartAgents. Restart agents manually !!! ", e);
        }
    }

    class StreamDump extends Thread {
        InputStream is;
        String type;

        StreamDump(InputStream is, String type)
        {
            this.is = is;
            this.type = type;
        }

        public void run()
        {
            try
            {
                InputStreamReader isr = new InputStreamReader(is);
                BufferedReader br = new BufferedReader(isr);
                String line=null;
                while ( (line = br.readLine()) != null)
                    logger.info(type + "> " + line);
            } catch (IOException ioe)
            {
                ioe.printStackTrace();
            }
        }
    }

    private Callable<Boolean> createErrorHandleCallable(String uploadPath) {

        final String tmpFilePath = uploadPath;

        return new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                logger.info("handle error start {}", tmpFilePath);
                boolean recovered = true;
                Path path = new Path(tmpFilePath);
                try {
                    FileStatus[] fileStatuses = getSibling(path);
                    if (fileStatuses.length == 0) {
                        logger.error("No error file !! Check manually!");
                    } else {
                        String hdfsFileComplete = tmpFilePath.substring(0, tmpFilePath.indexOf(".tmp")); ///a.dat.1.1400111111
                        for (FileStatus fileStatus: fileStatuses) {
                            logger.info("sibling : " + fileStatus.getPath().toString());
                            if (fileStatus.getPath().toString().equals(hdfsFileComplete)) {
                                logger.info("error file recovered !! {}", fileStatus.getPath());
                            } else if (fileStatus.getPath().toString().equals(tmpFilePath)) {
                                recovered = false;
                                logger.error("error file still tmp and have sibling !! {}", fileStatus.getPath());
                            } else {
                                logger.info("error file have sibling file !! {}", fileStatuses[0].getPath());
                            }
                        }
                    }

                    if (!recovered) {
                        logger.error("error file still tmp and check manually!!! May need to restart!!");
                    }

                } catch (Throwable e) {
                    logger.error("Fail to handle error file " + tmpFilePath + ". Check the HDFS and delete", e);
                }
                return Boolean.valueOf(recovered);
            }
        };
    }

    /**
     *
     * @param filePath ex> hdfs://kdap-nn:9000/user/ndap/repository/user/admin/cdr_txt/kdap319/S1AP/0630/daejeon_LOG_S1AP_201505200630.dat.1.1432071465999.tmp
     */
    private void removeFileEvent(String filePath) throws Exception{
        String[] paths = extract(filePath);

        //modify by kkw
        long timestamp  = parseTime(paths[0]);
        String hhmm = paths[1];
        String type = paths[2];
        FileEvent a = new FileEvent("", type, hhmm, filePath, "Creating",timestamp, "");

        collectInforWriter.removeFileEvent(a);
    }

    private FileSystem createFileSystem(String user, final URI uri, final Configuration conf)
            throws Exception {

        String nameNode = uri.getAuthority();

        UserGroupInformation ugi = getProxyUser(user);
        return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
            public FileSystem run() throws Exception {
                return FileSystem.get(uri, conf);
            }
        });
    }

    private UserGroupInformation getProxyUser(String user) throws IOException {
        return UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
    }

    /**
     *
     * @param message
     * @return ex>hdfs://kdap-nn:9000/kdap319/S1AP/0630/daejeon_LOG_S1AP_201505200630.dat.1.1432071465999.tmp
     */
    @VisibleForTesting
    String getFileFailedToCloseFinal(String message) {
        // Close retry final (BucketWriter) , "Unsuccessfully attempted to close(1.5.2)"
        String patternString1 = "(Unsuccessfully attempted to)(.*)(hdfs://(.*).tmp)(.*)";

        Pattern pattern = Pattern.compile(patternString1);
        Matcher matcher = pattern.matcher(message);

        if (matcher.find()) {
            return matcher.group(3);
        }
        return null;
    }

    /**
     *
     * @param message
     * @return ex> hdfs://seair:8020/user/Ltas/HTTP/1350/gwangju_LOG_HTTP_2015035.dat.1.1430369477451.tmp
     */
    String getHDFSIOErrorPath(String message) {
        // Handle HDFS IO Error message. BackOff. HDFSEventSink
        if (getHDFSIOErrorPath1(message) != null) {
            return getHDFSIOErrorPath1(message);
        } else if (getHDFSIOErrorPath2(message) != null) {
            return getNameNode(uploadPath) + getHDFSIOErrorPath2(message);
//        } else if (getHDFSIOErrorPath3(message) != null) {
//            return getHDFSIOErrorPath3(message);
        } else {
            return null;
        }
    }

    /**
     * Get the path of HDFS sink which occurs HDFS IO error.
     * @param message
     * @return the path in HDFS. ex> hdfs://seair:8020/user/Ltas/HTTP/1350/gwangju_LOG_HTTP_2015035.dat.1.1430369477451.tmp
     */
    @VisibleForTesting
    String getHDFSIOErrorPath1(String message) {
        String patternString1 = "(HDFS IO error)(.*)(hdfs://(.*).tmp)(.*)";

        Pattern pattern = Pattern.compile(patternString1);
        Matcher matcher = pattern.matcher(message);

        if (matcher.find()) {
            return matcher.group(3);
        }
        return null;
    }

    /**
     * Get the path of HDFS sink which occurs HDFS IO error.
     * @param message
     * @return ex> /user/Ltas/HTTP/1350/gwangju_LOG_HTTP_2015035.dat.1.1430369477451.tmp
     */
    @VisibleForTesting
    String getHDFSIOErrorPath2(String message) {
        String patternString1 = "HDFS IO error([^/]*)(.*.tmp)";
        Pattern pattern = Pattern.compile(patternString1);
        Matcher matcher = pattern.matcher(message);

        if (matcher.find()) {
            return matcher.group(2);
        }
        return null;
    }

    /**
     * Get the path of HDFS sink which occurs HDFS IO error. IOException in BucketWriter cause the HDFS IO error.
     * @param message
     * @return the path in HDFS. ex> hdfs://seair:8020/user/Ltas/HTTP/1350/gwangju_LOG_HTTP_2015035.dat.1.1430369477451.tmp
     */
    @VisibleForTesting
    String getHDFSIOErrorPath3(String message) {
        String patternString1 = "(Caught IOException while closing file)(.*)(hdfs://(.*).tmp)(.*)";

        Pattern pattern = Pattern.compile(patternString1);
        Matcher matcher = pattern.matcher(message);

        if (matcher.find()) {
            return matcher.group(3);
        }
        return null;
    }

    /**
     * Get the path of SpoolDir is in fatal error.
     * @param message
     * @return the path in HDFS. ex> /data1/info/upload1
     */
    @VisibleForTesting
    String getSpoolDirFatal(String message) {
        String patternString1 = "(FATAL:)(.*)(spoolDir: (.*) }: )(Uncaught exception in SpoolDirectorySource thread. Restart or reconfigure Flume to continue processing)(.*)";

        Pattern pattern = Pattern.compile(patternString1);
        Matcher matcher = pattern.matcher(message);

        if (matcher.find()) {
            return matcher.group(4);
        }
        return null;
    }

    @VisibleForTesting
    String getSrcFromHDFSPath(String uploadedPath) {
        String patternString1 = "(hdfs://)(.*)(/.*.dat)(.*)";
        Pattern pattern = Pattern.compile(patternString1);
        Matcher matcher = pattern.matcher(uploadedPath);
        if (matcher.find()) {
            String fileName = matcher.group(3);
            return fileName.substring(1);
        } else {
            patternString1 = "(hdfs://)(.*)(/.*.*)(.*)";
            pattern = Pattern.compile(patternString1);
            matcher = pattern.matcher(uploadedPath);
            if (matcher.find()) {
                String fileName = matcher.group(3);
                String[] ns = fileName.split("\\.");
                String result = "";
                for (int j = 0; j<=ns.length-4; j++) {
                    System.out.println(ns[j]);
                    result += ns[j] + ".";
                }
                return result.substring(1, result.length()-1);
            }
        }

        return null;
    }

    @VisibleForTesting
    String getNameNode(String uploadPath) {
        String patternStr = "(hdfs://)([a-z.\\-:0-9]*)";

        Pattern pattern = Pattern.compile(patternStr);
        Matcher matcher = pattern.matcher(uploadPath);
        if (matcher.find()) {
            return matcher.group(1) + matcher.group(2);
        }
        return null;
    }

    /**
     * Determine pass the event due to no need to handle.
     * @param message
     * @return true if the event pass
     */
    private boolean passEvent(String message) {
        String pattern = "(.*(Writer callback called|Close tries incremented).*)";  //
        Matcher matcher = Pattern.compile(pattern).matcher(message);
        return matcher.find();
    }

    private FileEvent addFileEvent(String collectServer, String message) throws Exception{
        try{
            String pattern = "(.*(Creating hdfs|Closing hdfs|Renaming hdfs).*)";
            Matcher matcher = Pattern.compile(pattern).matcher(message);
            if (matcher.find()) {

                // replace senddate to cluster_date by kkw
                //String time = message.substring(0,23);
                String statusGroup = matcher.group(2);
                int start = matcher.start(2);
                String actionMessage = message.substring(start, message.length());
                String status = statusGroup.substring(0, statusGroup.indexOf("hdfs") -1);
                String path = "";

                // replace senddate to cluster_date by kkw 2015.08.04
                //long timestamp = parseTime(time);

                if (FileEvent.Status.valueOf(status) == FileEvent.Status.Creating
                        || FileEvent.Status.valueOf(status) == FileEvent.Status.Closing) {
                    path = actionMessage.substring(actionMessage.indexOf("hdfs://"), actionMessage.length()).trim();
                } else if (FileEvent.Status.valueOf(status) == FileEvent.Status.Renaming) {
                    path = actionMessage.substring(actionMessage.indexOf("hdfs://"), actionMessage.indexOf(".tmp") + 4).trim();
                }



                String[] paths = extract(path);

                // replace senddate to cluster_date by kkw 2015.08.04
                long timestamp;
                /**
                String patternStringToClstrDate = "(.*)/(\\d{8})(.*)";
                Pattern patternToClstrDate = Pattern.compile(patternStringToClstrDate);
                Matcher matcherToClstrDate = patternToClstrDate.matcher(path);

                if (matcherToClstrDate.find()) {
                    timestamp = parseTime(matcherToClstrDate.group(2));
                }else {
                    timestamp = parseTime(message.substring(0,23));
                }
                 */
                //modify by kkw
                timestamp  = parseTimetoBDAP(paths[0] + "000000000");
                String hhmm = paths[1];
                String type = paths[2];

                FileEvent fileEvent = new FileEvent(collectServer, type, hhmm, path, status, timestamp, actionMessage);
                if (FileEvent.Status.valueOf(status) == FileEvent.Status.Renaming) {
                    fileEvent.setEndTime(timestamp);
                }

                logger.info("fileEvent : " + fileEvent.toString());
                collectInforWriter.addFileEvent(fileEvent);
                if (fileEvent.getStatus() == FileEvent.Status.Renaming) {
                    logger.info("Delete source {} " + fileEvent.getPath() + COMPLETED);
                    removeSrc(fileEvent);
                }
                return fileEvent;
            }
            return null;

        } catch (Exception e) {
            logger.error("Fail to add FileEvent", e);
            return null;
        }
    }

    /**
     *
     * @param filePath ex> hdfs://localhost:8020/xxx/collserver/HTTP/yyyyMMdd/1350/gwangju_LOG_HTTP_2015035.dat.1.1430369477451.tmp
     * @return
     */
    private String[] extract(String filePath) {
        String[] paths = filePath.split("/");
        if (paths.length < 3) {
            return null;
        }

        //modify by kkw
        String clstrDate = paths[paths.length-3];
        String hhmm = paths[paths.length-2];
        String type = paths[paths.length-4];
        return new String[]{clstrDate, hhmm, type};
    }

    public boolean sendCollectInfoMessage(String url, CollectInfoBean collectInfoBean) throws Exception{
        try {
            URL serverURL = new URL(url);
            String collectInfoJson = toJsonString(collectInfoBean);
            logger.debug("send {} to [{}]", collectInfoJson, url);

            HttpURLConnection connection = (HttpURLConnection) serverURL.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setInstanceFollowRedirects( false );
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("charset", "utf-8");
            connection.setRequestProperty("Content-Length", Integer.toString(collectInfoJson.length()));
            connection.setUseCaches(false);

            OutputStreamWriter wr = new OutputStreamWriter(connection.getOutputStream());
            wr.write(collectInfoJson);
            wr.flush();
            wr.close();

            if ((connection.getResponseCode() == HttpURLConnection.HTTP_OK)) {
                logger.debug("Sending message succeeded, result," + connection.getResponseCode());
            } else {
                logger.warn("Sending message failed, result, " + connection.getResponseCode() + ", " +
                        "See targetUrl log(collector.log) : " + url);
                throw new Exception("Respond is not HTTP_OK");
            }
            connection.disconnect();
            return true;
        } catch (Exception e) {
            int count = 0;
            if (retryMap.containsKey(collectInfoBean)) {
                count = retryMap.get(collectInfoBean).intValue();
            }
            if (count < RETRY_DEFAULT) {
                final String sUrl = url;
                final CollectInfoBean collect = collectInfoBean;

                logger.warn("Fail to sendCollectInfoMessage, Retry(" + count + ") will be in 1 minute, Collect info : " + collect + " , " + url);
                timedRollerPool.schedule(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            sendCollectInfoMessage(sUrl, collect);
                        } catch (Exception e) {
                            logger.warn("Fail to sendCollectInfoMessage (retry). Collect info : " + collect.toString(), e);
                        }
                    }
                }, 1, TimeUnit.MINUTES);
                retryMap.put(collectInfoBean, new Integer(count++));
            } else {
               throw e;
            }
            return false;
        }
    }

    public JSONArray getCollectInfoStatus(String url) throws Exception{
        JSONArray jsonArray = null;
        try {
            URL serverURL = new URL(url);
            HttpURLConnection connection = (HttpURLConnection) serverURL.openConnection();
            connection.setRequestMethod("GET");
            connection.setInstanceFollowRedirects(false);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("charset", "utf-8");
            connection.setUseCaches(false);

            if ((connection.getResponseCode() == HttpURLConnection.HTTP_OK)) {
                Reader reader = new InputStreamReader(connection.getInputStream());
                jsonArray = (JSONArray) JSONValue.parse(reader);

            } else {
                logger.warn("getCollectInfoStatus failed, result, " + connection.getResponseCode() + ", " +
                        "See targetUrl log(collector.log) : " + url);
                throw new Exception("Respond is not HTTP_OK");
            }
            connection.disconnect();
            return jsonArray;
        } catch (Exception e) {
            logger.warn("getCollectInfoStatus from [{}] fail", url);
            throw e;
        }
    }

    private void syncCollectInfo(JSONArray jsonArray) {
        logger.info("==== Start recovery Collect Info [{}] ", jsonArray.size());
        for (Object obj : jsonArray) {
            JSONObject jsonObject = (JSONObject) obj;
            String jsonStr = jsonObject.toJSONString();

            String collserver = jsonObject.get("collserver").toString();
            String dirname = jsonObject.get("dirname").toString();
            String senddate = jsonObject.get("senddate").toString();
            String hm = jsonObject.get("hm").toString();
            String hadoopload = jsonObject.get("hadoopload").toString();

            if (agentServer.endsWith(collserver)) {
                logger.info("-- Start sync Collect Info : " + collserver +", " + dirname + ", " + senddate + ", " + hm);
                try{
                    //modify by kkw
                    Path path = parseUploadPath(collserver, dirname, hm, senddate);
                    List<String> deletedFiles = cleanupHDFS(path);
                    if (reupload(deletedFiles)) {
                        logger.info("Tmp files are clean up and re-upload !!! [{}]", path.toString());
                        if (getDatFileCount(path) == 0) {
                            CollectInfoBean collectInfoBean = new CollectInfoBean(collserver, dirname, senddate, hm, CollectInfoBean.Status.DELETE);
                            sendCollectInfoMessage(sinkUrl, collectInfoBean);
                            logger.info("No files in Collect Info. Delete in DB : " + collectInfoBean.toString());
                        } else {
                            Calendar current = Calendar.getInstance();
                            String currentDate = formatTime(current.getTimeInMillis(), "yyyyMMdd");
                            String currentHm = formatTime(current.getTimeInMillis(), "HHmm");
                            currentHm = currentHm.substring(0,3) + "0";
                            if (parseTime(senddate + hm, "yyyyMMddHHmm") < parseTime(currentDate + currentHm, "yyyyMMddHHmm") ) {
                                // Collect Info is before than current
                                CollectInfoBean collectInfoBean = new CollectInfoBean(collserver, dirname, senddate, hm, CollectInfoBean.Status.END);
                                sendCollectInfoMessage(sinkUrl, collectInfoBean);
                                logger.info("Update Collect Info in DB : " + collectInfoBean.toString());
                            } else {
                                CollectInfoBean collectInfoBean = new CollectInfoBean(collserver, dirname, senddate, hm, CollectInfoBean.Status.END);
                                collectInforWriter.addCollectInfo(collectInfoBean);
                                logger.info("Add Collect Info : " + collectInfoBean.toString());
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.error("Fail to sync : " + jsonStr, e);
                }
                logger.info("-- End sync Collect Info");
            }
        }

    }

    /**
     * Deletes tmp files in HDFS
     * @param path
     * @return
     * @throws Exception
     */
    private List<String> cleanupHDFS(Path path) throws Exception{
        try {
            List<String> files = new ArrayList<String>();

            Configuration conf = new Configuration();
            FileSystem fs = createFileSystem(System.getProperty("user.name"), path.toUri(), conf);
            if (fs.isDirectory(path)) {
                FileStatus[] fileStatuses = getFileStatus(path, ".tmp", true);
                logger.info("Tmp files {}, in [{}]", fileStatuses.length, path.toUri());
                for (FileStatus child: fileStatuses) {
                    Path childPath = child.getPath();
                    if (fs.delete(childPath)) {
                        files.add(childPath.toString());
                        logger.info("delete {}", childPath);
                    } else {
                        logger.error("fail to delete {}", childPath);
                    }
                }
            } else if (fs.isFile(path)) {
                logger.info("try to delete {}", path);
                if (path.toString().endsWith(".tmp")) {
                    fs.delete(path);
                    if (fs.delete(path)) {
                        files.add(path.toString());
                        logger.info("delete {}", path);
                    } else {
                        logger.error("fail to delete {}", path);
                    }
                }
            } else {
                logger.warn("Path exists: [{}], {}", path, fs.exists(path));
                if (fs.delete(path)) {
                    logger.warn("delete {}", path);
                }
            }
            return files;

        } catch (Throwable e) {
            throw new Exception("Fail to delete tmp files in [" + path + "]. Check the HDFS and delete", e);
        }
    }

    private boolean reupload(List<String> deletedInHDFS) {
        Set<String> srcFiles = new HashSet<String>();
        for (String file: deletedInHDFS) {
            String srcFile = getSrcFromHDFSPath(file);
            if (srcFile !=null) {
                srcFiles.add(srcFile);
                logger.info("srcFile of deleted tmp file : " + srcFile);
            }
        }

        logger.info("try to re-upload files : " + srcFiles.size());
        if (srcFiles.size() == 0) return true;

        for (String dir: sourceDirs) {
            String[] children = new File(dir).list();
            for (String child: children) {
                File f = new File(dir, child);
                if (f.getName().endsWith(COMPLETED)) {
                    File renameFile = new File(f.getAbsolutePath().substring(0, f.getAbsolutePath().indexOf(COMPLETED)));
                    if (srcFiles.contains(renameFile.getName())) {
                        if (f.renameTo(renameFile)) {
                            logger.info("Re-upload [{}]", renameFile.getAbsolutePath());
                            srcFiles.remove(renameFile.getName());
                        } else {
                            logger.error("Fail to rename {}", f.getAbsolutePath());
                        }
                    }
                } else {
                    if (srcFiles.contains(f.getName())) {
                        logger.info("Re-upload not needed. Source file exist [{}]", f.getAbsolutePath());
                        srcFiles.remove(f.getName());
                    }
                }
            }
        }

        if (srcFiles.size() != 0) {
            for (String file: srcFiles) {
                logger.error("Tmp file in HDFS is clean up, but fail to re-upload source file {}, " +
                        "check the source backup directory !!! \n", file);
            }
        }
        return srcFiles.size() == 0;
    }

    private boolean reuploadAll() {
        logger.info("Start re-upload all COMPLETED files");
        boolean result = true;
        for (String dir: sourceDirs) {
            String[] children = new File(dir).list(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith(COMPLETED);
                }
            });
            if (children != null) {
                for (String child : children) {
                    File f = new File(dir, child);
                    File renameFile = new File(f.getAbsolutePath().substring(0, f.getAbsolutePath().indexOf(COMPLETED)));
                    if (f.renameTo(renameFile)) {
                        logger.info("Re-upload [{}]", renameFile.getAbsolutePath());
                    } else {
                        logger.error("Fail to rename [{}], check in HDFS and rename manually!!!", f.getAbsolutePath());
                        result = false;
                    }
                }
            }
            return false;
        }
        return result;
    }

    /**
     * Returns the filestatus of which source file is same one.
     * <p>
     *     getSibling('hdfs://nn:8020/dir/a.dat.1.tmp')
     *     dir/a.dat.1
     *     dir/a.dat.2
     *     dir/a.dat.3
     * </p>
     * @param path
     * @return
     * @throws Exception
     */
    @VisibleForTesting
    FileStatus[] getSibling(Path path) throws Exception{
        try {
            FileStatus[] status = null;
            final Path uploadPath = path;
            final String sourceFile = getSrcFromHDFSPath(uploadPath.toString());
            if (sourceFile != null) {
                Path dir = new Path(uploadPath.toString().substring(0, uploadPath.toString().indexOf(sourceFile)));
                logger.info("getSibling [{}], file [{}]", dir.toString(), sourceFile);
                Configuration conf = new Configuration();
                FileSystem fs = createFileSystem(System.getProperty("user.name"), dir.toUri(), conf);
                if (fs.isDirectory(dir)) {
                    FileStatus[] fileStatuses = fs.listStatus(dir, new PathFilter() {
                        @Override
                        public boolean accept(Path path) {
                            logger.info("- path : " + path);
                            return path.toString().contains(sourceFile);
                        }
                    });
                    status = fileStatuses;
                }
            }
            return status;

        } catch (Throwable e) {
            throw new Exception("Fail to get .dat files in [" + path + "]. Check the HDFS.", e);
        }
    }

    public String sendJson(String url, String method) throws Exception{
        String data = null;
        try {
            URL serverURL = new URL(url);
            logger.info("url : " + url);

            HttpURLConnection connection = (HttpURLConnection) serverURL.openConnection();
            connection.setRequestMethod(method);
            connection.setInstanceFollowRedirects(false);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("charset", "utf-8");
            connection.setUseCaches(false);

            if ((connection.getResponseCode() == HttpURLConnection.HTTP_OK)) {
                logger.debug("Sending message succeeded, result," + connection.getResponseCode());
                StringBuilder builder = new StringBuilder();
                BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                String line = null;
                while ( (line=reader.readLine()) !=null) {
                    builder.append(line);
                }
                logger.info("-- builder : " + builder);
                JSONArray array = new JSONArray();


            } else {
                logger.warn("Sending message failed, result, " + connection.getResponseCode() + ", " +
                        "See targetUrl Server log");
                throw new Exception("Respond is not HTTP_OK");
            }
            connection.disconnect();
        } catch (Exception e) {
            throw e;
        }
        return data;
    }

    private String getHostName() {
        InetAddress addr;
        String host = "localhost";
        try {
            addr = InetAddress.getLocalHost();
            host = addr.getCanonicalHostName();
        } catch (UnknownHostException e) {
            logger.warn("Could not get local host address. Exception follows.", e);
        }
        return host;
    }

    private String toJsonString(CollectInfoBean collectInfoBean) {
        return collectInfoBean.toJsonObject().toString();
    }

    public void updateCollectInfoStatus() throws ParseException{
        List<CollectInfoBean> collectorInfoList = new ArrayList<CollectInfoBean>();
        collectorInfoList.addAll(collectInforWriter.getCollectInfoBeans());
        if (collectorInfoList == null && collectorInfoList.size() <0 ) {
            logger.info("No Collect Info exists");
            return;
        }
        StringBuilder builder = new StringBuilder();
        for (CollectInfoBean collectInfoBean : collectorInfoList) {
            builder.append(collectInfoBean.getKey() + ",");
        }
        logger.info("Collect Info list [ " + builder.toString() + " ]");
        List<CollectInfoBean> updateList = new ArrayList<CollectInfoBean>();
        for (CollectInfoBean collectInfoBean : collectorInfoList) {
            long eventTime = parseTime(collectInfoBean.getSenddate() + " " + collectInfoBean.getHm(), "yyyyMMdd hhmm");

            if (System.currentTimeMillis() - eventTime > 0 ) {
                if (collectInfoBean.updateStatus() == CollectInfoBean.Status.END) {
                    // TODO not really updateStatus. Collect Info List is copied.
                    if (checkTmpFile(collectInfoBean) > 0) {
                        logger.info("Collect Info[{}] has tmp files. retry in next rolling.", collectInfoBean.getKey());
                    } else {
                        updateList.add(collectInfoBean);
                        logger.info("Collect Info[{}] will be updated.", collectInfoBean.getKey());
                    }
                } else {
                    logger.info("Collect Info[{}] has un-finished file event \n, {} ", collectInfoBean.getKey(), collectInfoBean.getFiles().values());
                }
            } else {
                logger.warn("Should not happen this. Collect Info is after than current ? : " + collectInfoBean.toString());
            }
        }

        // TODO Need queue and async?
        logger.info("Update Collect Info count : " + updateList.size());
        for(CollectInfoBean event : updateList) {
            try {
                if (sendCollectInfoMessage(sinkUrl, event) ) {
                    boolean removed = collectInforWriter.removeCollectInfo(event.getKey());
                    logger.info("update succeed !! remove from queue {} {} ", event.getKey(), removed);
                } else {
                    logger.info("retrying ......");
                }

            } catch (Exception e) {
                logger.warn("Fail to update Collect Info : " + event.toString(), e);
            }
        }
        logger.info("Finish updateCollectInfoStatus");
    }

    /**
     *
     * Check there is .timp file in HDFS
     * @param collectInfo
     */
    private int checkTmpFile(CollectInfoBean collectInfo) {
        //modify by kkw
        Path path = parseUploadPath(collectInfo.getCollserver(), collectInfo.getDirname(), collectInfo.getHm(), collectInfo.getSenddate());
        try {
            FileStatus[] status = getFileStatus(path, ".tmp", true);
            if (status != null) {
                logger.info("---- checkTmpFile size {}", status.length);
                for (FileStatus child : status) {
                    Path childPath = child.getPath();
                    logger.warn("Tmp file : " + childPath.toUri().toString());
                }
                return status.length;
            }
        } catch (Exception e) {
            logger.warn("Fail to search tmp files in [" + path + "]. Check the HDFS and delete", e);
        }
        return 0;
    }

    private int getDatFileCount(Path path) throws Exception{
        // not .tmp files
        FileStatus[] status = getFileStatus(path, ".tmp", false);
        return status == null ? 0 : status.length;
    }

    /**
     * Gets the FileStatus corresponding conditions.
     * @param dirPath parentDir
     * @param suffix the suffix
     * @param includeSuffix if extension is endwith <tt>suffix</tt>
     * @return
     * @throws Exception
     */
    @VisibleForTesting
    FileStatus[] getFileStatus(Path dirPath, String suffix, boolean includeSuffix) throws Exception{
        try {
            final boolean include = includeSuffix;
            final String extension = suffix;
            FileStatus[] statuses = null;
            Configuration conf = new Configuration();
            FileSystem fs = createFileSystem(System.getProperty("user.name"), dirPath.toUri(), conf);
            if (fs.isDirectory(dirPath)) {
                FileStatus[] fileStatuses = fs.listStatus(dirPath, new PathFilter() {
                    @Override
                    public boolean accept(Path path) {
                        return !(include ^ path.toString().endsWith(extension));
                    }
                });
                statuses = fileStatuses;
            }
            return statuses;

        } catch (Throwable e) {
            throw new Exception("Fail to get .dat files in [" + dirPath + "]. Check the HDFS.", e);
        }

    }

    //modify by kkw
    private Path parseUploadPath(String collserver, String dirname, String hm, String clstrDate) {
        String pathStr = uploadPath;
        pathStr = pathStr.replaceAll("\\%\\{hostname\\}", collserver);
        pathStr = pathStr.replaceAll("\\%\\{datatype\\}", dirname);
        pathStr = pathStr.replaceAll("\\%\\{hmpath\\}", hm);

        pathStr = pathStr.replaceAll("\\%\\{clstr_ymd\\}", clstrDate);
        Path path = new Path(pathStr);
        return path;
    }

    private void removeSrc(FileEvent fileEvent) {
        String srcFileName = getSrcFromHDFSPath(fileEvent.getPath()) + COMPLETED;
        for (String dir: sourceDirs) {
            File file = new File(dir, srcFileName);
            if (file.isFile()) {
                if (file.delete()) {
                    logger.info("delete source {}", file.getAbsolutePath());
                    break;
                } else {
                    logger.info("delete source fail, {}", file.getAbsolutePath());
                }
            }

        }
    }

    private void recovery() {
        try {
            String url = targetUrl + "/monitor";
            JSONArray jsonArray = getCollectInfoStatus(url);
            syncCollectInfo(jsonArray);
            reuploadAll();
            logger.info("==== End recovery Collect info !!!");
        } catch (Exception e) {
            logger.error("recovery fail", e);
        }
    }

    private int getInitalDelay() throws ParseException {
        Calendar current = Calendar.getInstance();
        Calendar startTime = Calendar.getInstance();
        startTime.setTimeInMillis(current.getTimeInMillis() + (1000 * 60 * 10));
        String mm = formatTime(startTime.getTimeInMillis(), "mm");
        startTime.set(Calendar.MINUTE, Integer.parseInt(mm.charAt(0) + "0"));
        startTime.set(Calendar.SECOND, 0);

        long diff = (startTime.getTimeInMillis() - current.getTimeInMillis()) / 1000;
        logger.info("Update Scheduler will start on " + formatTime(startTime.getTimeInMillis()));

        return (int) diff;
    }

    private void validateTargetURL(String targetUrl) throws EventDeliveryException, IOException{
        try {
            URL url = new URL(targetUrl);
            URLConnection conn = url.openConnection();
            conn.connect();
        } catch (MalformedURLException e) {
            String errMsg = "Web Service endpoint is malformed: (" + targetUrl + ")";
            logger.error(errMsg);
            throw new EventDeliveryException(errMsg);
        } catch (IOException e) {
            String errMsg = "Web Service endpoint is not valid: (" + targetUrl + ")";
            logger.error(errMsg);
            throw new IOException(errMsg);
        }
    }

    class CollectInforWriter {

        private List<CollectInfoBean> collectInfoBeans = new ArrayList<CollectInfoBean>();
        private Map<String,String> uploadFiles = new HashMap<String, String>();

        public CollectInforWriter() {

        }

        public void start() {

        }
        public void stop() {

        }

        public void addFileEvent(FileEvent event) {
            CollectInfoBean collectInfoBean = getParent(event);
            if (event.getStatus() == FileEvent.Status.Creating) {
                collectInfoBean.addFile(event);
                checkFileDuplication(event.getPath(), true);
            } else if (event.getStatus() == FileEvent.Status.Closing) {
                FileEvent fileEvent = collectInfoBean.getFile(event.getPath());
                if (fileEvent == null) {
                    logger.warn("Can not find Creating event of [" + event.getPath() + "], check HDFS directory");
                    // what if there is tmp file on hdfs
                } else {
                    fileEvent.setStatus(FileEvent.Status.Closing);
                }
            } else if (event.getStatus() == FileEvent.Status.Renaming) {
                FileEvent fileEvent = collectInfoBean.getFile(event.getPath());
                checkFileDuplication(event.getPath(), false);
                if (fileEvent == null) {
                    logger.warn("Can not find Creating event of [" + event.getPath() + "], check HDFS directory");
                } else {
                    if (fileEvent.getStatus() != FileEvent.Status.Closing) {
                        logger.warn("Can not find Closing event of [" + event.getPath() + "], check HDFS directory");
                    } else {
                        fileEvent.setStatus(FileEvent.Status.Renaming);
                    }
                }
            }
        }

        public void removeFileEvent(FileEvent event) {
            CollectInfoBean collectInfoBean = null;

            //modify by kkw 2015.08.04
            String collectInfoKey = CollectInfoBean.createKey(event.getDate(event.getStartTime()), event.getType(), event.getHhmm());
            for (CollectInfoBean xEvent: collectInfoBeans) {
                if (xEvent.getKey().equals(collectInfoKey)) {
                    collectInfoBean = xEvent;
                }
            }
            if (collectInfoBean == null) {
                logger.info("No need to remove fileEvent due to no Collect Info [{}]", collectInfoKey);
                return;
            }
            logger.info("remove file event [{}]", event.getPath());
            collectInfoBean.removeFile(event.getPath());
        }

        public CollectInfoBean getParent(FileEvent event) {
            CollectInfoBean collectInfoBean = null;

            //modify by kkw 2015.08.04
            String collectInfoKey = CollectInfoBean.createKey(event.getDate(event.getStartTime()), event.getType(), event.getHhmm());
            for (CollectInfoBean xEvent: collectInfoBeans) {
                if (xEvent.getKey().equals(collectInfoKey)) {
                    collectInfoBean = xEvent;
                }
            }

            //modify by kkw 2015.08.04
            if (collectInfoBean == null) {
                collectInfoBean = new CollectInfoBean(event.getCollectServer(), event.getType(), event.getDate(event.getStartTime()), event.getHhmm(),
                        CollectInfoBean.Status.START);
                collectInfoBeans.add(collectInfoBean);
                try {
                    sendCollectInfoMessage(sinkUrl, collectInfoBean);
                } catch (Exception e) {
                    logger.warn("Fail to sendCollectInfoMessage Collect Info : " + event.toString(), e);
                }
            }

            return collectInfoBean;
        }

        public void addCollectInfo(CollectInfoBean collectInfoBean) {
            collectInfoBeans.add(collectInfoBean);
        }

        public String getCollectInfoStatus(CollectInfoBean collectInfoBean) {
            StringBuilder builder = new StringBuilder("[  \n");
            builder.append("(Renaming) : " + collectInfoBean.getFiles(FileEvent.Status.Renaming) +", \n");
            builder.append("(Closing) : " + collectInfoBean.getFiles(FileEvent.Status.Closing) +", \n");
            builder.append("(Creating) : " + collectInfoBean.getFiles(FileEvent.Status.Creating) +", \n");
            builder.append("\n  ]");
            return builder.toString();
        }

        public List<CollectInfoBean> getCollectInfoBeans() {
            return collectInfoBeans;
        }

        public List<CollectInfoBean> getCollectInfoList(CollectInfoBean.Status status) {
            List<CollectInfoBean> events = new ArrayList<CollectInfoBean>();
            for (CollectInfoBean event: collectInfoBeans) {
            }
            return null;
        }

        public boolean removeCollectInfo(String key) {
            for (CollectInfoBean event: collectInfoBeans) {
                if (key.equals(event.getKey())) {
                    return collectInfoBeans.remove(event);
                }
            }
            return false;
        }

        public void checkFileDuplication(String path, boolean start) {
            String src = getSrcFromHDFSPath(path);
            if (start) {
                if (uploadFiles.containsKey(src)) {
                    logger.warn("Upload same source file, exist [{}], [{}]", uploadFiles.get(src.toString()), path);
                } else {
                    uploadFiles.put(src, path);
                }
            } else {
                String uploadPath = uploadFiles.remove(src);
                if (uploadPath == null) {
                    logger.warn("Upload path is null, [{}]", path);
                }
            }
        }

        public void clear() {
            collectInfoBeans.clear();
            uploadFiles.clear();
        }

    }

    class UploadFiles {
        // src, uploadPath
        Map<String,String> files = new HashMap<String, String>();

        public UploadFiles() {
            logger.info("---- UploadFiles initialized");
        }

        public void startUpload(String path) {
            String src = getSrcFromHDFSPath(path);
            if (files.containsKey(src)) {
                logger.warn("Upload same source file, exist [{}], [{}]", files.get(src.toString()), path);
            } else {
                files.put(src, path);
            }
        }

        public void endUpload(String path) {
            String src = getSrcFromHDFSPath(path);
            String uploadPath = files.remove(src);
            if (uploadPath == null) {
                logger.warn("Upload path is null, [{}]", path);
            }
        }
    }

    //add by kkw
    public static long parseTimetoBDAP(String timeString) throws ParseException {
        String pattern = "yyyyMMddHHmmssSSS";
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        return format.parse(timeString).getTime();
    }

    public static long parseTime(String timeString) throws ParseException {
        String pattern = "yyyy-MM-dd HH:mm:ss,SSS";
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        return format.parse(timeString).getTime();
    }

    public static long parseTime(String timeString, String pattern) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        return format.parse(timeString).getTime();
    }

    public static String formatTime(long timestamp) {
        String pattern = "yyyy-MM-dd HH:mm:ss,SSS";
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp);
        return format.format(calendar.getTime());
    }

    public static String formatTime(long timestamp, String pattern) {
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp);
        return format.format(calendar.getTime());
    }
}
