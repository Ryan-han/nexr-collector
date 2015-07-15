package com.nexr.sink;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestHttpSink {

    private static HttpSink httpSink;

    @BeforeClass
    public static void setupClass() {
        httpSink = new HttpSink();
    }

    @AfterClass
    public static void afterClass() {
        httpSink = null;
    }

    @Test
    public void testGroup() {

        String text = "John writes about this, and John Doe writes about that," +
                        " and John Wayne writes about everything.";

        String patternString1 = "(John)(.+?) ";

        Pattern pattern = Pattern.compile(patternString1);
        Matcher matcher = pattern.matcher(text);

        while (matcher.find()) {
            System.out.println("found: " + matcher.group(1) +
                    " " + matcher.group(2));
        }
    }

    @Test
    public void testFailToCloseException() {
        /*
        2015-05-03 00:43:51,453 WARN  [hdfs-XtasHDFS6-roll-timer-8] (org.apache.flume.sink.hdfs.BucketWriter.close:416)  - failed to close() HDFSWriter for file (hdfs://kdap-nn:9000/user/ndap/repository/user/admin/cdr_txt/kdap319/S1AP/0040/guro_LOG_S1AP_201505030036.dat.6.1430581409053.tmp). Exception follows.
org.apache.hadoop.ipc.RemoteException: org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException: No lease on /user/ndap/repository/user/admin/cdr_txt/kdap319/S1AP/0040/guro_LOG_S1AP_201505030036.dat.6.1430581409053.tmp File does not exist. [Lease.  Holder: DFSClient_NONMAPREDUCE_-1444350111_80, pendingcreates: 34]
        at org.apache.hadoop.hdfs.server.namenode.FSNamesystem.checkLease(FSNamesystem.java:1999)
        at org.apache.hadoop.hdfs.server.namenode.FSNamesystem.checkLease(FSNamesystem.java:1990)
        at org.apache.hadoop.hdfs.server.namenode.FSNamesystem.completeFileInternal(FSNamesystem.java:2045)
        at org.apache.hadoop.hdfs.server.namenode.FSNamesystem.completeFile(FSNamesystem.java:2033)
        at org.apache.hadoop.hdfs.server.namenode.NameNode.complete(NameNode.java:805)
        at sun.reflect.GeneratedMethodAccessor30.invoke(Unknown Source)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:25)
        at java.lang.reflect.Method.invoke(Method.java:597)
        at org.apache.hadoop.ipc.RPC$Server.call(RPC.java:587)
        at org.apache.hadoop.ipc.Server$Handler$1.run(Server.java:1432)
        at org.apache.hadoop.ipc.Server$Handler$1.run(Server.java:1428)
        at java.security.AccessController.doPrivileged(Native Method)
        at javax.security.auth.Subject.doAs(Subject.java:396)
        at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1190)
        at org.apache.hadoop.ipc.Server$Handler.run(Server.java:1426)
		at org.apache.hadoop.ipc.Client.call(Client.java:1113)
        at org.apache.hadoop.ipc.RPC$Invoker.invoke(RPC.java:229)
        at com.sun.proxy.$Proxy9.complete(Unknown Source)
        at sun.reflect.GeneratedMethodAccessor12.invoke(Unknown Source)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:622)
        at org.apache.hadoop.io.retry.RetryInvocationHandler.invokeMethod(RetryInvocationHandler.java:85)
        at org.apache.hadoop.io.retry.RetryInvocationHandler.invoke(RetryInvocationHandler.java:62)
        at com.sun.proxy.$Proxy9.complete(Unknown Source)
        at org.apache.hadoop.hdfs.DFSClient$DFSOutputStream.closeInternal(DFSClient.java:4121)
        at org.apache.hadoop.hdfs.DFSClient$DFSOutputStream.close(DFSClient.java:4022)
        at org.apache.hadoop.fs.FSDataOutputStream$PositionCache.close(FSDataOutputStream.java:61)
        at org.apache.hadoop.fs.FSDataOutputStream.close(FSDataOutputStream.java:86)
        at org.apache.flume.sink.hdfs.HDFSDataStream.close(HDFSDataStream.java:140)
        at org.apache.flume.sink.hdfs.BucketWriter$3.call(BucketWriter.java:341)
        at org.apache.flume.sink.hdfs.BucketWriter$3.call(BucketWriter.java:335)
        at org.apache.flume.sink.hdfs.BucketWriter$9$1.run(BucketWriter.java:718)
        at org.apache.flume.sink.hdfs.BucketWriter.runPrivileged(BucketWriter.java:183)
        at org.apache.flume.sink.hdfs.BucketWriter.access$1700(BucketWriter.java:59)
        at org.apache.flume.sink.hdfs.BucketWriter$9.call(BucketWriter.java:715)
         */

        String text = "2015-05-03 00:43:51,453 WARN  [hdfs-XtasHDFS6-roll-timer-8] " +
                "(org.apache.flume.sink.hdfs.BucketWriter.close:416)  - failed to close() HDFSWriter for file " +
                "(hdfs://kdap-nn:9000/user/ndap/repository/user/admin/cdr_txt/kdap319/S1AP/0040/guro_LOG_S1AP_201505030036.dat.6.1430581409053.tmp)" +
                ". Exception follows.\n" +
                "org.apache.hadoop.ipc.RemoteException: org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException: No lease on" +
                " /user/ndap/repository/user/admin/cdr_txt/kdap319/S1AP/0040/guro_LOG_S1AP_201505030036.dat.6.1430581409053.tmp File does not exist. ";

        String patternString1 = "(failed to close\\(\\) HDFSWriter)(.*)(\\(hdfs://(.*)\\))(.*)";

        Pattern pattern = Pattern.compile(patternString1);
        Matcher matcher = pattern.matcher(text);

        Assert.assertEquals(true, matcher.find());
        System.out.println("---- group count : " + matcher.groupCount());
        System.out.println(matcher.group(1));
        String m2 = matcher.group(2);
        System.out.println(m2);
        String m3 = matcher.group(3);
        System.out.println(m3);
        String m4 = matcher.group(4);
        System.out.println(m4);

        Assert.assertEquals("hdfs://kdap-nn:9000/user/ndap/repository/user/admin/cdr_txt/kdap319/S1AP/0040" +
                "/guro_LOG_S1AP_201505030036.dat.6.1430581409053.tmp", "hdfs://" + m4);

        text = "2015-05-20 06:40:55,123 WARN  [hdfs-XtasHDFS1-roll-timer-6] " +
                "(org.apache.flume.sink.hdfs.BucketWriter.close:416) null - failed to close() HDFSWriter for file " +
                "(hdfs://kdapp-nn:9000/user/ndap/repository/user/admin/cdr_txt/kdap319/S1AP/0630/daejeon_LOG_S1AP_201505200630.dat.1.1432071465999.tmp)" +
                ". Exception follows. java.io.IOExceptii\n" +
                "on: Callable timed out after 120000 ms on file: hdfs://kdap-nn:9000/user/ndap/repository/user/admin/cdr_txt/kdap319/S1AP/0630/daejeon_LOG_S1AP_201505200630.datt\n" +
                ".1.1432071465999.tmp";

        matcher = pattern.matcher(text);

        Assert.assertEquals(true, matcher.find());
        m4 = matcher.group(4);
        System.out.println(m4);

        Assert.assertEquals("hdfs://kdapp-nn:9000/user/ndap/repository/user/admin/cdr_txt/kdap319/S1AP/0630/daejeon_LOG_S1AP_201505200630.dat.1.1432071465999.tmp", "hdfs://" + m4);
    }

    @Test
    public void testFailToCloseFinalException() {
        /*
        2015-05-20 06:49:55,135 WARN  [hdfs-XtasHDFS1-roll-timer-8] (org.apache.flume.sink.hdfs.BucketWriter$4.call:362)  - Unsuccessfully attempted to close hdfs://kdd
ap-nn:9000/user/ndap/repository/user/admin/cdr_txt/kdap319/S1AP/0630/daejeon_LOG_S1AP_201505200630.dat.1.1432071465999.tmp 3 times. File may be open, or may noo
t have been renamed.
         */

        String text = "2015-05-20 06:49:55,135 WARN  [hdfs-XtasHDFS1-roll-timer-8] (org.apache.flume.sink.hdfs.BucketWriter$4.call:362)  - " +
                "Unsuccessfully attempted to close hdfs://kdap-nn:9000/user/ndap/repository/user/admin/cdr_txt/kdap319/S1AP/0630/daejeon_LOG_S1AP_201505200630.dat.1.1432071465999.tmp 3 times. " +
                "File may be open, or may not have been renamed.";

        String patternString1 = "(Unsuccessfully attempted to)(.*)(hdfs://(.*).tmp)(.*)";

        Pattern pattern = Pattern.compile(patternString1);
        Matcher matcher = pattern.matcher(text);

        Assert.assertEquals(true, matcher.find());
        System.out.println("---- group count : " + matcher.groupCount());
        System.out.println(matcher.group(1));
        String m2 = matcher.group(2);
        System.out.println(m2);
        String m3 = matcher.group(3);
        System.out.println(m3);
        String m4 = matcher.group(4);
        System.out.println(m4);
        String m5 = matcher.group(5);
        System.out.println(m5);

        String path = "hdfs://kdap-nn:9000/user/ndap/repository/user/admin/cdr_txt/kdap319/S1AP/0630" +
                "/daejeon_LOG_S1AP_201505200630.dat.1.1432071465999.tmp";

        Assert.assertEquals(path, m3);

        String fileName = httpSink.getFileFailedToCloseFinal(text);
        Assert.assertNotNull(fileName);


        Assert.assertEquals(path, fileName);

    }


    @Test
    public void testHDFSIOErrorPath() {

        String text = "2015-05-04 15:10:36,851 WARN  [SinkRunner-PollingRunner-DefaultSinkProcessor] " +
                "(org.apache.flume.sink.hdfs.HDFSEventSink.process:463) null - HDFS IO error java.io.IOException: " +
                "Callable timed out after 1100 ms on file: " +
                "hdfs://seair:8020/user/flume/HTTP/1350/gwangju_LOG_HTTP_2015035.dat.1.1430369477451.tmp\n" +
                "\tat org.apache.flume.sink.hdfs.BucketWriter.callWithTimeout(BucketWriter.java:732)\n" +
                "\tat org.apache.flume.sink.hdfs.BucketWriter.open(BucketWriter.java:262)\n";

        String patternString1 = "(HDFS IO error)(.*)(hdfs://(.*).tmp)(.*)";

        Pattern pattern = Pattern.compile(patternString1);
        Matcher matcher = pattern.matcher(text);

        Assert.assertEquals(true, matcher.find());
        System.out.println("---- group count : " + matcher.groupCount());
        System.out.println(matcher.group(1));
        System.out.println(matcher.group(2));
        System.out.println(matcher.group(3));
        System.out.println(matcher.group(4));

        String path = "hdfs://seair:8020/user/flume/HTTP/1350/gwangju_LOG_HTTP_2015035.dat.1.1430369477451.tmp";

        Assert.assertEquals(path, matcher.group(3));

        String fileName = httpSink.getHDFSIOErrorPath1(text);
        Assert.assertEquals(path, fileName);

        text = "2015-05-03 00:43:49,731 WARN  [SinkRunner-PollingRunner-DefaultSinkProcessor] " +
                "(org.apache.flume.sink.hdfs.HDFSEventSink.process:463)  - HDFS IO error org.apache.hadoop.ipc.RemoteException: org.apache.hadoop.hdfs.server.namenode." +
                "LeaseExpiredException: No lease on /user/ndap/repository/user/admin/cdr_txt/kdap319/S1AP/0040/daegu_LOG_S1AP_201505030036.dat.7.1430581428554.tmp File does not exist. [Lease.  Holder: DFSClient_NONMAPREDUCE_-1444350111_80, pendingcreates: 34]";
        path = "/user/ndap/repository/user/admin/cdr_txt/kdap319/S1AP/0040/daegu_LOG_S1AP_201505030036.dat.7.1430581428554.tmp";

        patternString1 = "HDFS IO error([^/]*)(.*.tmp)";
        pattern = Pattern.compile(patternString1);
        matcher = pattern.matcher(text);

        Assert.assertEquals(true, matcher.find());
        String m2 = matcher.group(2);

        Assert.assertEquals(path, m2);

        Assert.assertEquals(path, httpSink.getHDFSIOErrorPath2(text));

    }

    @Test
    public void testHDFSIOErrorPath3() {

        String text = "2015-05-22 08:50:11,980 WARN  [SinkRunner-PollingRunner-DefaultSinkProcessor] " +
                "(org.apache.flume.sink.hdfs.BucketWriter.append:601)  - " +
                "Caught IOException while closing file (hdfs://kdap-nn:9000/user/ndap/repository/user/admin/cdr_txt/kdap319/S1AP/0830/guro_LOG_S1AP_201505220816.dat.3.1432251507480.tmp). " +
                "Exception follows.";

        String patternString1 = "(Caught IOException while closing file)(.*)(hdfs://(.*).tmp)(.*)";

        Pattern pattern = Pattern.compile(patternString1);
        Matcher matcher = pattern.matcher(text);

        Assert.assertEquals(true, matcher.find());
        System.out.println("---- group count : " + matcher.groupCount());
        System.out.println(matcher.group(1));
        System.out.println(matcher.group(2));
        System.out.println(matcher.group(3));
        System.out.println(matcher.group(4));

        String path = "hdfs://kdap-nn:9000/user/ndap/repository/user/admin/cdr_txt/kdap319/S1AP/0830/guro_LOG_S1AP_201505220816.dat.3.1432251507480.tmp";

        Assert.assertEquals(path, matcher.group(3));

        String fileName = httpSink.getHDFSIOErrorPath3(text);
        Assert.assertEquals(path, fileName);

        text = "2015-05-26 08:21:54,590 WARN  [SinkRunner-PollingRunner-DefaultSinkProcessor] " +
                "(org.apache.flume.sink.hdfs.BucketWriter.append:601)  - " +
                "Caught IOException while closing file (hdfs://kdap-nn:9000/user/ndap/repository/user/admin/cdr_txt/kdap319/S1AP/0810/daejeon_LOG_S1AP_201505260810.dat.1.1432596096739.tmp). " +
                "Exception follows.\n" +
                "java.io.IOException: DFSOutputStream is closed";

        path = "hdfs://kdap-nn:9000/user/ndap/repository/user/admin/cdr_txt/kdap319/S1AP/0810/daejeon_LOG_S1AP_201505260810.dat.1.1432596096739.tmp";


        Assert.assertEquals(path, httpSink.getHDFSIOErrorPath3(text));

    }

    @Test
    public void testGetSpoolDirFatal() {

        String text = "2015-06-04 10:15:19,667 ERROR [pool-14-thread-1] " +
                "(org.apache.flume.source.SpoolDirectorySource$SpoolDirectoryRunnable.run:256)  - " +
                "FATAL: Spool Directory sce LtasSrc2: { spoolDir: /data1/xtas/upload2 }: " +
                "Uncaught exception in SpoolDirectorySource thread. Restart or reconfigure Flume to continue processing.\n" +
                "java.lang.IllegalStateException: File has been modified since being read: /data1/xtas/upload2/guro_LOG_S1AP_201506040824.dat\n" +
                "        at org.apache.flume.client.avro.ReliableSpoolingFileEventReader.retireCurrentFile(ReliableSpoolingFileEventReader.java:306)";

        String patternString1 = "(FATAL:)(.*)(spoolDir: (.*) }: )(Uncaught exception in SpoolDirectorySource thread. Restart or reconfigure Flume to continue processing)(.*)";

        Pattern pattern = Pattern.compile(patternString1);
        Matcher matcher = pattern.matcher(text);

        Assert.assertEquals(true, matcher.find());
        String dir = "/data1/xtas/upload2";

        Assert.assertEquals(dir, matcher.group(4));

        String spoolDir = httpSink.getSpoolDirFatal(text);
        Assert.assertEquals(dir, spoolDir);
    }


    @Test
    public void testGetSrcFileName() {

        String text = "hdfs://seair:8020/user/seoeun/Ltas/seair.nexr.com/S1AP/1020/yongin_LOG_S1AP_201503192315.dat.1.1431566978097.tmp";

        String patternString1 = "(hdfs://)(.*)(/.*.dat)(.*)";

        Pattern pattern = Pattern.compile(patternString1);
        Matcher matcher = pattern.matcher(text);

        Assert.assertEquals(true, matcher.find());
        System.out.println("---- group count : " + matcher.groupCount());
        System.out.println(matcher.group(1));
        System.out.println(matcher.group(2));
        System.out.println(matcher.group(3));

        String fileName = httpSink.getSrcFromHDFSPath(text);
        Assert.assertNotNull(fileName);
        Assert.assertEquals("yongin_LOG_S1AP_201503192315.dat", fileName);

    }

    @Test
    public void testExtractNameNode() {
        //String uploadPath = "hdfs://seair.nexr.com:8020/user/ndap/Ltas";
        String uploadPath = "hdfs://kdap-nn:9000/user/ndap/Ltas";
        String patternStr = "(hdfs://)([a-z.\\-:0-9]*)";

        Pattern pattern = Pattern.compile(patternStr);
        Matcher matcher = pattern.matcher(uploadPath);
        Assert.assertEquals(true, matcher.find());
        System.out.println(matcher.group(1));
        System.out.println(matcher.group(2));

        String namenode = matcher.group(1) + matcher.group(2);
        Assert.assertEquals("hdfs://kdap-nn:9000", namenode);

        Assert.assertEquals("hdfs://kdap-nn:9000", httpSink.getNameNode("hdfs://kdap-nn:9000/user/ndap/Ltas"));
        Assert.assertEquals("hdfs://seair.nexr.com:8020", httpSink.getNameNode("hdfs://seair.nexr.com:8020/user/ndap/Ltas"));
    }

    @Test
    public void testExtractTime() {
        String message = "2015-05-04 15:10:36,851 WARN  [SinkRunner-PollingRunner-DefaultSinkProcessor]";
        String dateTime = "2015-05-04 15:10:36,851";

        String patternStr = "(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3})(.*)";

        Pattern pattern = Pattern.compile(patternStr);
        Matcher matcher = pattern.matcher(message);
        Assert.assertEquals(true, matcher.find());
        String m1 = matcher.group(1);
        System.out.println(m1);

        Assert.assertEquals(dateTime, m1);
    }

    @Test
    public void testInclude() {

        System.out.println((true ^ true) + "");
        System.out.println((true ^ false) + "");
        System.out.println((false ^ true) + "");
        System.out.println((false ^ false) + "");


        Assert.assertEquals(true, hasSuffix("a.tmp", "tmp", true));
        Assert.assertEquals(true, hasSuffix("a.dat", "tmp", false));

        Assert.assertEquals(false, hasSuffix("a.hello", "tmp", true));
        Assert.assertEquals(false, hasSuffix("a.dat", "dat", false));
    }

    private boolean hasSuffix(String path, String suffix, boolean include) {

        boolean result = !(include ^ path.endsWith(suffix));
        return result;
    }

}
