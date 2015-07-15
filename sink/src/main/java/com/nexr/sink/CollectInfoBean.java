package com.nexr.sink;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CollectInfoBean {

    private static final Logger logger = LoggerFactory.getLogger(CollectInfoBean.class);

    public static enum Status {
        START,
        END,
        DELETE;
    }

    private String collserver;
    private String dirname;
    private String senddate;
    private String hm;
    private Status status;
    private Map<String, FileEvent> files;


    public CollectInfoBean(String collectServer, String type, String date, String hhmm, Status status) {
        this.collserver = collectServer;
        this.dirname = type;
        this.senddate = date;
        this.hm = hhmm;
        this.status = status;
        this.files = new ConcurrentHashMap<String, FileEvent>();
    }

    public String getCollserver() {
        return collserver;
    }

    public void setCollserver(String collserver) {
        this.collserver = collserver;
    }

    public String getDirname() {
        return dirname;
    }

    public void setDirname(String dirname) {
        this.dirname = dirname;
    }

    public String getSenddate() {
        return senddate;
    }

    /**
     * @param senddate yyyyMMdd
     */
    public void setSenddate(String senddate) {
        this.senddate = senddate;
    }

    public String getHm() {
        return hm;
    }

    public void setHm(String hm) {
        this.hm = hm;
    }

    public void addFile(FileEvent event) {
        files.put(event.getPath(), event);
    }

    /**
     * @param path
     * @return null if there is no corresponding FileEvent
     */
    public FileEvent getFile(String path) {
        return files.get(path);
    }

    public Map<String, FileEvent> getFiles() {
        return files;
    }

    public void removeFile(String path) {
        files.remove(path);
    }

    public void setFiles(Map<String, FileEvent> files) {
        this.files = files;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public List<FileEvent> getFiles(FileEvent.Status status) {
        List<FileEvent> fileEvents = new ArrayList<FileEvent>();
        for (Map.Entry<String, FileEvent> entry : files.entrySet()) {
            if (entry.getValue().getStatus() == status) {
                fileEvents.add(entry.getValue());
            }
        }
        return fileEvents;
    }

    public String getKey() {
        return createKey(dirname, hm);
    }

    public Status updateStatus() {
        Status status = Status.END;
        if (files.size() == 0) {
            logger.info("Collect Info does not contain file");
        }
        for (Map.Entry<String, FileEvent> entry : files.entrySet()) {
            if (entry.getValue().getStatus() != FileEvent.Status.Renaming) {
                status = Status.START;
            }
        }
        if (this.status == status) {
            logger.debug("Status not changed : " + status);
        }
        this.setStatus(status);
        return status;
    }

    public static String createKey(String type, String hhmm) {
        return type + "&&" + hhmm;
    }

    public JSONObject toJsonObject() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("collserver", getCollserver());
        jsonObject.put("dirname", getDirname());
        jsonObject.put("senddate", getSenddate());
        jsonObject.put("hm", getHm());
        jsonObject.put("status", getStatus().toString());
        return jsonObject;
    }

    public String toString() {
        return toJsonObject().toString();
    }
}
