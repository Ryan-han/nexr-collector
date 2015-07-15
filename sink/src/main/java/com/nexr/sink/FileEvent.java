package com.nexr.sink;

public class FileEvent {

    public static enum Status {
        Creating,
        Closing,
        Renaming;
    }

    private String collectServer;
    private String type;
    private String hhmm;
    private String path;
    private long startTime;
    private long endTime;
    private Status status;
    private String actionMessage;


    public FileEvent(String collectServer, String type, String hhmm, String path, String status, long startTime, String actionMessage) {
        this(collectServer, type, hhmm, path, Status.valueOf(status), startTime, -1, actionMessage);
    }

    public FileEvent(String collectServer, String type, String hhmm, String path, Status status, long startTime, long endTime,
                     String actionMessage) {
        this.collectServer = collectServer;
        this.type = type;
        this.hhmm = hhmm;
        this.path = path;
        this.status = status;
        this.startTime = startTime;
        this.endTime = endTime;
        this.actionMessage = actionMessage;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getActionMessage() {
        return actionMessage;
    }

    public void setActionMessage(String actionMessage) {
        this.actionMessage = actionMessage;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String getCollectServer() {

        return collectServer;
    }

    public void setCollectServer(String collectServer) {
        this.collectServer = collectServer;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getHhmm() {
        return hhmm;
    }

    public String getDate(long timeStamp) {
        return HttpSink.formatTime(timeStamp, "yyyyMMdd");
    }

    public void setHhmm(String hhmm) {
        this.hhmm = hhmm;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder().append("[");
        builder.append(status.toString() + ", ");
        builder.append(collectServer + ", ");
        builder.append(type + ", ");
        builder.append(hhmm + ", ");
        builder.append(path + ", ");
        builder.append(startTime + ", ");
        builder.append(endTime + " ");
        builder.append("]");
        return builder.toString();
    }
}
