package com.nexr.master.jpa;

import org.json.simple.JSONObject;

import javax.persistence.*;

@Entity
@NamedQueries(value = {
        @NamedQuery(name = "GET_CONFIG", query = "select a.id, a.agentHost, a.spoolDir, a.dataType, a.hdfsPath " +
                "from UploaderConfig a order by a.agentHost"),
        @NamedQuery(name = "GET_CONFIG_BY_AGENT_HOST", query ="select a.id, a.agentHost, a.spoolDir, a.dataType, a.hdfsPath " +
                "from UploaderConfig a where a.agentHost = :agentHost" )

})

@Table(name = "UPLOADER_CONFIG")
public class UploaderConfig {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;
    @Column(name = "agentHost")
    private String agentHost;
    @Column(name = "spoolDir")
    private String spoolDir;
    @Column(name = "dataType")
    private String dataType;
    @Column(name = "hdfsPath")
    private String hdfsPath;


    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getAgentHost() {
        return agentHost;
    }

    public void setAgentHost(String agentHost) {
        this.agentHost = agentHost;
    }

    public String getSpoolDir() {
        return spoolDir;
    }

    public void setSpoolDir(String spoolDir) {
        this.spoolDir = spoolDir;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getHdfsPath() {
        return hdfsPath;
    }

    public void setHdfsPath(String hdfsPath) {
        this.hdfsPath = hdfsPath;
    }



    public JSONObject toJsonOblect() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("id", id);
        jsonObject.put("agentHost", agentHost);
        jsonObject.put("spoolDir", spoolDir);
        jsonObject.put("dataType", dataType);
        jsonObject.put("hdfsPath", hdfsPath);
        return jsonObject;
    }

    public String toString() {
        return toJsonOblect().toString();
    }

    public Object[] toParams() {
        Object[] params = new Object[]{getId(), getAgentHost(), getSpoolDir(), getDataType(), getHdfsPath()};
        return params;
    }
}
