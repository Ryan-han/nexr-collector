package com.nexr.master.jpa;

/**
 * Created by ndap on 15. 7. 7.
 */

import org.json.simple.JSONObject;

import javax.persistence.*;

@Entity
@NamedQueries(value = {
        @NamedQuery(name = "GET_MONITOR_CONFIG_BY_AGENT_HOST", query ="select a.id, a.agentHost, a.agentHomeDir, " +
                "a.configDir, a.uploadPath, a.cmasterServer, a.userName, a.password, a.sshPort " +
                "from MonitorConfig a where a.agentHost = :agentHost" )

})

@Table(name = "MONITOR_CONFIG")
public class MonitorConfig {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @Column(name = "agentHost")
    private String agentHost;

    @Column(name = "agentHomeDir")
    private String agentHomeDir;

    @Column(name = "configDir")
    private String configDir;

    @Column(name = "uploadPath")
    private String uploadPath;

    @Column(name = "cmasterServer")
    private String cmasterServer;

    @Column(name = "userName")
    private String userName;

    @Column(name = "password")
    private String password;

    @Column(name = "sshPort")
    private Long sshPort;


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

    public String getAgentHomeDir() {
        return agentHomeDir;
    }

    public void setAgentHomeDir(String agentHomeDir) {
        this.agentHomeDir = agentHomeDir;
    }

    public String getConfigDir() {
        return configDir;
    }

    public void setConfigDir(String configDir) {
        this.configDir = configDir;
    }

    public String getUploadPath() {
        return uploadPath;
    }

    public void setUploadPath(String uploadPath) {
        this.uploadPath = uploadPath;
    }

    public String getCmasterServer() {
        return cmasterServer;
    }

    public void setCmasterServer(String cmasterServer) {
        this.cmasterServer = cmasterServer;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }


    public Long getSshPort() {
        return sshPort;
    }

    public void setSshPort(Long sshPort) {
        this.sshPort = sshPort;
    }

    public JSONObject toJsonOblect() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("id", id);
        jsonObject.put("agentHost", agentHost);
        jsonObject.put("agentHomeDir", agentHomeDir);
        jsonObject.put("configDir", configDir);
        jsonObject.put("uploadPath", uploadPath);
        jsonObject.put("cmasterServer", cmasterServer);
        jsonObject.put("userName", userName);
        jsonObject.put("password", password);
        jsonObject.put("sshPort", sshPort);
        return jsonObject;
    }

    public String toString() {
        return toJsonOblect().toString();
    }

    public Object[] toParams() {
        Object[] params = new Object[]{getId(), getAgentHost(), getAgentHomeDir(), getConfigDir(), getUploadPath(), getCmasterServer()
        , getUserName(), getPassword(), getSshPort()};
        return params;
    }
}
