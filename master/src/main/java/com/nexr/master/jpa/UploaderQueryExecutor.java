package com.nexr.master.jpa;

import com.nexr.master.services.CMasterService;
import com.nexr.master.services.JDBCService;
import com.nexr.master.util.SshConnector;
import com.nexr.master.util.SshManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class UploaderQueryExecutor {

    private Logger LOG = LoggerFactory.getLogger(UploaderQueryExecutor.class);
    private MonitorConfigExecutor monitorConfigExecutor = new MonitorConfigExecutor();

    public enum UploaderConfigQuery {
        GET_CONFIG,
        GET_CONFIG_BY_AGENT_HOST;
    }


    private Query getSelectQuery(UploaderConfigQuery namedQuery, EntityManager em, Object... parameters) throws JPAExecutorException {
        Query query = em.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case GET_CONFIG:
                break;
            case GET_CONFIG_BY_AGENT_HOST:
                query.setParameter("agentHost", parameters[0]);
                break;
            default:
                throw new JPAExecutorException("QueryExecutor cannot execute " + namedQuery.name());

        }
        return query;
    }

    public boolean deployConfigs(UploaderConfigQuery namedQuery, Object... parameters) throws JPAExecutorException {
        List<UploaderConfig> configList = getUploaderConfigs(namedQuery, parameters);
        if (configList.size() == 0) {
            return false;
        }
        if (parameters == null) {
            LOG.info("Deploy All Hosts configuration !!");
            deployConfig(configList, null);
        } else {
            deployConfig(configList, (String) parameters[0]);
        }
        return true;
    }

    private void deployConfig(List<UploaderConfig> configList, String host) {
        try {
            if (host == null) {
                List<String> hosts = getAgentHosts(configList);
                for (String agentHost : hosts) {
                    LOG.info("Start Deploy to {} ", agentHost);
                    List<UploaderConfig> configs = getConfigByHosts(UploaderConfigQuery.GET_CONFIG_BY_AGENT_HOST, agentHost);
                    MonitorConfig monitorconfig = getAgentMonitorConfig(agentHost);
                    deploy(agentHost, configs, monitorconfig);
                }
            } else {
                LOG.info("Start Deploy to {} ", host);
                List<UploaderConfig> configs = getConfigByHosts(UploaderConfigQuery.GET_CONFIG_BY_AGENT_HOST, host);
                MonitorConfig monitorconfig = getAgentMonitorConfig(host);
                deploy(host, configs, monitorconfig);

            }
        } catch (JPAExecutorException e) {
            e.printStackTrace();
        }
    }

    private void deploy(String host, List<UploaderConfig> configs, MonitorConfig monitorconfig) {
        StringBuffer config = new StringBuffer();
        SshConnector sshConnector = new SshManager();
        sshConnector.auth(monitorconfig.getUserName(), monitorconfig.getPassword());
        try {
            sshConnector.connect(host, monitorconfig.getSshPort().intValue(), SshConnector.DEFAULT_TIMEOUT);
            String uploaderConfFileDir = monitorconfig.getConfigDir() + File.separator + "uploader";
            if (sshConnector.isExist(uploaderConfFileDir, "uploader.conf")) {
                LOG.info("COMMAND mv " + uploaderConfFileDir + File.separator + "uploader.conf" + " " + uploaderConfFileDir + File.separator + "uploader.conf_" + System.currentTimeMillis());
                sshConnector.sendCommand("mv " + uploaderConfFileDir + File.separator + "uploader.conf" + " " + uploaderConfFileDir + File.separator + "uploader.conf_" + System.currentTimeMillis());
            }
            config.append(convertTopology(configs));
            InputStream is = new ByteArrayInputStream(config.toString().getBytes());
            sshConnector.put(uploaderConfFileDir, "uploader.conf", is);

//            if (sshConnector.isExist(uploaderConfFileDir, "log4j.properties")) {
//                LOG.info("COMMAND mv " + uploaderConfFileDir + File.separator + "uploader.conf" + " " + uploaderConfFileDir + File.separator + "log4j.properties_" + System.currentTimeMillis());
//                sshConnector.sendCommand("mv " + uploaderConfFileDir + File.separator + "uploader.conf" + " " + uploaderConfFileDir + File.separator + "log4j.properties_" + System.currentTimeMillis());
//            }

//            config = new StringBuffer();
//            config.append(convertLogger(host));
//            is = new ByteArrayInputStream(config.toString().getBytes());
//            sshConnector.put(uploaderConfFileDir, "log4j.properties", is);

            String monitorConfFileDir = monitorconfig.getConfigDir() + File.separator + "umonitor";
            if (sshConnector.isExist(monitorConfFileDir, "umonitor.conf")) {
                sshConnector.sendCommand("COMMAND mv " + monitorConfFileDir + File.separator + "umonitor.conf" + " " + monitorConfFileDir + File.separator + "umonitor.conf_" + System.currentTimeMillis());
            }
            config = new StringBuffer();
            config.append(convertMonitor(host, configs));
            is = new ByteArrayInputStream(config.toString().getBytes());
            sshConnector.put(monitorConfFileDir, "umonitor.conf", is);
            sshConnector.close();

        } catch (Exception e) {
            sshConnector.close();
            e.printStackTrace();
        }
    }


    public String getAllConfigPreview(UploaderConfigQuery namedQuery, Object... parameters) throws JPAExecutorException {
        List<UploaderConfig> configList = getUploaderConfigs(namedQuery, parameters);
        if (configList.size() == 0) {
            return "";
        }
        return configGenerate(configList, null);
    }

    public String getHostConfigPreview(UploaderConfigQuery namedQuery, Object... parameters) throws JPAExecutorException {
        List<UploaderConfig> configList = getUploaderConfigs(namedQuery, parameters);
        if (configList.size() == 0) {
            return "";
        }
        return configGenerate(configList, (String) parameters[0]);
    }

    public List<UploaderConfig> getConfigByHosts(UploaderConfigQuery namedQuery, Object... parameters) throws JPAExecutorException {
        JDBCService jdbcService = CMasterService.getInstance().getJdbcService();
        EntityManager em = jdbcService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        List<?> retList = (List<?>) jdbcService.executeGetList(namedQuery.name(), query, em);
        List<UploaderConfig> configList = new ArrayList<UploaderConfig>();
        if (retList != null) {
            for (Object ret : retList) {
                configList.add(constructBean(namedQuery, ret));
            }
        }

        return configList;
    }

    private List<UploaderConfig> getUploaderConfigs(UploaderConfigQuery namedQuery, Object[] parameters) throws JPAExecutorException {
        JDBCService jdbcService = CMasterService.getInstance().getJdbcService();
        EntityManager em = jdbcService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        List<?> retList = (List<?>) jdbcService.executeGetList(namedQuery.name(), query, em);

        List<UploaderConfig> configList = new ArrayList<UploaderConfig>();
        if (retList != null) {
            for (Object ret : retList) {
                configList.add(constructBean(namedQuery, ret));
            }
        }
        return configList;
    }


    private String configGenerate(List<UploaderConfig> configList, String host) {
        StringBuffer config = new StringBuffer();
        try {
            if (host == null) {
                List<String> hosts = getAgentHosts(configList);

                for (String agentHost : hosts) {
                    List<UploaderConfig> configs = getConfigByHosts(UploaderConfigQuery.GET_CONFIG_BY_AGENT_HOST, agentHost);
                    config.append("\n\n ==== Agent " + agentHost + " uploder.conf ====\n\n" + convertTopology(configs));
                    config.append("\n\n ==== Agent " + agentHost + " umonitor.conf ====\n\n" + convertMonitor(agentHost, configs));
                }
            } else {
                List<UploaderConfig> configs = getConfigByHosts(UploaderConfigQuery.GET_CONFIG_BY_AGENT_HOST, host);
                config.append("\n\n ==== Agent " + host + " uploder.conf ==== \n\n" + convertTopology(configs));
                config.append("\n\n === Agent " + host + " umonitor.conf ==== \n\n" + convertMonitor(host, configs));
            }
        } catch (JPAExecutorException e) {
            e.printStackTrace();
        }

        return config.toString();
    }

    private List<String> getAgentHosts(List<UploaderConfig> configList) {
        List<String> hosts = new ArrayList<String>();
        for (UploaderConfig config : configList) {
            hosts.add(config.getAgentHost());
        }
        List<String> uniqueHosts = new ArrayList<String>(new HashSet<String>(hosts));
        return uniqueHosts;
    }

    private String convertTopology(List<UploaderConfig> configList) {
        InputStream template = Thread.currentThread().getContextClassLoader().getResourceAsStream("uploader.topology.template");
        String uploaderConfig = getStringFromInputStream(template);

        String sourcePrefix = "source-";
        String channelPrefix = "channel-";
        String sinkPrefix = "sink-";

        StringBuffer sourceBuf = new StringBuffer();
        StringBuffer channelBuf = new StringBuffer();
        StringBuffer sinkBuf = new StringBuffer();

        for (int i = 0; i < configList.size(); i++) {
            sourceBuf.append(sourcePrefix + i + " ");
            channelBuf.append(channelPrefix + i + " ");
            sinkBuf.append(sinkPrefix + i + " ");
        }
        uploaderConfig = uploaderConfig.replace("${sourceList}", sourceBuf.toString().trim());
        uploaderConfig = uploaderConfig.replace("${channelList}", channelBuf.toString().trim());
        uploaderConfig = uploaderConfig.replace("${sinkList}", sinkBuf.toString().trim());


        uploaderConfig = getConfig(configList, sourceBuf, channelBuf, sinkBuf, uploaderConfig);


        return uploaderConfig;
    }

    private String getConfig(List<UploaderConfig> configList, StringBuffer sourceBuf, StringBuffer channelBuf, StringBuffer sinkBuf, String content) {
        String[] sources = sourceBuf.toString().split(" ");
        String[] channels = channelBuf.toString().split(" ");
        String[] sinks = sinkBuf.toString().split(" ");

        for (int i = 0; i < sources.length; i++) {
            content += convertSource(sources[i].trim(), configList.get(i).getSpoolDir(), channels[i].trim());
        }

        for (int i = 0; i < sources.length; i++) {
            content += convertInterceptor(sources[i].trim(), configList.get(i).getDataType());
        }

        for (int i = 0; i < sources.length; i++) {
            content += convertSink(sinks[i].trim(), configList.get(i).getHdfsPath(), channels[i].trim());
        }

        for (int i = 0; i < sources.length; i++) {
            content += convertChannel(channels[i].trim());
        }

        return content;
    }

    private String convertSource(String sourceName, String spoolDir, String channelName) {
        InputStream template = Thread.currentThread().getContextClassLoader().getResourceAsStream("uploader.source.template");
        String content = getStringFromInputStream(template);
        content = content.replace("${sourceName}", sourceName);
        content = content.replace("${spoolDir}", spoolDir);
        content = content.replace("${channelName}", channelName);
        return "\n\n" + content;
    }

    private String convertInterceptor(String sourceName, String dataType) {
        InputStream template = Thread.currentThread().getContextClassLoader().getResourceAsStream("uploader.interceptor.template");
        String content = getStringFromInputStream(template);
        content = content.replace("${sourceName}", sourceName);
        content = content.replace("${dataType}", dataType);
        return "\n\n" + content;
    }

    private String convertChannel(String channelName) {
        InputStream template = Thread.currentThread().getContextClassLoader().getResourceAsStream("uploader.channel.template");
        String content = getStringFromInputStream(template);
        content = content.replace("${channelName}", channelName);
        return "\n\n" + content;
    }

    private String convertSink(String sinkName, String hdfsPath, String channelName) {
        InputStream template = Thread.currentThread().getContextClassLoader().getResourceAsStream("uploader.sink.template");
        String content = getStringFromInputStream(template);
        content = content.replace("${sinkName}", sinkName);
        content = content.replace("${hdfsPath}", hdfsPath);
        content = content.replace("${channelName}", channelName);
        return "\n\n" + content;
    }

    private String convertMonitor(String agentHost, List<UploaderConfig> configList) {
        InputStream template = Thread.currentThread().getContextClassLoader().getResourceAsStream("umonitor.template");
        String content = getStringFromInputStream(template);


        StringBuffer uploadDirBuf = new StringBuffer();
        for (int i = 0; i < configList.size(); i++) {
            uploadDirBuf.append(configList.get(i).getSpoolDir() + ",");
        }

        content = content.replace("${local_agent_server}", agentHost);
        content = content.replace("${upload_dirs}", uploadDirBuf.substring(0, uploadDirBuf.length() - 1));

        MonitorConfig monitorConfig = getAgentMonitorConfig(agentHost);
        if (monitorConfig != null) {
            content = content.replace("${uploadpath}", monitorConfig.getUploadPath());
            content = content.replace("${uploader_start_command}", "bash " + monitorConfig.getAgentHomeDir() + "/bin/uploader.sh start");
            content = content.replace("${uploader_stop_command}", "bash " + monitorConfig.getAgentHomeDir() + "/bin/uploader.sh stop");
            content = content.replace("${cmaster_server}", monitorConfig.getCmasterServer());
        }
        return "\n\n" + content;
    }

    private String convertLogger(String hostName) {
        InputStream template = Thread.currentThread().getContextClassLoader().getResourceAsStream("log4j.properties.template");
        String content = getStringFromInputStream(template);
        content = content.replace("${local_agent_server}", hostName);
        return content;

    }
    private MonitorConfig getAgentMonitorConfig(String agentHost) {
        MonitorConfig monitorConfig = null;
        try {
            monitorConfig = monitorConfigExecutor.getConfigByHosts(MonitorConfigExecutor.MonitorConfigQuery.GET_MONITOR_CONFIG_BY_AGENT_HOST, agentHost);
        } catch (JPAExecutorException e) {
            e.printStackTrace();
        }
        return monitorConfig;
    }

    private static String getStringFromInputStream(InputStream is) {
        BufferedReader br = null;
        StringBuilder sb = new StringBuilder();

        String line;
        try {
            br = new BufferedReader(new InputStreamReader(is));
            while ((line = br.readLine()) != null) {
                sb.append(line + "\n");
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return sb.toString();
    }

    private UploaderConfig constructBean(UploaderConfigQuery namedQuery, Object ret, Object... parameters)
            throws JPAExecutorException {
        UploaderConfig bean = new UploaderConfig();
        if (ret == null) {
            throw new JPAExecutorException("Query result is null");
        }
        Object[] arr = (Object[]) ret;
        bean.setId((Long) arr[0]);
        bean.setAgentHost((String) arr[1]);
        bean.setSpoolDir((String) arr[2]);
        bean.setDataType((String) arr[3]);
        bean.setHdfsPath((String) arr[4]);
        return bean;
    }

    public void insert(UploaderConfig config) throws JPAExecutorException {
        JDBCService jdbcService = CMasterService.getInstance().getJdbcService();
        jdbcService.insert(config);
    }

}
