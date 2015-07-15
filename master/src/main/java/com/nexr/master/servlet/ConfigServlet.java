package com.nexr.master.servlet;


import com.nexr.master.jpa.MonitorConfig;
import com.nexr.master.jpa.MonitorConfigExecutor;
import com.nexr.master.jpa.UploaderQueryExecutor;
import com.nexr.master.jpa.JPAExecutorException;
import com.nexr.master.util.SshConnector;
import com.nexr.master.util.SshManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by ndap on 15. 7. 7.
 */
public class ConfigServlet extends HttpServlet {

    private static Logger LOG = LoggerFactory.getLogger(ConfigServlet.class);
    private SshConnector sshConnector = new SshManager();
    UploaderQueryExecutor executor = new UploaderQueryExecutor();


    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String op = request.getParameter("op");
        String agentHost = request.getParameter("agentHost");
        String result = null;
        LOG.info("Operation ==> {}", op);
        if (op == null || op.equals("preview")) {
            result = getPreview(agentHost);
        } else if (op.contentEquals("deploy")) {
            result = deploy(agentHost);
        }

        try {
            response.setContentType("text/html");
            response.getOutputStream().write(new String("<pre>" + result + "</pre>").getBytes());
            response.setStatus(HttpServletResponse.SC_OK);

        } catch (Exception e) {
            LOG.warn("Fail to get Config !! ", e);
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);

        }

    }

    private String getPreview(String agentHost) {
        String content = null;
        try {
            if (agentHost == null || agentHost.equals("all")) {
                LOG.info("Get All Agents configration !!");
                content = executor.getAllConfigPreview(UploaderQueryExecutor.UploaderConfigQuery.GET_CONFIG, null);
            } else {
                LOG.info("Get {} Agent configration !!", agentHost);
                content = executor.getHostConfigPreview(UploaderQueryExecutor.UploaderConfigQuery.GET_CONFIG_BY_AGENT_HOST, agentHost);

            }
        } catch (JPAExecutorException e) {
            e.printStackTrace();
        }
        return content;
    }

    private String deploy(String agentHost) {
        try {
            if (agentHost == null || agentHost.equals("all")) {
                executor.deployConfigs(UploaderQueryExecutor.UploaderConfigQuery.GET_CONFIG, null);
            } else {
                executor.deployConfigs(UploaderQueryExecutor.UploaderConfigQuery.GET_CONFIG, agentHost);
            }
        } catch (JPAExecutorException e) {
            e.printStackTrace();
        }
        String content = getPreview(agentHost);

        return getPreview(agentHost);
    }

}
