package com.nexr.master.services;

import com.nexr.master.CollectorException;
import com.nexr.master.servlet.ConfigServlet;
import com.nexr.master.servlet.MonitorServlet;
import com.nexr.master.servlet.SinkServlet;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CMasterService implements AppService, Constants{

    private static Logger LOG = LoggerFactory.getLogger(CMasterService.class);

    private Class[] services = new Class[] {JDBCService.class};

    private Server jettyServer;
    private int port;

    public static int DEFAULT_PORT = 19191;
    public int PORT = DEFAULT_PORT;

    private static CMasterService cMasterService;
    private JDBCService jdbcService;
    private QueueService queueService;

    private CMasterService() {

    }

    public JDBCService getJdbcService() {
        return jdbcService;
    }

    public QueueService getQueueService() {
        return queueService;
    }

    public static CMasterService getInstance() {
        if (cMasterService == null) {
            cMasterService = new CMasterService();
            cMasterService.init();
        }
        return cMasterService;
    }

    private void init() {
        try {
            initContext();
            initServices();
        } catch (CollectorException e) {
            LOG.error("Fail to init services ", e);
        }
    }

    private void initContext() {
        PORT = CMasterContext.getContext().getConfiguration().getInt(MONITOR_PORT, DEFAULT_PORT);
    }

    private void initServices() throws CollectorException {
        jdbcService = new JDBCService();
        jdbcService.start();
        queueService = new QueueService();
        queueService.start();
    }


    @Override
    public void start() throws CollectorException {
        LOG.info("========= Collector Master Starting ......   ========");

        //init();
        jettyServer = new Server(PORT);

        ContextHandlerCollection contexts = new ContextHandlerCollection();
        jettyServer.setHandler(contexts);

        Context root = new Context(contexts,"/collector",Context.SESSIONS);
        root.addServlet(new ServletHolder(new SinkServlet()), "/sink");
        root.addServlet(new ServletHolder(new MonitorServlet()), "/monitor");
        root.addServlet(new ServletHolder(new ConfigServlet()), "/config");

        try {
            jettyServer.start();
            LOG.info("Collector Master Started !! ");
            jettyServer.join();
        } catch (Exception e) {
            LOG.error("Error starting Jetty. Collector Master may not be available.", e);
        }

    }

    @Override
    public void shutdown() throws CollectorException {
        jdbcService.shutdown();
        queueService.shutdown();

        try {
            jettyServer.stop();
            jettyServer.join();
        } catch (Exception ex) {
            LOG.error("Error stopping Jetty. Collector Master may not be available.", ex);
        }
        LOG.info("========= Collector Master Shutdown ======== \n");

    }

    public static void main(String[] args) {
        String cmd = args[0];
        System.out.println("command : " + cmd);

        if ("start".equals(cmd)) {
            AppService app = CMasterService.getInstance();
            ShutdownInterceptor shutdownInterceptor = new ShutdownInterceptor(app);
            Runtime.getRuntime().addShutdownHook(shutdownInterceptor);
            try {
                app.start();
            } catch (CollectorException e) {
                e.printStackTrace();
            }
        } else if ("stop".equals(cmd)) {

        }
    }

    private static class ShutdownInterceptor extends Thread{

        private AppService app;

        public ShutdownInterceptor(AppService app) {
            this.app = app;
        }

        public void run() {
            System.out.println("Call the shutdown routine");
            try {
                app.shutdown();
            } catch (CollectorException e) {
                e.printStackTrace();
            }
        }
    }

}
