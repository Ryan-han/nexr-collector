package com.nexr.master.services;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CMasterContext {

    private static Logger LOG = LoggerFactory.getLogger(CMasterContext.class);

    private static CMasterContext context;

    private Configuration configuration;

    private static final String SITE_XML = "collector-site.xml";

    private CMasterContext() {
        initConfig();
    }

    private void initConfig() {
        configuration = new Configuration(false);
        configuration.addResource(Thread.currentThread().getContextClassLoader().getResourceAsStream("collector-default.xml"));

        if (Thread.currentThread().getContextClassLoader().getResourceAsStream(SITE_XML) !=  null) {
            System.out.println("resource : " + Thread.currentThread().getContextClassLoader().getResource(SITE_XML).getPath());
            configuration.addResource(Thread.currentThread().getContextClassLoader().getResourceAsStream(SITE_XML));
        }

        for (Map.Entry<String,String> entry: configuration) {
            LOG.debug("Configuration : " + entry.getKey() + " : " + entry.getValue());
        }
    }

    public static CMasterContext getContext() {
        if (context == null) {
            context = new CMasterContext();
        }
        return context;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

}
