package com.nexr.master.services;

import com.nexr.master.CollectorException;
import com.nexr.master.jpa.MonitorConfig;
import com.nexr.master.jpa.UploaderConfig;
import com.nexr.master.jpa.JPAExecutorException;
import com.nexr.master.jpa.CollectInfo;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.openjpa.lib.jdbc.DecoratingDataSource;
import org.apache.openjpa.persistence.OpenJPAEntityManagerFactorySPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.NoResultException;
import javax.persistence.Persistence;
import javax.persistence.PersistenceException;
import javax.persistence.Query;
import java.text.MessageFormat;
import java.util.List;
import java.util.Properties;

public class JDBCService implements AppService{

    private static Logger LOG = LoggerFactory.getLogger(CMasterService.class);

    private EntityManagerFactory factory;
    public static String persistentUnit = "master-mysql";

    public static final String CONF_PREFIX = "collector.master.";
    public static final String CONF_URL = CONF_PREFIX + "jdbc.url";
    public static final String CONF_DRIVER = CONF_PREFIX + "jdbc.driver";
    public static final String CONF_USERNAME = CONF_PREFIX + "jdbc.username";
    public static final String CONF_PASSWORD = CONF_PREFIX + "jdbc.password";
    public static final String CONF_DB_SCHEMA = CONF_PREFIX + "schema.name";
    public static final String CONF_CONN_DATA_SOURCE = CONF_PREFIX + "connection.data.source";
    public static final String CONF_CONN_PROPERTIES = CONF_PREFIX + "connection.properties";
    public static final String CONF_MAX_ACTIVE_CONN = CONF_PREFIX + "pool.max.active.conn";
    public static final String CONF_CREATE_DB_SCHEMA = CONF_PREFIX + "create.db.schema";
    public static final String CONF_VALIDATE_DB_CONN = CONF_PREFIX + "validate.db.connection";
    public static final String CONF_VALIDATE_DB_CONN_EVICTION_INTERVAL = CONF_PREFIX + "validate.db.connection.eviction.interval";
    public static final String CONF_VALIDATE_DB_CONN_EVICTION_NUM = CONF_PREFIX + "validate.db.connection.eviction.num";

    public JDBCService() {

    }

    private BasicDataSource getBasicDataSource() {
        // Get the BasicDataSource object; it could be wrapped in a DecoratingDataSource
        // It might also not be a BasicDataSource if the user configured something different
        BasicDataSource basicDataSource = null;
        OpenJPAEntityManagerFactorySPI spi = (OpenJPAEntityManagerFactorySPI) factory;
        Object connectionFactory = spi.getConfiguration().getConnectionFactory();
        if (connectionFactory instanceof DecoratingDataSource) {
            DecoratingDataSource decoratingDataSource = (DecoratingDataSource) connectionFactory;
            basicDataSource = (BasicDataSource) decoratingDataSource.getInnermostDelegate();
        } else if (connectionFactory instanceof BasicDataSource) {
            basicDataSource = (BasicDataSource) connectionFactory;
        }
        return basicDataSource;
    }

    public void start() throws CollectorException {
        Configuration conf = CMasterContext.getContext().getConfiguration();
        String dbSchema = conf.get(CONF_DB_SCHEMA);
        String url = conf.get(CONF_URL);
        String driver = conf.get(CONF_DRIVER);
        String user = conf.get(CONF_USERNAME);
        String password = conf.get(CONF_PASSWORD).trim();
        String maxConn = conf.get(CONF_MAX_ACTIVE_CONN).trim();
        String dataSource = conf.get(CONF_CONN_DATA_SOURCE);
        String connPropsConfig = conf.get(CONF_CONN_PROPERTIES);
        boolean autoSchemaCreation = conf.getBoolean(CONF_CREATE_DB_SCHEMA, true);
        boolean validateDbConn = conf.getBoolean(CONF_VALIDATE_DB_CONN, true);
        String evictionInterval = conf.get(CONF_VALIDATE_DB_CONN_EVICTION_INTERVAL).trim();
        String evictionNum = conf.get(CONF_VALIDATE_DB_CONN_EVICTION_NUM).trim();

        if (!url.startsWith("jdbc:")) {
            throw new CollectorException("invalid JDBC URL, must start with 'jdbc:'");
        }
        String dbType = url.substring("jdbc:".length());
        if (dbType.indexOf(":") <= 0) {
            throw new CollectorException("invalid JDBC URL, missing vendor 'jdbc:[VENDOR]:...'");
        }

        String connProps = "DriverClassName={0},Url={1},Username={2},Password={3},MaxActive={4}";
        connProps = MessageFormat.format(connProps, driver, url, user, password, maxConn);
        Properties props = new Properties();
        if (autoSchemaCreation) {
            connProps += ",TestOnBorrow=false,TestOnReturn=false,TestWhileIdle=false";
            props.setProperty("openjpa.jdbc.SynchronizeMappings", "buildSchema(ForeignKeys=true)");
        } else if (validateDbConn) {
            String interval = "timeBetweenEvictionRunsMillis=" + evictionInterval;
            String num = "numTestsPerEvictionRun=" + evictionNum;
            connProps += ",TestOnBorrow=true,TestOnReturn=true,TestWhileIdle=true," + interval + "," + num;
            connProps += ",ValidationQuery=select count(*) from VALIDATE_CONN";
            connProps = MessageFormat.format(connProps, dbSchema);
        } else {
            connProps += ",TestOnBorrow=false,TestOnReturn=false,TestWhileIdle=false";
        }
        if (connPropsConfig != null) {
            connProps += "," + connPropsConfig;
        }
        props.setProperty("openjpa.ConnectionProperties", connProps);

        props.setProperty("openjpa.ConnectionDriverName", dataSource);

        factory = Persistence.createEntityManagerFactory(persistentUnit, props);

        EntityManager entityManager = getEntityManager();
        entityManager.find(CollectInfo.class, 1);
        entityManager.find(UploaderConfig.class, 1);
        entityManager.find(MonitorConfig.class, 1);

        LOG.info("All entities initialized");
        entityManager.getTransaction().begin();
        OpenJPAEntityManagerFactorySPI spi = (OpenJPAEntityManagerFactorySPI) factory;
        String logMsg = spi.getConfiguration().getConnectionProperties().replaceAll("Password=.*?,", "Password=***,");
        LOG.info("JDBC configuration: {}", logMsg);
        entityManager.getTransaction().commit();
        entityManager.close();

        getBasicDataSource();
    }

    public EntityManager getEntityManager() {
        return factory.createEntityManager();
    }

    public void insert(CollectInfo collectInfo) throws JPAExecutorException {
        EntityManager em = getEntityManager();
        try {
            LOG.trace("Executing JPAExecutor [{}]", "CollectInfoInsertExecution");
            em.getTransaction().begin();
            em.persist(collectInfo);
            if (em.getTransaction().isActive()) {
                em.getTransaction().commit();
            }
        }
        catch (PersistenceException e) {
            throw new JPAExecutorException(e);
        }
        finally {
            try {
                if (em.getTransaction().isActive()) {
                    LOG.warn("JPAExecutor [{}] ended with an active transaction, rolling back", "CollectInfoInsertExecution");
                    em.getTransaction().rollback();
                }
            }
            catch (Exception ex) {
                LOG.warn("CollectInfoInsertExecution", ex.getMessage(), ex);
            }
            try {
                if (em.isOpen()) {
                    em.close();
                }
                else {
                    LOG.warn("JPAExecutor [{}] closed the EntityManager, it should not!", "CollectInfoInsertExecution");
                }
            }
            catch (Exception ex) {
                LOG.warn("Could not close EntityManager after JPAExecutor [{}], {} " +"CollectInfoInsertExecution", ex
                        .getMessage(), ex);
            }
        }
    }

    public int executeUpdate(String namedQueryName, Query query, EntityManager em) throws JPAExecutorException {
        try {

            LOG.trace("Executing Update/Delete Query [{}]", namedQueryName);
            em.getTransaction().begin();
            int ret = query.executeUpdate();
            if (em.getTransaction().isActive()) {
                em.getTransaction().commit();
            }
            return ret;
        }
        catch (PersistenceException e) {
            throw new JPAExecutorException(e);
        }
        finally {
            processFinally(em, namedQueryName, true);
        }
    }

    public Object executeGet(String namedQueryName, Query query, EntityManager em) {
        try {

            Object obj = null;
            try {
                obj = query.getSingleResult();
            }
            catch (NoResultException e) {
                // return null when no matched result
            }
            return obj;
        }
        finally {
            processFinally(em, namedQueryName, false);
        }
    }

    public List<?> executeGetList(String namedQueryName, Query query, EntityManager em) {
        try {

            List<?> resultList = null;
            try {
                resultList = query.getResultList();
            }
            catch (NoResultException e) {
                // return null when no matched result
            }
            return resultList;
        }
        finally {
            processFinally(em, namedQueryName, false);
        }
    }

    private void processFinally(EntityManager em, String name, boolean checkActive) {
        if (checkActive) {
            try {
                if (em.getTransaction().isActive()) {
                    LOG.warn("[{}] ended with an active transaction, rolling back", name);
                    em.getTransaction().rollback();
                }
            }
            catch (Exception ex) {
                LOG.warn("Could not check/rollback transaction after [{}], {}", name + ex.getMessage(), ex);
            }
        }
        try {
            if (em.isOpen()) {
                em.close();
            }
            else {
                LOG.warn("[{0}] closed the EntityManager, it should not!", name);
            }
        }
        catch (Exception ex) {
            LOG.warn("Could not close EntityManager after [{}], {}", name + ex.getMessage(), ex);
        }
    }


    public void insert(UploaderConfig collectorConfig) throws JPAExecutorException {
        EntityManager em = getEntityManager();
        try {
            LOG.trace("Executing JPAExecutor [{}]", "CollectConfigInsertExecution");
            em.getTransaction().begin();
            em.persist(collectorConfig);
            if (em.getTransaction().isActive()) {
                em.getTransaction().commit();
            }
        }
        catch (PersistenceException e) {
            throw new JPAExecutorException(e);
        }
        finally {
            try {
                if (em.getTransaction().isActive()) {
                    LOG.warn("JPAExecutor [{}] ended with an active transaction, rolling back", "CollectConfigInsertExecution");
                    em.getTransaction().rollback();
                }
            }
            catch (Exception ex) {
                LOG.warn("CollectConfigInsertExecution", ex.getMessage(), ex);
            }
            try {
                if (em.isOpen()) {
                    em.close();
                }
                else {
                    LOG.warn("JPAExecutor [{}] closed the EntityManager, it should not!", "CollectConfigInsertExecution");
                }
            }
            catch (Exception ex) {
                LOG.warn("Could not close EntityManager after JPAExecutor [{}], {} " +"CollectConfigInsertExecution", ex
                        .getMessage(), ex);
            }
        }
    }

    @Override
    public void shutdown() {
        if (factory != null && factory.isOpen()) {
            factory.close();
        }
    }
}
