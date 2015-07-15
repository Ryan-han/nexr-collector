package com.nexr.master.jpa;

import com.nexr.master.services.CMasterService;
import com.nexr.master.services.JDBCService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ndap on 15. 7. 7.
 */
public class MonitorConfigExecutor {
    private Logger LOG = LoggerFactory.getLogger(MonitorConfigExecutor.class);
    public enum MonitorConfigQuery {
        GET_MONITOR_CONFIG_BY_AGENT_HOST;
    }

    private Query getSelectQuery(MonitorConfigQuery namedQuery, EntityManager em, Object... parameters) throws JPAExecutorException {
        Query query = em.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case GET_MONITOR_CONFIG_BY_AGENT_HOST:
                query.setParameter("agentHost", parameters[0]);
                break;
            default:
                throw new JPAExecutorException("QueryExecutor cannot execute " + namedQuery.name());

        }
        return query;
    }

    public MonitorConfig getConfigByHosts(MonitorConfigQuery namedQuery, Object... parameters) throws JPAExecutorException {
        JDBCService jdbcService = CMasterService.getInstance().getJdbcService();
        EntityManager em = jdbcService.getEntityManager();

        Query query = getSelectQuery(namedQuery, em, parameters);
        Object obj = jdbcService.executeGet(namedQuery.name(), query, em);
        MonitorConfig monitorConfig = constructBean(namedQuery, obj);

        return monitorConfig;
    }

    private MonitorConfig constructBean(MonitorConfigQuery namedQuery, Object ret, Object... parameters)
            throws JPAExecutorException {
        if (ret == null) {
            throw new JPAExecutorException("Query result is null");
        }
        MonitorConfig bean = new MonitorConfig();
        Object[] arr = (Object[]) ret;
        bean.setId((Long) arr[0]);
        bean.setAgentHost((String) arr[1]);
        bean.setAgentHomeDir((String) arr[2]);
        bean.setConfigDir((String) arr[3]);
        bean.setUploadPath((String) arr[4]);
        bean.setCmasterServer((String) arr[5]);
        bean.setUserName((String) arr[6]);
        bean.setPassword((String) arr[7]);
        bean.setSshPort((Long) arr[8]);
        return bean;
    }
}
