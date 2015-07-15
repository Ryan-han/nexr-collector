package com.nexr.master.jpa;

import com.nexr.master.services.CMasterService;
import com.nexr.master.services.JDBCService;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.ArrayList;
import java.util.List;

public class CollectInfoExecutor {

    public enum CollectInfoQuery {
        UPDAT_COLLECT_INFO,
        DELETE_COLLECT_INFO,
        GET_COLLECT_INFO,
        GET_UNFINISHED_COLLECT_INFO;
    };

    public Query getUpdateQuery(CollectInfoQuery namedQuery, CollectInfo collectInfo, EntityManager em)
            throws JPAExecutorException {
        Query query = em.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case DELETE_COLLECT_INFO:
                query.setParameter("collserver", collectInfo.getCollserver());
                query.setParameter("dirname", collectInfo.getDirname());
                query.setParameter("senddate", collectInfo.getSenddate());
                query.setParameter("hm", collectInfo.getHm());
                break;
            case UPDAT_COLLECT_INFO:
                query.setParameter("hadoopload", collectInfo.getHadoopload());
                query.setParameter("collserver", collectInfo.getCollserver());
                query.setParameter("dirname", collectInfo.getDirname());
                query.setParameter("senddate", collectInfo.getSenddate());
                query.setParameter("hm", collectInfo.getHm());
                break;
            default:
                throw new JPAExecutorException("QueryExecutor cannot set parameters for " + namedQuery.name());
        }
        return query;
    }

    public Query getSelectQuery(CollectInfoQuery namedQuery, EntityManager em, Object... parameters)
            throws JPAExecutorException {
        Query query = em.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case GET_COLLECT_INFO:
                query.setParameter("collserver", parameters[0]);
                query.setParameter("dirname", parameters[1]);
                query.setParameter("senddate", parameters[2]);
                query.setParameter("hm", parameters[3]);
                break;
            case GET_UNFINISHED_COLLECT_INFO:
                query.setParameter("hadoopload", parameters[0]);
                break;
            default:
                throw new JPAExecutorException("QueryExecutor cannot set parameters for " + namedQuery.name());
        }
        return query;
    }

    public int executeUpdate(CollectInfoQuery namedQuery, CollectInfo collectInfo) throws JPAExecutorException {
        JDBCService jdbcService = CMasterService.getInstance().getJdbcService();
        EntityManager em = jdbcService.getEntityManager();
        Query query = getUpdateQuery(namedQuery, collectInfo, em);
        int ret = jdbcService.executeUpdate(namedQuery.name(), query, em);
        return ret;
    }

    public CollectInfo get(CollectInfoQuery namedQuery, Object... parameters) throws JPAExecutorException {
        JDBCService jdbcService = CMasterService.getInstance().getJdbcService();
        EntityManager em = jdbcService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        Object ret = jdbcService.executeGet(namedQuery.name(), query, em);
        if (ret == null) {
            throw new JPAExecutorException(query.toString());
        }
        CollectInfo bean = constructBean(namedQuery, ret, parameters);
        return bean;
    }

    public List<CollectInfo> getList(CollectInfoQuery namedQuery, Object... parameters) throws JPAExecutorException {
        JDBCService jdbcService = CMasterService.getInstance().getJdbcService();
        EntityManager em = jdbcService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        List<?> retList = (List<?>) jdbcService.executeGetList(namedQuery.name(), query, em);
        List<CollectInfo> collectInfoList = new ArrayList<CollectInfo>();
        if (retList != null) {
            for (Object ret : retList) {
                collectInfoList.add(constructBean(namedQuery, ret));
            }
        }
        return collectInfoList;
    }

    public void insert(CollectInfo collectInfo) throws JPAExecutorException{
        JDBCService jdbcService = CMasterService.getInstance().getJdbcService();
        jdbcService.insert(collectInfo);
    }

    private CollectInfo constructBean(CollectInfoQuery namedQuery, Object ret, Object... parameters)
            throws JPAExecutorException {
        CollectInfo bean;
        Object[] arr;
        switch (namedQuery) {
            case GET_UNFINISHED_COLLECT_INFO:
            case GET_COLLECT_INFO:
                bean = new CollectInfo();
                arr = (Object[]) ret;
                bean.setCollserver((String) arr[0]);
                bean.setDirname((String) arr[1]);
                bean.setSenddate((String) arr[2]);
                bean.setHm((String) arr[3]);
                bean.setHadoopload((String) arr[4]);
                break;
            default:
                throw new JPAExecutorException("QueryExecutor cannot construct job bean for " + namedQuery.name());
        }
        return bean;
    }
}
