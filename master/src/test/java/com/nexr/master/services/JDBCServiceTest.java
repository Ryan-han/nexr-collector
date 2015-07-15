package com.nexr.master.services;


import com.nexr.master.jpa.JPAExecutorException;
import com.nexr.master.jpa.CollectInfoExecutor;
import com.nexr.master.jpa.CollectInfo;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class JDBCServiceTest {

    private static JDBCService jdbcService;

    @BeforeClass
    public static void setupClass() {
        try {
            jdbcService = new JDBCService();
            initJDBConfiguration();
            jdbcService.start();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @AfterClass
    public static void tearDown() {
        jdbcService.shutdown();
        jdbcService=null;
    }

    private static void initJDBConfiguration() {
        Configuration conf = CMasterContext.getContext().getConfiguration();
        conf.set(JDBCService.CONF_URL, "jdbc:derby:memory:myDB;create=true");
        conf.set(JDBCService.CONF_DRIVER, "org.apache.derby.jdbc.EmbeddedDriver");
    }

    @Test
    public void testInsert() {
        CollectInfo xtas = new CollectInfo();
        xtas.setCollserver("tserver");
        xtas.setDirname("TCP");
        xtas.setSenddate("20150317");
        xtas.setHm("1010");
        xtas.setHadoopload("I");

        try {
            CollectInfoExecutor queryExecutor = new CollectInfoExecutor();
            queryExecutor.insert(xtas);

            CollectInfo xtas1 = queryExecutor.get(CollectInfoExecutor.CollectInfoQuery.GET_COLLECT_INFO, xtas.toParams());
            Assert.assertEquals(xtas.toJsonOblect().toJSONString(), xtas1.toJsonOblect().toJSONString());

            System.out.println("Inserted xtas : " + xtas1.toJsonOblect().toJSONString());

            Object[] params = new java.lang.Object[]{"I"};
            List<CollectInfo> xtasList = queryExecutor.getList(CollectInfoExecutor.CollectInfoQuery.GET_UNFINISHED_COLLECT_INFO, params);
            Assert.assertEquals(1, xtasList.size());

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testUpdate() {
        try {
            CollectInfoExecutor queryExecutor = new CollectInfoExecutor();
            CollectInfo xtas = new CollectInfo();
            xtas.setCollserver("tserver");
            xtas.setDirname("TCP");
            xtas.setSenddate("20150317");
            xtas.setHm("1010");
            xtas.setHadoopload("Y");
            int ret = queryExecutor.executeUpdate(CollectInfoExecutor.CollectInfoQuery.UPDAT_COLLECT_INFO, xtas);

            Assert.assertEquals(1, ret);

            ret = queryExecutor.executeUpdate(CollectInfoExecutor.CollectInfoQuery.UPDAT_COLLECT_INFO, xtas);
            Assert.assertEquals(1, ret);

        } catch (JPAExecutorException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGet() {
        try {
            //tserver      | hdfs://seair:8020/user/seoeun/flume | 20150317 | 1010 | I
            CollectInfoExecutor queryExecutor = new CollectInfoExecutor();
            Object[] params = new java.lang.Object[]{"tserver", "TCP", "20150317", "1010"};
            CollectInfo xtas = queryExecutor.get(CollectInfoExecutor.CollectInfoQuery.GET_COLLECT_INFO, params);

            Assert.assertNotNull(xtas);
            System.out.println("Get xtas : " + xtas.toJsonOblect().toJSONString());

        } catch (JPAExecutorException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetNegative() {
        try {
            //tserver      | hdfs://seair:8020/user/seoeun/flume | 20150317 | 1010 | I
            CollectInfoExecutor queryExecutor = new CollectInfoExecutor();
            Object[] params = new java.lang.Object[]{"tserver", "TCP", "20150317", "2020"};
            CollectInfo xtas = queryExecutor.get(CollectInfoExecutor.CollectInfoQuery.GET_COLLECT_INFO, params);

            Assert.fail("Should there no xtas");
        } catch (JPAExecutorException e) {
            //e.printStackTrace();
        }
    }

    @Test
    public void testGetXtas() {
        try {
            //tserver      | hdfs://seair:8020/user/seoeun/flume | 20150317 | 1010 | I
            CollectInfoExecutor queryExecutor = new CollectInfoExecutor();
            Object[] params = new java.lang.Object[]{"Y"};
            List<CollectInfo> xtasList = queryExecutor.getList(CollectInfoExecutor.CollectInfoQuery.GET_UNFINISHED_COLLECT_INFO, params);
            Assert.assertEquals(1, xtasList.size());

            params = new java.lang.Object[]{"I"};
            xtasList = queryExecutor.getList(CollectInfoExecutor.CollectInfoQuery.GET_UNFINISHED_COLLECT_INFO, params);
            Assert.assertEquals(0, xtasList.size());

        } catch (JPAExecutorException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDelete() {
        CollectInfo xtas = new CollectInfo();
        xtas.setCollserver("tserver");
        xtas.setDirname("TCP");
        xtas.setSenddate("20150516");
        xtas.setHm("2010");
        xtas.setHadoopload("I");

        try {
            CollectInfoExecutor queryExecutor = new CollectInfoExecutor();

            int ret = queryExecutor.executeUpdate(CollectInfoExecutor.CollectInfoQuery.DELETE_COLLECT_INFO, xtas);
            Assert.assertEquals(0, ret);


            queryExecutor.insert(xtas);

            CollectInfo xtas1 = queryExecutor.get(CollectInfoExecutor.CollectInfoQuery.GET_COLLECT_INFO, xtas.toParams());
            Assert.assertEquals(xtas.toJsonOblect().toJSONString(), xtas1.toJsonOblect().toJSONString());

            System.out.println("Inserted xtas : " + xtas1.toJsonOblect().toJSONString());

            Object[] params = new java.lang.Object[]{"I"};
            List<CollectInfo> xtasList = queryExecutor.getList(CollectInfoExecutor.CollectInfoQuery.GET_UNFINISHED_COLLECT_INFO, params);
            Assert.assertEquals(1, xtasList.size());

            params = new java.lang.Object[]{"tserver", "TCP", "20150516", "2010"};
            CollectInfo xtas2 = queryExecutor.get(CollectInfoExecutor.CollectInfoQuery.GET_COLLECT_INFO, params);

            Assert.assertNotNull(xtas2);

            ret = queryExecutor.executeUpdate(CollectInfoExecutor.CollectInfoQuery.DELETE_COLLECT_INFO, xtas1);
            Assert.assertEquals(1, ret);

            try {
                params = new java.lang.Object[]{"tserver", "TCP", "20150516", "2010"};
                xtas2 = queryExecutor.get(CollectInfoExecutor.CollectInfoQuery.GET_COLLECT_INFO, params);
                Assert.fail("Should no xtas");
            } catch (JPAExecutorException e) {

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
