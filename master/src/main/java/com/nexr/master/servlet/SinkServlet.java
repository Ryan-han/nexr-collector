package com.nexr.master.servlet;

import com.nexr.master.CollectorException;
import com.nexr.master.jpa.JPAExecutorException;
import com.nexr.master.jpa.CollectInfoExecutor;
import com.nexr.master.jpa.CollectInfo;
import com.nexr.master.services.CMasterService;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class SinkServlet extends HttpServlet {

    private static Logger LOG = LoggerFactory.getLogger(SinkServlet.class);

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String collectServer = request.getParameter("collectServer");
        response.setContentType("text/html");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("<h1>GET SinkServlet, Hello : " + collectServer +", What are you doing here? </h1>");
        response.getWriter().println("session="+request.getSession(true).getId());
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException,IOException {
        String body = writeBody(request.getInputStream());

        try {
            updateCollectinfo(body);

            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
        } catch (Exception e) {
            LOG.warn("Can not create/update Collect Info " + body, e);
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        }
    }

    private String writeBody(InputStream inputStream) throws IOException{
        StringWriter writer = new StringWriter();
        InputStreamReader reader = new InputStreamReader(inputStream);
        char[] buffer = new char[1024];
        int n = 0;
        while (-1 != (n = reader.read(buffer))) {
            writer.write(buffer, 0, n);
        }
        return writer.toString();
    }

    public void updateCollectinfo(String jsonString) throws IOException, ParseException, CollectorException {
        LOG.debug("Start updateCollectinfo to DB : " + jsonString);
        CollectInfoExecutor queryExecutor = new CollectInfoExecutor();
        //{"status":"END","hm":"0850","collserver":"seair.nexr.com","senddate":"20150316","dirname":"TCP"}
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = (JSONObject)parser.parse(new StringReader(jsonString));

        CollectInfo collectInfo = new CollectInfo();
        collectInfo.setCollserver(jsonObject.get("collserver").toString());
        collectInfo.setDirname(jsonObject.get("dirname").toString());
        collectInfo.setSenddate(jsonObject.get("senddate").toString());
        collectInfo.setHm(jsonObject.get("hm").toString());

        if (jsonObject.get("status").toString().equals("START")) {
            try {
                collectInfo.setHadoopload(CollectInfo.START);
                queryExecutor.insert(collectInfo);
                LOG.info("Insert Succeed, collectInfo {}", collectInfo);
            } catch (JPAExecutorException e) {
                CollectInfo collectInfo1 = queryExecutor.get(CollectInfoExecutor.CollectInfoQuery.GET_COLLECT_INFO, collectInfo.toParams());
                if (collectInfo1 != null) {
                    LOG.warn("collectInfo already exist : " + collectInfo1.toString());
                    if (collectInfo1.getHadoopload().equals(CollectInfo.END)) {
                        collectInfo1.setHadoopload(CollectInfo.START);
                        try {
                            queryExecutor.executeUpdate(CollectInfoExecutor.CollectInfoQuery.UPDAT_COLLECT_INFO, collectInfo1);
                            LOG.warn("collectInfo hadoop upload is [Y], update to [I]");
                        } catch (JPAExecutorException e1) {
                            LOG.warn("Could not update " + collectInfo.toString() + ", retry in 60 seconds", e);
                            CMasterService.getInstance().getQueueService().schedule(
                                    createUpldateCallable(CollectInfoExecutor.CollectInfoQuery.UPDAT_COLLECT_INFO, collectInfo1), 60,
                                    TimeUnit.SECONDS);
                        }
                    }
                } else {
                    throw new JPAExecutorException("Could not insert collectInfo : " + collectInfo.toString(), e);
                }
            }

        } else if (jsonObject.get("status").toString().equals("END")){
            collectInfo.setHadoopload(CollectInfo.END);
            try {
                int ret = queryExecutor.executeUpdate(CollectInfoExecutor.CollectInfoQuery.UPDAT_COLLECT_INFO, collectInfo);
                LOG.info("Update Succeed, collectInfo {}, count {}", collectInfo, ret);
                if (ret == 0) {
                    queryExecutor.insert(collectInfo);
                    LOG.info("Insert {} ", collectInfo);
                }
            } catch (JPAExecutorException e) {
                LOG.warn("Could not update " + collectInfo.toString() + ", retry in 60 seconds", e);
                CMasterService.getInstance().getQueueService().schedule(
                        createUpldateCallable(CollectInfoExecutor.CollectInfoQuery.UPDAT_COLLECT_INFO, collectInfo), 60, TimeUnit.SECONDS);
            }
        } else if (jsonObject.get("status").toString().equals("DELETE")) {
            try {
                int ret = queryExecutor.executeUpdate(CollectInfoExecutor.CollectInfoQuery.DELETE_COLLECT_INFO, collectInfo);
                LOG.info("Delete Succeed, collectInfo {}, count {}", collectInfo, ret);
            } catch (JPAExecutorException e) {
                LOG.warn("Could not delete" + collectInfo.toString() + ", retry in 60 seconds", e);
                CMasterService.getInstance().getQueueService().schedule(
                        createUpldateCallable(CollectInfoExecutor.CollectInfoQuery.DELETE_COLLECT_INFO, collectInfo), 60, TimeUnit.SECONDS);
            }

        }

    }

    private Callable<Void> createUpldateCallable(CollectInfoExecutor.CollectInfoQuery collectInfoQuery, CollectInfo collectInfo) {
        final CollectInfoExecutor.CollectInfoQuery query = collectInfoQuery;
        final CollectInfo collectInfoObj = collectInfo;
        Callable<Void> callable = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try {
                    CollectInfoExecutor queryExecutor = new CollectInfoExecutor();
                    queryExecutor.executeUpdate(query, collectInfoObj);
                    LOG.info("UpdateQuery succeed, " + collectInfoObj.toString());
                } catch (JPAExecutorException e) {
                    LOG.error("Fail to updateQuery " + collectInfoObj.toString(), e);
                }
                return null;
            }
        };
        return callable;
    }

}
