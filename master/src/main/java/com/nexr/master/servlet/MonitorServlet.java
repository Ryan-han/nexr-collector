package com.nexr.master.servlet;

import com.nexr.master.jpa.JPAExecutorException;
import com.nexr.master.jpa.CollectInfoExecutor;
import com.nexr.master.jpa.CollectInfo;
import org.json.simple.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

public class MonitorServlet extends HttpServlet {

    private static Logger LOG = LoggerFactory.getLogger(MonitorServlet.class);

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String hadoopload = request.getParameter("hadoopload");
        LOG.debug("hadoopload {} ", hadoopload);
        if (hadoopload == null || hadoopload.equals("")) {
            hadoopload = CollectInfo.START;
        }

        try {
            String jsonString = getUnfinishedCollectInfo();

            response.setContentType("application/json");
            response.getOutputStream().write(jsonString.getBytes());
            response.setStatus(HttpServletResponse.SC_OK);

        } catch (Exception e) {
            LOG.warn("Can not get UnFinished CollectInfo ", e);
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);

        }

    }

    private String getUnfinishedCollectInfo() {
        JSONArray jsonObject = new JSONArray();
        try {
            CollectInfoExecutor queryExecutor = new CollectInfoExecutor();
            List<CollectInfo> collectInfoList = queryExecutor.getList(CollectInfoExecutor.CollectInfoQuery.GET_UNFINISHED_COLLECT_INFO, new Object[]{CollectInfo.START});
            for (CollectInfo collectInfo: collectInfoList) {
                jsonObject.add(collectInfo.toJsonOblect());
            }
        } catch (JPAExecutorException e) {
            LOG.error("Fail to get UnFinishedCollectInfo", e);
        }
        return jsonObject.toJSONString();
    }
}
