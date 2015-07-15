package com.nexr.master.jpa;

import org.json.simple.JSONObject;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

@Entity
@NamedQueries({

        @NamedQuery(name = "UPDAT_COLLECT_INFO", query = "update CollectInfo a set a.hadoopload = :hadoopload "
                + "where a.collserver = :collserver and a.dirname = :dirname and a.senddate = :senddate and a.hm = :hm"),
        @NamedQuery(name = "DELETE_COLLECT_INFO", query = "delete from CollectInfo a where a.collserver = :collserver " +
                "and a.dirname = :dirname and a.senddate = :senddate and a.hm = :hm"),
        @NamedQuery(name = "GET_COLLECT_INFO", query = "select a.collserver, a.dirname, a.senddate, a.hm, a.hadoopload " +
                "from CollectInfo a where a.collserver = :collserver AND a.dirname = :dirname " +
                "AND a.senddate = :senddate AND a.hm = :hm"),
        @NamedQuery(name = "GET_UNFINISHED_COLLECT_INFO", query = "select a.collserver, a.dirname, a.senddate, a.hm, a.hadoopload " +
                "from CollectInfo a where a.hadoopload = :hadoopload order by a.senddate desc")

})
@Table(name = "COLLECT_INFO")
public class CollectInfo {


    @Column(name = "collserver", length = 40)
    private String collserver;
    @Column(name = "dirname", length = 40)
    private String dirname;
    @Column(name = "senddate", length = 8)
    private String senddate;
    @Column(name = "hm", length = 4)
    private String hm;
    @Column(name = "hadoopload", length = 1)

    private String hadoopload;

    public static String START = "I";
    public static String END = "Y";

    public String getCollserver() {
        return collserver;
    }

    public void setCollserver(String collserver) {
        this.collserver = collserver;
    }

    public String getDirname() {
        return dirname;
    }

    public void setDirname(String dirname) {
        this.dirname = dirname;
    }

    public String getSenddate() {
        return senddate;
    }

    public void setSenddate(String senddate) {
        this.senddate = senddate;
    }

    public String getHm() {
        return hm;
    }

    public void setHm(String hm) {
        this.hm = hm;
    }

    public String getHadoopload() {
        return hadoopload;
    }

    public void setHadoopload(String hadoopload) {
        this.hadoopload = hadoopload;
    }

    public JSONObject toJsonOblect() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("collserver", collserver);
        jsonObject.put("dirname", dirname);
        jsonObject.put("senddate", senddate);
        jsonObject.put("hm", hm);
        jsonObject.put("hadoopload", hadoopload);
        return jsonObject;
    }

    public String toString() {
        return toJsonOblect().toString();
    }

    public Object[] toParams() {
        Object[] params = new java.lang.Object[]{getCollserver(), getDirname(), getSenddate(), getHm(), getHadoopload()};
        return params;
    }
}
