package com.nexr.interceptor;

import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestDataTypeExtractionInterceptor {

    private static DataTypeExtractorInterceptor interceptor;

    @BeforeClass
    public static void setupClass() {
        DataTypeExtractorInterceptor.Builder builder = new DataTypeExtractorInterceptor.Builder();
        interceptor = (DataTypeExtractorInterceptor)builder.build();
    }

    @AfterClass
    public static void afterClass() {
        interceptor = null;
    }

    @Test
    public void testGetDataTypeFromName() {

        String text = "yongin_LOG_S1AP_201503192356.dat";

        Assert.assertEquals("S1AP", interceptor.extractDataType(text));

        text = "yongin_LOG_S1AP_HTTP_201503192356.dat";

        Assert.assertEquals("S1AP_HTTP", interceptor.extractDataType(text));

    }

    @Test
    public void testGetHm() {

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date date = format.parse("2015-05-27 10:06:01");
            String hm = interceptor.roundDown(date.getTime() +"");
            Assert.assertEquals("1000", hm);

            date = format.parse("2015-05-27 12:25:01");
            hm = interceptor.roundDown(date.getTime() +"");
            Assert.assertEquals("1220", hm);

            date = format.parse("2015-05-27 13:00:01");
            hm = interceptor.roundDown(date.getTime() +"");
            Assert.assertEquals("1300", hm);

            date = format.parse("2015-05-27 14:59:01");
            hm = interceptor.roundDown(date.getTime() +"");
            Assert.assertEquals("1450", hm);



        } catch (Exception e) {

        }



    }
}
