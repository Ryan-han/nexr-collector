package com.nexr.master;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by ndap on 15. 7. 14.
 */
public class Test {
    public static void main(String[] args) {
        String uploadedPath = "hdfs://nexr.ryan.com:8020/user/ndap/nexr.ryan.com/TYPE1/1810/access_log.2012-12-14.1.1436825657682.tmp";
//        String text = "hdfs://seair:8020/user/seoeun/Ltas/seair.nexr.com/S1AP/1020/yongin_LOG_S1AP_201503192315.dat.1.1431566978097.tmp";
        test(uploadedPath);
//        test(text);
    }

    private static void test(String uploadedPath) {
        String patternString1 = "(hdfs://)(.*)(/.*.dat)(.*)";
        Pattern pattern = Pattern.compile(patternString1);
        Matcher matcher = pattern.matcher(uploadedPath);
        if (matcher.find()) {
            String fileName = matcher.group(3);
            System.out.println("==> " + fileName);
            System.out.println(matcher.group(1));
            System.out.println(matcher.group(2));
            System.out.println(matcher.group(3));
            System.out.println(matcher.group(4));
        } else {
            patternString1 = "(hdfs://)(.*)(/.*.*)(.*)";
            pattern = Pattern.compile(patternString1);
            matcher = pattern.matcher(uploadedPath);
            if (matcher.find()) {
                String fileName = matcher.group(3);
                System.out.println("==> " + fileName);


                String[] ns = fileName.split("\\.");
                System.out.println(ns.length);
                String result = "";
                for (int j = 0; j<=ns.length-4; j++) {
                    System.out.println(ns[j]);
                   result += ns[j] + ".";
                }
                System.out.println("result => " + result.substring(0, result.length()-1));
            }
        }
    }
}
