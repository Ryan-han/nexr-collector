package com.nexr.master;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Generator {

    private static int START= 97;
    private static int END = 122;
    private static boolean generating = false;

    private String FILE_NAME = "_LOG_S1AP_";
    private String DEST_DIR = "";

    private static String USAGE = "java com.nexr.master.Generator srcFile uploadDir >>> \n " +
            "java com.nexr.master.Generator /Users/seoeun/libs/LtasSrc1/hehLOG_S1AP_TCP_201503192315.dat.COMPLETED /Users/seoeun/libs/LtasSrc1\n";


    private String srcFile;
    private String uploadDir;



    public Generator(String srcFile, String uploadDir) {
        this.srcFile = srcFile;
        this.uploadDir = uploadDir;

    }

    public void generate(File src, String dest) {
        //hehe_LOG_S1AP_TCP_201503192315.dat.COMPLETED

    }

    public void generate() throws IOException{

        File file = new File(srcFile);

        int base = START;

        while (generating) {
            if (base > END) {
                base = START;
            }
            String name = genName(base++, 2);
            name = name + FILE_NAME + parseTime("yyyyMMddHHmm") + ".dat";
            copyFile(file, name);
            try {
                Thread.sleep(4000);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

    }

    private void copyFile(File src, String name) throws IOException {
        File destFile = new File(uploadDir, name);
        FileInputStream fi = new FileInputStream(src);
        FileOutputStream fo  = new FileOutputStream(destFile);
        System.out.println("---- destFile : " + destFile.getAbsolutePath());
        copy(fi, fo, 4086);
    }

    public static long copy(final InputStream input, final OutputStream output, int buffersize) throws IOException {
        final byte[] buffer = new byte[buffersize];
        int n = 0;
        long count=0;
        while (-1 != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
            count += n;
        }
        return count;
    }

    private String parseTime(String pattern) {
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        String a =  format.format(Calendar.getInstance().getTime());
        return a;
    }

    public String genName(int base, int increment) {
        char aa = (char) base;
        char bb = base + increment >= END ? (char) (base - 25 + increment) : (char) (base + increment);
        return String.valueOf(aa) + String.valueOf(bb);
    }

    public static void main(String... args) {
        if (args.length != 2) {
            System.out.println(USAGE);
            return;
        }
        System.out.println("srcFile : " + args[0]);
        System.out.println("uploadDir : " + args[1]);

        Generator generator = new Generator(args[0], args[1]);
        Generator.generating = true;
        try {
            generator.generate();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

    }
}
