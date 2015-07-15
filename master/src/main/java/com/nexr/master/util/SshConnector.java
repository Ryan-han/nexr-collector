package com.nexr.master.util;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Created by ndap on 15. 7. 8.
 */
public interface SshConnector {
    int DEFAULT_PORT = 22;
    int DEFAULT_TIMEOUT = 60;

    void auth(String username, String password);

    void connect(String connectionIP, int connectionPort, int timeout) throws Exception;

    int sendCommand(String command) throws Exception;

    void get(String sourceDir, String filename, OutputStream out) throws Exception;

    void put(String targetDir, String filename, InputStream input) throws Exception;

    boolean isExist(String path, String fileName) throws Exception;

    void close();
}
