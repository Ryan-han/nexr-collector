package com.nexr.master.util;

import com.jcraft.jsch.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Vector;

/**
 * Created by ndap on 15. 7. 8.
 */
public class SshManager implements SshConnector {

    private static final Logger LOG = LoggerFactory.getLogger(SshManager.class);

    private JSch jschSSHChannel;

    private String host;
    private int connectionPort;
    private Session session;

    private String username;
    private String password;


    public SshManager() {
        jschSSHChannel = new JSch();
    }

    public void auth(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public void connect(String host, int connectionPort, int timeout) throws Exception {
        if (session != null) {
            throw new Exception("already connected!");
        }

        this.host = host;
        this.connectionPort = connectionPort;

        try {
                session = jschSSHChannel.getSession(username, this.host, this.connectionPort);
                session.setPassword(password);

                session.setConfig("StrictHostKeyChecking", "no");
                session.connect(timeout);
        } catch (JSchException e) {
            e.printStackTrace();
        }
    }

    public int sendCommand(String command) throws Exception {
        if (session == null) {
            throw new Exception("connect first!");
        }

        ChannelExec channel = null;
        try {
            channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(command);
            channel.connect();

            while (true) {
                if (channel.isClosed()) {
                    break;
                }
                Thread.sleep(500);
            }
            return channel.getExitStatus();
        } catch (Exception e) {
            throw new Exception(e);
        } finally {
            if (channel != null) {
                channel.disconnect();
            }
        }
    }

    public void get(String sourceDir, String filename, OutputStream out) throws Exception {
        if (session == null) {
            throw new Exception("connect first!");
        }

        ChannelSftp channel = null;
        try {
            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect();
            channel.cd(sourceDir);
            channel.get(filename, out);
        } catch (Exception e) {
            throw new Exception(e);
        } finally {
            if (channel != null) {
                channel.disconnect();
            }
        }

    }

    public void put(String targetDir, String filename, InputStream input) throws Exception {
        if (session == null) {
            throw new Exception("connect first!");
        }

        ChannelSftp channel = null;
        try {
            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect();
            channel.cd(targetDir);
            channel.put(input, filename);
        } catch (Exception e) {
            throw new Exception(e);
        } finally {
            if (channel != null) {
                channel.disconnect();
            }
        }
    }

    public boolean isExist(String path, String fileName) throws Exception {
        if (session == null) {
            throw new Exception("connect first!");
        }


        ChannelSftp channel = null;
        channel = (ChannelSftp) session.openChannel("sftp");
        channel.connect();

        channel.cd(path);
        Vector v = channel.ls(path);
        if (v != null) {
            for (int ii = 0; ii < v.size(); ii++) {

                Object obj = v.elementAt(ii);
                if (obj instanceof com.jcraft.jsch.ChannelSftp.LsEntry) {
                    String file = ((com.jcraft.jsch.ChannelSftp.LsEntry) obj).getFilename();
                    if (file.equals(file)) {
                        return true;
                    }

                }
            }
        }
        return false;
    }

    public void close() {
        if (session != null) {
            session.disconnect();
            session = null;
        }
    }
}
