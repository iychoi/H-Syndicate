package edu.arizona.cs.syndicate.fs.client;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SyndicateIPCClient implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(SyndicateIPCClient.class);
    
    private String address;
    private int port;
    private Socket clientSocket;
    private DataInputStream socketDataInputStream;
    private DataOutputStream socketDataOutputStream;
    
    public SyndicateIPCClient(String address, int port) throws InstantiationException {
        this.address = address;
        this.port = port;
        
        try {
            LOG.info("connect to " + address);
            this.clientSocket = new Socket(address, port);
            this.socketDataInputStream = new DataInputStream(this.clientSocket.getInputStream());
            this.socketDataOutputStream = new DataOutputStream(this.clientSocket.getOutputStream());
        } catch (UnknownHostException ex) {
            LOG.error(ex);
            throw new InstantiationException(ex.getMessage());
        } catch (IOException ex) {
            LOG.error(ex);
            throw new InstantiationException(ex.getMessage());
        }
    }

    public String getAddress() {
        return this.address;
    }
    
    public int getPort() {
        return this.port;
    }
    
    public String getHostString() {
        return this.address + ":" + this.port;
    }
    
    @Override
    public synchronized void close() throws IOException {
        this.socketDataInputStream.close();
        this.socketDataOutputStream.close();
        this.clientSocket.close();
    }

    public synchronized Stat getStat(String path) throws IOException {
        // send
        MessageBuilder.sendStringsMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_GET_STAT, path);
        // recv
        try {
            return MessageBuilder.readStatMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_GET_STAT);
        } catch (IOException ex) {
            // rethrow
            throw new IOException(getHostString() + " - " + ex.getMessage(), ex);
        }
    }

    public synchronized void delete(String path) throws IOException {
        // send
        MessageBuilder.sendStringsMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_DELETE, path);
        // recv
        try {
            MessageBuilder.readResultMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_DELETE);
        } catch (IOException ex) {
            // rethrow
            throw new IOException(getHostString() + " - " + ex.getMessage(), ex);
        }
    }

    public synchronized void removeDirectory(String path) throws IOException {
        // send
        MessageBuilder.sendStringsMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_REMOVE_DIRECTORY, path);
        // recv
        try {
            MessageBuilder.readResultMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_REMOVE_DIRECTORY);
        } catch (IOException ex) {
            // rethrow
            throw new IOException(getHostString() + " - " + ex.getMessage(), ex);
        }
    }

    public synchronized void rename(String path, String newPath) throws IOException {
        // send
        MessageBuilder.sendStringsMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_RENAME, path, newPath);
        // recv
        try {
            MessageBuilder.readResultMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_RENAME);
        } catch (IOException ex) {
            // rethrow
            throw new IOException(getHostString() + " - " + ex.getMessage(), ex);
        }
    }

    public synchronized void mkdir(String path) throws IOException {
        // send
        MessageBuilder.sendStringsMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_MKDIR, path);
        // recv
        try {
            MessageBuilder.readResultMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_MKDIR);
        } catch (IOException ex) {
            // rethrow
            throw new IOException(getHostString() + " - " + ex.getMessage(), ex);
        }
    }

    public synchronized String[] readDirectoryEntries(String path) throws IOException {
        // send
        MessageBuilder.sendStringsMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_READ_DIRECTORY, path);
        // recv
        try {
            String[] entries = MessageBuilder.readDirectoryMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_READ_DIRECTORY);
            List<String> entry_arr = new ArrayList<String>();
            for (String entry : entries) {
                if (!entry.equals(".") && !entry.equals("..")) {
                    entry_arr.add(entry);
                }
            }

            //String[] new_entries = new String[entry_arr.size()];
            //new_entries = entry_arr.toArray(new_entries);
            String[] new_entries = entry_arr.toArray(new String[0]);
            return new_entries;
        } catch (IOException ex) {
            // rethrow
            throw new IOException(getHostString() + " - " + ex.getMessage(), ex);
        }
    }

    public synchronized FileInfo getFileHandle(String path) throws IOException {
        // send
        MessageBuilder.sendStringsMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_GET_FILE_HANDLE, path);
        // recv
        try {
            FileInfo fi = MessageBuilder.readFileInfoMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_GET_FILE_HANDLE);
            return fi;
        } catch (IOException ex) {
            // rethrow
            throw new IOException(getHostString() + " - " + ex.getMessage(), ex);
        }
    }

    public synchronized Stat createNewFile(String path) throws IOException {
        // send
        MessageBuilder.sendStringsMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_CREATE_NEW_FILE, path);
        // recv
        try {
            return MessageBuilder.readStatMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_CREATE_NEW_FILE);
        } catch (IOException ex) {
            // rethrow
            throw new IOException(getHostString() + " - " + ex.getMessage(), ex);
        }
    }

    public synchronized int readFileData(FileInfo fileinfo, long fileoffset, byte[] buffer, int offset, int size) throws IOException {
        // send
        MessageBuilder.sendFileReadMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_READ_FILEDATA, fileinfo, fileoffset, size);
        // recv
        try {
            return MessageBuilder.readFileData(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_READ_FILEDATA, buffer, offset);
        } catch (IOException ex) {
            // rethrow
            throw new IOException(getHostString() + " - " + ex.getMessage(), ex);
        }
    }

    public synchronized void writeFileData(FileInfo fileinfo, long fileoffset, byte[] buffer, int offset, int size) throws IOException {
        // send
        MessageBuilder.sendFileWriteMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_WRITE_FILEDATA, fileinfo, fileoffset, buffer, offset, size);
        // recv
        try {
            MessageBuilder.readResultMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_WRITE_FILEDATA);
        } catch (IOException ex) {
            // rethrow
            throw new IOException(getHostString() + " - " + ex.getMessage(), ex);
        }
    }

    public synchronized void flush(FileInfo fileinfo) throws IOException {
        // send
        MessageBuilder.sendFileInfoMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_FLUSH, fileinfo);
        // recv
        try {
            MessageBuilder.readResultMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_FLUSH);
        } catch (IOException ex) {
            // rethrow
            throw new IOException(getHostString() + " - " + ex.getMessage(), ex);
        }
    }

    public synchronized void closeFileHandle(FileInfo fileinfo) throws IOException {
        // send
        MessageBuilder.sendFileInfoMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_CLOSE_FILE_HANDLE, fileinfo);
        // recv
        try {
            MessageBuilder.readResultMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_CLOSE_FILE_HANDLE);
        } catch (IOException ex) {
            // rethrow
            throw new IOException(getHostString() + " - " + ex.getMessage(), ex);
        }
    }
    
    public synchronized void truncateFile(FileInfo fileinfo, long fileoffset) throws IOException {
        // send
        MessageBuilder.sendFileTruncateMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_TRUNCATE_FILE, fileinfo, fileoffset);
        // recv
        try {
            MessageBuilder.readResultMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_TRUNCATE_FILE);
        } catch (IOException ex) {
            // rethrow
            throw new IOException(getHostString() + " - " + ex.getMessage(), ex);
        }
    }

    public synchronized String[] listExtendedAttr(String path) throws IOException {
        // send
        MessageBuilder.sendStringsMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_LIST_EXTENDED_ATTR, path);
        // recv
        try {
            return MessageBuilder.readStringsMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_LIST_EXTENDED_ATTR);
        } catch (IOException ex) {
            // rethrow
            throw new IOException(getHostString() + " - " + ex.getMessage(), ex);
        }
    }
    
    public synchronized String getExtendedAttr(String path, String name) throws IOException {
        // send
        MessageBuilder.sendStringsMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_GET_EXTENDED_ATTR, path, name);
        // recv
        try {
            return MessageBuilder.readStringMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_GET_EXTENDED_ATTR);
        } catch (IOException ex) {
            // rethrow
            throw new IOException(getHostString() + " - " + ex.getMessage(), ex);
        }
    }
}
