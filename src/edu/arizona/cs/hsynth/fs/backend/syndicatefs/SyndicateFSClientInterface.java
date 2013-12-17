package edu.arizona.cs.hsynth.fs.backend.syndicatefs;

import edu.arizona.cs.hsynth.fs.backend.syndicatefs.client.message.MessageBuilder;
import edu.arizona.cs.hsynth.fs.backend.syndicatefs.client.message.SyndicateFSFileInfo;
import edu.arizona.cs.hsynth.fs.backend.syndicatefs.client.message.SyndicateFSStat;
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

public class SyndicateFSClientInterface implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(SyndicateFSClientInterface.class);
    
    private Socket clientSocket;
    private DataInputStream socketDataInputStream;
    private DataOutputStream socketDataOutputStream;
    
    public SyndicateFSClientInterface(String address, int port) throws InstantiationException {
        try {
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

    @Override
    public synchronized void close() throws IOException {
        this.socketDataInputStream.close();
        this.socketDataOutputStream.close();
        this.clientSocket.close();
    }

    public synchronized SyndicateFSStat getStat(String path) throws IOException {
        // send
        MessageBuilder.sendStringsMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_GET_STAT, path);
        // recv
        return MessageBuilder.readStatMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_GET_STAT);
    }

    public synchronized void delete(String path) throws IOException {
        // send
        MessageBuilder.sendStringsMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_DELETE, path);
        // recv
        MessageBuilder.readResultMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_DELETE);
    }

    public synchronized void removeDirectory(String path) throws IOException {
        // send
        MessageBuilder.sendStringsMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_REMOVE_DIRECTORY, path);
        // recv
        MessageBuilder.readResultMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_REMOVE_DIRECTORY);
    }

    public synchronized void rename(String path, String newPath) throws IOException {
        // send
        MessageBuilder.sendStringsMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_RENAME, path, newPath);
        // recv
        MessageBuilder.readResultMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_RENAME);
    }

    public synchronized void mkdir(String path) throws IOException {
        // send
        MessageBuilder.sendStringsMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_MKDIR, path);
        // recv
        MessageBuilder.readResultMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_MKDIR);
    }

    public synchronized String[] readDirectoryEntries(String path) throws IOException {
        // send
        MessageBuilder.sendStringsMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_READ_DIRECTORY, path);
        // recv
        String[] entries = MessageBuilder.readDirectoryMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_READ_DIRECTORY);
        List<String> entry_arr = new ArrayList<String>();
        for(String entry : entries) {
            if(!entry.equals(".") && !entry.equals("..")) {
                entry_arr.add(entry);    
            }
        }
        
        //String[] new_entries = new String[entry_arr.size()];
        //new_entries = entry_arr.toArray(new_entries);
        String[] new_entries = entry_arr.toArray(new String[0]);
        return new_entries;
    }

    public synchronized SyndicateFSFileInfo getFileHandle(String path) throws IOException {
        // send
        MessageBuilder.sendStringsMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_GET_FILE_HANDLE, path);
        // recv
        SyndicateFSFileInfo fi = MessageBuilder.readFileInfoMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_GET_FILE_HANDLE);
        return fi;
    }

    public synchronized SyndicateFSStat createNewFile(String path) throws IOException {
        // send
        MessageBuilder.sendStringsMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_CREATE_NEW_FILE, path);
        // recv
        return MessageBuilder.readStatMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_CREATE_NEW_FILE);
    }

    public synchronized int readFileData(SyndicateFSFileInfo fileinfo, long fileoffset, byte[] buffer, int offset, int size) throws IOException {
        // send
        MessageBuilder.sendFileReadMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_READ_FILEDATA, fileinfo, fileoffset, size);
        // recv
        return MessageBuilder.readFileData(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_READ_FILEDATA, buffer, offset);
    }

    public synchronized void writeFileData(SyndicateFSFileInfo fileinfo, long fileoffset, byte[] buffer, int offset, int size) throws IOException {
        // send
        MessageBuilder.sendFileWriteMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_WRITE_FILEDATA, fileinfo, fileoffset, buffer, offset, size);
        // recv
        MessageBuilder.readResultMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_WRITE_FILEDATA);
    }

    public synchronized void flush(SyndicateFSFileInfo fileinfo) throws IOException {
        // send
        MessageBuilder.sendFileInfoMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_FLUSH, fileinfo);
        // recv
        MessageBuilder.readResultMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_FLUSH);
    }

    public synchronized void closeFileHandle(SyndicateFSFileInfo fileinfo) throws IOException {
        // send
        MessageBuilder.sendFileInfoMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_CLOSE_FILE_HANDLE, fileinfo);
        // recv
        MessageBuilder.readResultMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_CLOSE_FILE_HANDLE);
    }
    
    public synchronized void truncateFile(SyndicateFSFileInfo fileinfo, long fileoffset) throws IOException {
        // send
        MessageBuilder.sendFileTruncateMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_TRUNCATE_FILE, fileinfo, fileoffset);
        // recv
        MessageBuilder.readResultMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_TRUNCATE_FILE);
    }

    public synchronized byte[] getExtendedAttr(String path) throws IOException {
        // send
        MessageBuilder.sendStringsMessage(this.socketDataOutputStream, MessageBuilder.MessageOperation.OP_GET_EXTENDED_ATTR, path);
        // recv
        return MessageBuilder.readBytesMessage(this.socketDataInputStream, MessageBuilder.MessageOperation.OP_GET_EXTENDED_ATTR);
    }
}
