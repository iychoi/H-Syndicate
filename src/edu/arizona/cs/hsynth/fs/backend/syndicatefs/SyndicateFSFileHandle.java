package edu.arizona.cs.hsynth.fs.backend.syndicatefs;

import edu.arizona.cs.hsynth.fs.Path;
import edu.arizona.cs.hsynth.fs.backend.syndicatefs.client.message.SyndicateFSFileInfo;
import java.io.Closeable;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SyndicateFSFileHandle implements Closeable {
    private static final Log LOG = LogFactory.getLog(SyndicateFSFileHandle.class);

    private SyndicateFSFileSystem filesystem;
    private SyndicateFSClientInterface client;
    private SyndicateFSFileStatus status;
    private SyndicateFSFileInfo fileinfo;
    private boolean readonly = false;
    private boolean closed = true;
    private boolean modified = false;
    
    public SyndicateFSFileHandle(SyndicateFSFileSystem fs, SyndicateFSClientInterface client, SyndicateFSFileStatus status, SyndicateFSFileInfo fi, boolean readonly) {
        this.filesystem = fs;
        this.client = client;
        this.status = status;
        this.fileinfo = fi;
        this.readonly = readonly;
        this.closed = false;
        this.modified = false;
    }
    
    public SyndicateFSFileSystem getFileSystem() {
        return this.filesystem;
    }
    
    public SyndicateFSClientInterface getClient() {
        return this.client;
    }
    
    public Path getPath() {
        return this.status.getPath();
    }
    
    public SyndicateFSFileStatus getStatus() {
        return this.status;
    }
    
    public int readFileData(long fileoffset, byte[] buffer, int offset, int size) throws IOException {
        return this.client.readFileData(this.fileinfo, fileoffset, buffer, offset, size);
    }
    
    public void writeFileData(long fileoffset, byte[] buffer, int offset, int size) throws IOException {
        if(this.readonly) {
            throw new IOException("Can not write data to readonly handle");
        }
        
        this.client.writeFileData(this.fileinfo, fileoffset, buffer, offset, size);
        this.status.notifySizeChanged(fileoffset + size);
        this.modified = true;
    }
    
    public void truncate(long fileoffset) throws IOException {
        if(this.readonly) {
            throw new IOException("Can not truncate data to readonly handle");
        }
        
        this.client.truncateFile(this.fileinfo, fileoffset);
        this.status.notifySizeChanged(fileoffset);
        this.modified = true;
    }
    
    public boolean isOpen() {
        return !this.closed;
    }
    
    public void flush() throws IOException {
        if(this.readonly) {
            throw new IOException("Can not flush data to readonly handle");
        }
        
        this.client.flush(this.fileinfo);
    }
    
    public boolean isReadonly() {
        return this.readonly;
    }
    
    @Override
    public void close() throws IOException {
        this.client.closeFileHandle(this.fileinfo);
        this.closed = true;
        
        if(!this.readonly) {
            if(this.modified) {
                this.status.setDirty();
            }
        }
    }
}
