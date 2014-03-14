package edu.arizona.cs.syndicate.fs;

import edu.arizona.cs.syndicate.fs.client.FileInfo;
import java.io.Closeable;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SyndicateFSFileHandle implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(SyndicateFSFileHandle.class);

    private SyndicateFileSystem filesystem;
    private SyndicateFSFileStatus status;
    private FileInfo fileinfo;
    private boolean readonly = false;
    private boolean closed = true;
    private boolean modified = false;
    
    SyndicateFSFileHandle(SyndicateFileSystem fs, SyndicateFSFileStatus status, FileInfo fi, boolean readonly) {
        this.filesystem = fs;
        this.status = status;
        this.fileinfo = fi;
        this.readonly = readonly;
        this.closed = false;
        this.modified = false;
    }
    
    public SyndicateFileSystem getFileSystem() {
        return this.filesystem;
    }
    
    public SyndicateFSPath getPath() {
        return this.status.getPath();
    }
    
    public SyndicateFSFileStatus getStatus() {
        return this.status;
    }
    
    public int readFileData(long fileoffset, byte[] buffer, int offset, int size) throws IOException {
        return this.filesystem.getIPCClient().readFileData(this.fileinfo, fileoffset, buffer, offset, size);
    }
    
    public void writeFileData(long fileoffset, byte[] buffer, int offset, int size) throws IOException {
        if(this.readonly) {
            throw new IOException("Can not write data to readonly handle");
        }
        
        this.filesystem.getIPCClient().writeFileData(this.fileinfo, fileoffset, buffer, offset, size);
        this.status.notifySizeChanged(fileoffset + size);
        this.modified = true;
    }
    
    public void truncate(long fileoffset) throws IOException {
        if(this.readonly) {
            throw new IOException("Can not truncate data to readonly handle");
        }
        
        this.filesystem.getIPCClient().truncateFile(this.fileinfo, fileoffset);
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
        
        this.filesystem.getIPCClient().flush(this.fileinfo);
    }
    
    public boolean isReadonly() {
        return this.readonly;
    }
    
    @Override
    public void close() throws IOException {
        this.filesystem.getIPCClient().closeFileHandle(this.fileinfo);
        this.closed = true;
        
        if(!this.readonly) {
            if(this.modified) {
                this.status.setDirty();
            }
        }
    }
}
