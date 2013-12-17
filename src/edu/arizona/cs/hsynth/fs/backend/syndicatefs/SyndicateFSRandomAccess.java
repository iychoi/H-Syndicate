package edu.arizona.cs.hsynth.fs.backend.syndicatefs;

import edu.arizona.cs.hsynth.fs.HSynthFSRandomAccess;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SyndicateFSRandomAccess implements HSynthFSRandomAccess {
    
    private static final Log LOG = LogFactory.getLog(SyndicateFSRandomAccess.class);
    
    private SyndicateFSFileSystem filesystem;
    private SyndicateFSFileHandle handle;
    private long offset;
    private boolean closed;
    
    SyndicateFSRandomAccess(SyndicateFSFileSystem fs, SyndicateFSConfiguration conf, SyndicateFSFileHandle handle) {
        this.filesystem = fs;
        this.handle = handle;
        
        this.offset = 0;
        this.closed = false;
    }
    
    @Override
    public int read() throws IOException {
        if(this.closed) {
            LOG.error("InputStream is already closed");
            throw new IOException("InputStream is already closed");
        }
        
        byte[] buffer = new byte[1];
        int read = this.handle.readFileData(this.offset, buffer, 0, 1);
        if(read != 1) {
            LOG.error("Read failed");
            throw new IOException("Read failed");
        }
        this.offset++;
        return buffer[0];
    }

    @Override
    public int read(byte[] bytes) throws IOException {
        if(this.closed) {
            LOG.error("InputStream is already closed");
            throw new IOException("InputStream is already closed");
        }
        
        int read = this.handle.readFileData(this.offset, bytes, 0, bytes.length);
        this.offset += read;
        return read;
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        if(this.closed) {
            LOG.error("InputStream is already closed");
            throw new IOException("InputStream is already closed");
        }
        
        int read = this.handle.readFileData(this.offset, bytes, off, len);
        this.offset += read;
        return read;
    }

    @Override
    public int skip(int n) throws IOException {
        if(this.closed) {
            LOG.error("InputStream is already closed");
            throw new IOException("InputStream is already closed");
        }
        
        long size = this.handle.getStatus().getSize();
        if(size > this.offset + n) {
            this.offset += n;
        } else {
            n = (int) (size - this.offset);
            this.offset += n;
        }
        return n;
    }

    @Override
    public long getFilePointer() throws IOException {
        if(this.closed) {
            LOG.error("InputStream is already closed");
            throw new IOException("InputStream is already closed");
        }
        
        return this.offset;
    }

    @Override
    public long length() throws IOException {
        if(this.closed) {
            LOG.error("InputStream is already closed");
            throw new IOException("InputStream is already closed");
        }
        
        return this.handle.getStatus().getSize();
    }

    @Override
    public void seek(long l) throws IOException {
        if(this.closed) {
            LOG.error("InputStream is already closed");
            throw new IOException("InputStream is already closed");
        }
        
        if(l < 0) {
            LOG.error("seek point can not be negative");
            throw new IOException("seek point can not be negative");
        }
        this.offset = l;
    }
    
    @Override
    public void close() throws IOException {
        this.handle.close();
        this.filesystem.notifyRandomAccessClosed(this);
        this.closed = true;
    }
}
