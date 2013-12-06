package edu.arizona.cs.hsynth.fs.backend.syndicatefs;

import edu.arizona.cs.hsynth.fs.HSynthFSInputStream;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SyndicateFSInputStream extends HSynthFSInputStream {

    private static final Log LOG = LogFactory.getLog(SyndicateFSInputStream.class);
    
    private SyndicateFSFileSystem filesystem;
    private SyndicateFSFileHandle handle;
    private long offset;
    private boolean closed;
    
    SyndicateFSInputStream(SyndicateFSFileSystem fs, SyndicateFSFileHandle handle) {
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
    public long skip(long n) throws IOException {
        if(this.closed) {
            LOG.error("InputStream is already closed");
            throw new IOException("InputStream is already closed");
        }
        
        long size = this.handle.getStatus().getSize();
        if(size > this.offset + n) {
            this.offset += n;
        } else {
            n = size - this.offset;
            this.offset = size;
        }
        return n;
    }
    
    @Override
    public int available() throws IOException {
        if(this.closed) {
            LOG.error("InputStream is already closed");
            throw new IOException("InputStream is already closed");
        }
        
        long size = this.handle.getStatus().getSize();
        long diff = size - this.offset;
        
        if(diff > this.handle.getStatus().getBlockSize()) {
            return (int) this.handle.getStatus().getBlockSize();
        } else {
            return (int) diff;
        }
    }
    
    @Override
    public void close() throws IOException {
        this.handle.close();
        this.filesystem.notifyInputStreamClosed(this);
        this.closed = true;
    }
    
    @Override
    public synchronized void mark(int readlimit) {
    }
    
    @Override
    public synchronized void reset() throws IOException {
        if(this.closed) {
            LOG.error("InputStream is already closed");
            throw new IOException("InputStream is already closed");
        }
    }
    
    @Override
    public boolean markSupported() {
        return false;
    }
}
