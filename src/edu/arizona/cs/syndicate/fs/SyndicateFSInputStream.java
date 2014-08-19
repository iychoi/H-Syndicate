package edu.arizona.cs.syndicate.fs;

import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SyndicateFSInputStream extends InputStream {

    private static final Log LOG = LogFactory.getLog(SyndicateFSInputStream.class);
    
    private SyndicateFSFileHandle handle;
    private long offset;
    private boolean closed;
    
    SyndicateFSInputStream(SyndicateFSFileHandle handle) {
        this.handle = handle;
        
        this.offset = 0;
        this.closed = false;
    }
    
    @Override
    public synchronized int read() throws IOException {
        if(this.closed) {
            LOG.error("InputStream is already closed");
            throw new IOException("InputStream is already closed");
        }
        
        int readLen = (int) Math.min(this.handle.getStatus().getSize() - this.offset, 1);
        if(readLen > 0) {
            byte[] buffer = new byte[1];
            int read = this.handle.readFileData(this.offset, buffer, 0, 1);
            if(read == 0) {
                // EOF
                return -1;
            }
            if(read != 1) {
                LOG.error("Read failed");
                throw new IOException("Read failed");
            }
            this.offset++;
            return buffer[0];
        } else {
            // EOF
            return -1;
        }
    }
    
    @Override
    public synchronized int read(byte[] bytes) throws IOException {
        if(this.closed) {
            LOG.error("InputStream is already closed");
            throw new IOException("InputStream is already closed");
        }
        
        int readLen = (int) Math.min(this.handle.getStatus().getSize() - this.offset, bytes.length);
        if(readLen > 0) { 
            int read = this.handle.readFileData(this.offset, bytes, 0, readLen);
            this.offset += read;
            return read;
        } else {
            // EOF
            return -1;
        }
    }
    
    @Override
    public synchronized int read(byte[] bytes, int off, int len) throws IOException {
        if(this.closed) {
            LOG.error("InputStream is already closed");
            throw new IOException("InputStream is already closed");
        }
        
        int readLen = (int) Math.min(this.handle.getStatus().getSize() - this.offset, len);
        if(readLen > 0) { 
            int read = this.handle.readFileData(this.offset, bytes, off, readLen);
            this.offset += read;
            return read;
        } else {
            // EOF
            return -1;
        }
    }
    
    @Override
    public synchronized long skip(long n) throws IOException {
        if(this.closed) {
            LOG.error("InputStream is already closed");
            throw new IOException("InputStream is already closed");
        }
        
        if(n <= 0) {
            return 0;
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
    public synchronized int available() throws IOException {
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
    public synchronized void close() throws IOException {
        this.handle.close();
        this.handle.getFileSystem().notifyInputStreamClosed(this);
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
    public synchronized boolean markSupported() {
        return false;
    }
}
