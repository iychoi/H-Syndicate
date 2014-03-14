package edu.arizona.cs.syndicate.fs;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SyndicateFSRandomAccess implements ISyndicateFSRandomAccess {
    
    private static final Log LOG = LogFactory.getLog(SyndicateFSRandomAccess.class);
    
    private SyndicateFSFileHandle handle;
    private long offset;
    private boolean closed;
    
    SyndicateFSRandomAccess(SyndicateFSFileHandle handle) {
        this.handle = handle;
        
        this.offset = 0;
        this.closed = false;
    }
    
    @Override
    public int read() throws IOException {
        if(this.closed) {
            LOG.error("RandomAccess is already closed");
            throw new IOException("RandomAccess is already closed");
        }
        
        int readLen = (int) Math.min(this.handle.getStatus().getSize() - this.offset, 1);
        if(readLen > 0) {
            byte[] buffer = new byte[1];
            int read = this.handle.readFileData(this.offset, buffer, 0, 1);
            if(read == 0) {
                // EOF
                return -1;
            }
            if (read != 1) {
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
    public int read(byte[] bytes) throws IOException {
        if(this.closed) {
            LOG.error("RandomAccess is already closed");
            throw new IOException("RandomAccess is already closed");
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
    public int read(byte[] bytes, int off, int len) throws IOException {
        if(this.closed) {
            LOG.error("RandomAccess is already closed");
            throw new IOException("RandomAccess is already closed");
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
    public int skip(int n) throws IOException {
        if(this.closed) {
            LOG.error("RandomAccess is already closed");
            throw new IOException("RandomAccess is already closed");
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
            LOG.error("RandomAccess is already closed");
            throw new IOException("RandomAccess is already closed");
        }
        
        return this.offset;
    }

    @Override
    public long length() throws IOException {
        if(this.closed) {
            LOG.error("RandomAccess is already closed");
            throw new IOException("RandomAccess is already closed");
        }
        
        return this.handle.getStatus().getSize();
    }

    @Override
    public void seek(long l) throws IOException {
        if(this.closed) {
            LOG.error("RandomAccess is already closed");
            throw new IOException("RandomAccess is already closed");
        }
        
        if(l < 0) {
            LOG.error("seek point can not be negative");
            throw new IOException("seek RandomAccess can not be negative");
        }
        
        if(this.handle.getStatus().getSize() > l) {
            this.offset = l;
        } else {
            LOG.error("seek point can not be larger than the file size");
            throw new IOException("seek point can not be larger than the file size");
        }
    }
    
    @Override
    public void close() throws IOException {
        this.handle.close();
        this.handle.getFileSystem().notifyRandomAccessClosed(this);
        this.closed = true;
    }
}
