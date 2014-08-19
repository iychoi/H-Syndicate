package edu.arizona.cs.syndicate.fs;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SyndicateFSOutputStream extends OutputStream {

    private static final Log LOG = LogFactory.getLog(SyndicateFSOutputStream.class);
    
    private SyndicateFSFileHandle handle;
    private long offset;
    private boolean closed;
    private long prevFlushedOffset;
    
    SyndicateFSOutputStream(SyndicateFSFileHandle handle) {
        this.handle = handle;
        
        this.offset = 0;
        this.prevFlushedOffset = 0;
        this.closed = false;
    }
    
    @Override
    public synchronized void write(int i) throws IOException {
        if(this.closed) {
            LOG.error("OutputStream is already closed");
            throw new IOException("OutputStream is already closed");
        }
        
        byte[] buffer = new byte[1];
        buffer[0] = (byte)(i & 0xff);
        
        this.handle.writeFileData(this.offset, buffer, 0, 1);
        this.offset++;
    }
    
    @Override
    public synchronized void write(byte[] bytes) throws IOException {
        if(this.closed) {
            LOG.error("OutputStream is already closed");
            throw new IOException("OutputStream is already closed");
        }
        
        this.handle.writeFileData(this.offset, bytes, 0, bytes.length);
        this.offset += bytes.length;
    }
    
    @Override
    public synchronized void write(byte[] bytes, int offset, int len) throws IOException {
        if(this.closed) {
            LOG.error("OutputStream is already closed");
            throw new IOException("OutputStream is already closed");
        }
        
        int writeLen = len;
        if(offset + len > bytes.length) {
            writeLen = bytes.length - offset;
        }
        
        this.handle.writeFileData(this.offset, bytes, offset, writeLen);
        this.offset += writeLen;
    }
    
    @Override
    public synchronized void flush() throws IOException {
        if(this.closed) {
            LOG.error("OutputStream is already closed");
            throw new IOException("OutputStream is already closed");
        }
        
        if(this.prevFlushedOffset != this.offset) {
            // flush stale data
            this.handle.flush();
            this.prevFlushedOffset = this.offset;
        }
    }
    
    @Override
    public synchronized void close() throws IOException {
        flush();
        
        this.handle.close();
        this.handle.getFileSystem().notifyOutputStreamClosed(this);
        this.closed = true;
    }
}
