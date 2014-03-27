package org.apache.hadoop.fs.hsynth;

import edu.arizona.cs.syndicate.fs.SyndicateFSPath;
import edu.arizona.cs.syndicate.fs.ISyndicateFSRandomAccess;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

public class HSynthInputStream extends FSInputStream {

    private static final Log LOG = LogFactory.getLog(HSynthInputStream.class);
    
    private SyndicateFSPath path;
    private edu.arizona.cs.syndicate.fs.ASyndicateFileSystem hsynth;
    private FileSystem.Statistics stats;
    private boolean closed;
    private long fileLength;
    private long pos = 0;
    private ISyndicateFSRandomAccess raf;
    
    public HSynthInputStream(Configuration conf, SyndicateFSPath path, edu.arizona.cs.syndicate.fs.ASyndicateFileSystem hsynth, FileSystem.Statistics stats) throws IOException {
        this.path = path;
        this.hsynth = hsynth;
        this.stats = stats;
        this.fileLength = hsynth.getSize(path);
        this.pos = 0;
        this.raf = hsynth.getRandomAccess(path);
    }
    
    @Override
    public synchronized long getPos() throws IOException {
        return this.pos;
    }

    @Override
    public synchronized int available() throws IOException {
        return (int) (this.fileLength - this.pos);
    }
    
    @Override
    public synchronized void seek(long targetPos) throws IOException {
        if (targetPos > this.fileLength) {
            throw new IOException("Cannot seek after EOF");
        }
        this.pos = targetPos;
        this.raf.seek(targetPos);
    }

    @Override
    public long skip(long l) throws IOException {
        long newOff = Math.min(l, this.fileLength - this.pos);
        seek(this.pos + newOff);
        return newOff;
    }

    @Override
    public synchronized boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }
    
    @Override
    public synchronized int read() throws IOException {
        if (this.closed) {
            throw new IOException("Stream closed");
        }
        int result = -1;
        if (this.pos < this.fileLength) {
            result = this.raf.read();
            
            if (result >= 0) {
                this.pos++;
            }
        }
        if (this.stats != null & result >= 0) {
            this.stats.incrementBytesRead(1);
        }
        return result;
    }
    
    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        if (this.closed) {
            throw new IOException("Stream closed");
        }
        int result = -1;
        if (this.pos < this.fileLength) {
            int readLen = (int)Math.min(this.fileLength - this.pos, len);
            
            result = this.raf.read(bytes, off, readLen);
                
            if (result >= 0) {
                this.pos += result;
            }
        }
        
        if (this.stats != null && result > 0) {
            this.stats.incrementBytesRead(result);
        }
        return result;
    }
    
    @Override
    public void close() throws IOException {
        if (this.closed) {
            return;
        }
        if (this.raf != null) {
            this.raf.close();
            this.raf = null;
        }
        super.close();
        this.closed = true;
    }
    
    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void mark(int readLimit) {
        // Do nothing
    }

    @Override
    public void reset() throws IOException {
        throw new IOException("Mark not supported");
    }
}
