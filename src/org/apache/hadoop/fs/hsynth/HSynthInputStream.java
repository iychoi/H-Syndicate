package org.apache.hadoop.fs.hsynth;

import edu.arizona.cs.hsynth.fs.HSynthFSPath;
import edu.arizona.cs.hsynth.fs.HSynthFSRandomAccess;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

public class HSynthInputStream extends FSInputStream {

    private HSynthFSPath path;
    private edu.arizona.cs.hsynth.fs.HSynthFileSystem hsynth;
    private FileSystem.Statistics stats;
    private boolean closed;
    private long fileLength;
    private long pos = 0;
    private HSynthFSRandomAccess raf;

    
    public HSynthInputStream(Configuration conf, HSynthFSPath path, edu.arizona.cs.hsynth.fs.HSynthFileSystem hsynth, FileSystem.Statistics stats) throws IOException {
        this.path = path;
        this.hsynth = hsynth;
        this.stats = stats;
        this.fileLength = hsynth.getSize(path);
        
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
        if (this.pos < this.fileLength) {
            int result = this.raf.read(bytes, off, len);
            if (result >= 0) {
                this.pos += result;
            }
            if (this.stats != null && result > 0) {
                this.stats.incrementBytesRead(result);
            }
            return result;
        }
        return -1;
    }
    
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        if (this.raf != null) {
            this.raf.close();
            this.raf = null;
        }
        super.close();
        closed = true;
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
