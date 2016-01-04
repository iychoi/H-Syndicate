package edu.arizona.cs.hsyndicate.dfs;

import edu.arizona.cs.hsyndicate.fs.AHSyndicateFileSystemBase;
import edu.arizona.cs.hsyndicate.fs.SyndicateFSInputStream;
import edu.arizona.cs.hsyndicate.fs.SyndicateFSPath;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

public class HSyndicateInputStream extends FSInputStream implements Seekable, PositionedReadable {

    private static final Log LOG = LogFactory.getLog(HSyndicateInputStream.class);
    
    private SyndicateFSPath path;
    private AHSyndicateFileSystemBase fs;
    private FileSystem.Statistics stats;
    private long fileLength;
    private SyndicateFSInputStream in;
    
    public HSyndicateInputStream(AHSyndicateFileSystemBase fs, SyndicateFSPath path, FileSystem.Statistics stats) throws IOException {
        this.path = path;
        this.fs = fs;
        this.stats = stats;
        this.fileLength = fs.getSize(path);
        this.in = fs.getFileInputStream(path);
    }
    
    public synchronized long getSize() throws IOException {
        return this.fileLength;
    }
    
    @Override
    public synchronized long getPos() throws IOException {
        return this.in.getPos();
    }

    @Override
    public synchronized int available() throws IOException {
        return this.in.available();
    }
    
    @Override
    public synchronized void seek(long targetPos) throws IOException {
        this.in.seek(targetPos);
    }

    @Override
    public synchronized long skip(long l) throws IOException {
        return this.in.skip(l);
    }

    @Override
    public synchronized boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }
    
    @Override
    public synchronized int read() throws IOException {
        int result = this.in.read();
        if (this.stats != null & result >= 0) {
            this.stats.incrementBytesRead(1);
        }
        
        return result;
    }
    
    @Override
    public synchronized int read(byte[] bytes, int off, int len) throws IOException {
        int result = this.in.read(bytes, off, len);
        if (this.stats != null && result > 0) {
            this.stats.incrementBytesRead(result);
        }
        
        return result;
    }
    
    @Override
    public int read(long pos, byte[] bytes, int off, int len) throws IOException {
        this.in.seek(pos);
        
        if(this.in.getPos() != pos) {
            throw new IOException("Cannot find position : " + pos);
        }
        
        int available = this.in.available();
        if(available > 0) {
            return this.in.read(bytes, off, Math.min(len, available));
        } else {
            return this.in.read(bytes, off, len);
        }
    }

    @Override
    public void readFully(long pos, byte[] bytes, int off, int len) throws IOException {
        this.in.seek(pos);
        
        if(this.in.getPos() != pos) {
            throw new IOException("Cannot find position : " + pos);
        }
        
        super.read(bytes, off, len);
    }

    @Override
    public void readFully(long pos, byte[] bytes) throws IOException {
        this.in.seek(pos);
        
        if(this.in.getPos() != pos) {
            throw new IOException("Cannot find position : " + pos);
        }
        
        super.read(bytes, 0, bytes.length);
    }
    
    @Override
    public synchronized void close() throws IOException {
        if (this.in != null) {
            this.in.close();
            this.in = null;
        }
        super.close();
    }
    
    @Override
    public synchronized boolean markSupported() {
        return false;
    }

    @Override
    public synchronized void mark(int readLimit) {
        // Do nothing
    }

    @Override
    public synchronized void reset() throws IOException {
        throw new IOException("Mark not supported");
    }
}
