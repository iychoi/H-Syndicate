package edu.arizona.cs.hsynth.hadoop.input;

import edu.arizona.cs.hsynth.fs.RandomAccess;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.fs.Seekable;

public class HSynthSeekableInputStream extends InputStream implements Seekable {

    private RandomAccess raf;
    private long length = 0;
    
    public HSynthSeekableInputStream(RandomAccess raf) {
        this.raf = raf;
        try {
            // for better performance
            this.length = raf.length();
        } catch (IOException ex) {}
    }

    @Override
    public int read() throws IOException {
        return this.raf.read();
    }
    
    @Override
    public int read(byte[] bytes) throws IOException {
        return this.raf.read(bytes);
    }
    
    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        return this.raf.read(bytes, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return this.raf.skip((int)n);
    }
    
    @Override
    public void seek(long l) throws IOException {
        this.raf.seek(l);
    }

    @Override
    public long getPos() throws IOException {
        return this.raf.getFilePointer();
    }
    
    @Override
    public int available() throws IOException {
        if((this.length - getPos()) > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        } else {
            return (int)(this.length - getPos());
        }
    }

    @Override
    public boolean seekToNewSource(long l) throws IOException {
        return false;
    }
    
    @Override
    public void close() throws IOException {
        this.raf.close();
    }
    
    @Override
    public synchronized void mark(int readlimit) {
    }
    
    @Override
    public synchronized void reset() throws IOException {
    }
    
    @Override
    public boolean markSupported() {
        return false;
    }
}
