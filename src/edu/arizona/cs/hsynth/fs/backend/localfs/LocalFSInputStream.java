package edu.arizona.cs.hsynth.fs.backend.localfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

public class LocalFSInputStream extends InputStream {
    
    private LocalFSFileSystem filesystem;
    private FileInputStream is;
    
    LocalFSInputStream(LocalFSFileSystem fs, LocalFSConfiguration conf, String name) throws FileNotFoundException {
        this.is = new FileInputStream(name);
        this.filesystem = fs;
    }
    
    LocalFSInputStream(LocalFSFileSystem fs, LocalFSConfiguration conf, File file) throws FileNotFoundException {
        this.is = new FileInputStream(file);
        this.filesystem = fs;
    }
    
    @Override
    public void close() throws IOException {
        this.is.close();
        
        this.filesystem.notifyInputStreamClosed(this);
    }

    @Override
    public int read() throws IOException {
        return this.is.read();
    }
    
    @Override
    public int read(byte[] bytes) throws IOException {
        return this.is.read(bytes);
    }
    
    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        return this.is.read(bytes, off, len);
    }
    
    @Override
    public long skip(long n) throws IOException {
        return this.is.skip(n);
    }
    
    @Override
    public int available() throws IOException {
        return this.is.available();
    }
    
    @Override
    public synchronized void mark(int readlimit) {
        this.is.mark(readlimit);
    }
    
    @Override
    public synchronized void reset() throws IOException {
        this.is.reset();
    }
    
    @Override
    public boolean markSupported() {
        return this.is.markSupported();
    }
}
