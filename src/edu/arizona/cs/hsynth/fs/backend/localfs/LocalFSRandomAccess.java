package edu.arizona.cs.hsynth.fs.backend.localfs;

import edu.arizona.cs.hsynth.fs.RandomAccess;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class LocalFSRandomAccess implements RandomAccess {
    
    private RandomAccessFile raf;
    private LocalFSFileSystem filesystem;
    
    LocalFSRandomAccess(LocalFSFileSystem fs, String name, String mode) throws FileNotFoundException {
        this.filesystem = fs;
        this.raf = new RandomAccessFile(name, mode);
    }
    
    LocalFSRandomAccess(LocalFSFileSystem fs, File file, String mode) throws FileNotFoundException {
        this.raf = new RandomAccessFile(file, mode);
        
        this.filesystem = fs;
    }
    
    @Override
    public void close() throws IOException {
        this.raf.close();
        
        this.filesystem.notifyRandomAccessClosed(this);
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
    public int skip(int n) throws IOException {
        return this.raf.skipBytes(n);
    }

    @Override
    public long getFilePointer() throws IOException {
        return this.raf.getFilePointer();
    }

    @Override
    public long length() throws IOException {
        return this.raf.length();
    }

    @Override
    public void seek(long l) throws IOException {
        this.raf.seek(l);
    }
}