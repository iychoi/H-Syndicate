package edu.arizona.cs.hsynth.fs.backend.localfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class LocalFSOutputStream extends OutputStream {
    
    private LocalFSFileSystem filesystem;
    private FileOutputStream os;
    
    LocalFSOutputStream(LocalFSFileSystem fs, LocalFSConfiguration conf, String name) throws FileNotFoundException {
        this.os = new FileOutputStream(name);
        this.filesystem = fs;
    }
    
    LocalFSOutputStream(LocalFSFileSystem fs, LocalFSConfiguration conf, String name, boolean append) throws FileNotFoundException {
        this.os = new FileOutputStream(name, append);
        this.filesystem = fs;
    }
    
    LocalFSOutputStream(LocalFSFileSystem fs, LocalFSConfiguration conf, File file) throws FileNotFoundException {
        this.os = new FileOutputStream(file);
        this.filesystem = fs;
    }
    
    LocalFSOutputStream(LocalFSFileSystem fs, LocalFSConfiguration conf, File file, boolean append) throws FileNotFoundException {
        this.os = new FileOutputStream(file, append);
        this.filesystem = fs;
    }
    
    @Override
    public void write(int i) throws IOException {
        this.os.write(i);
    }
    
    @Override
    public void write(byte[] bytes) throws IOException {
        this.os.write(bytes);
    }
    
    @Override
    public void write(byte[] bytes, int offset, int len) throws IOException {
        this.os.write(bytes, offset, len);
    }
    
    @Override
    public void flush() throws IOException {
        this.os.flush();
    }
    
    @Override
    public void close() throws IOException {
        this.os.close();
        
        this.filesystem.notifyOutputStreamClosed(this);
    }
}
