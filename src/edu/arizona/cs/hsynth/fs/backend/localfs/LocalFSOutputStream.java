package edu.arizona.cs.hsynth.fs.backend.localfs;

import edu.arizona.cs.hsynth.fs.HSynthFSOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class LocalFSOutputStream extends HSynthFSOutputStream{
    
    private LocalFSFileSystem filesystem;
    private FileOutputStream os;
    
    LocalFSOutputStream(LocalFSFileSystem fs, String name) throws FileNotFoundException {
        this.os = new FileOutputStream(name);
        
        this.filesystem = fs;
    }
    
    LocalFSOutputStream(LocalFSFileSystem fs, String name, boolean append) throws FileNotFoundException {
        this.os = new FileOutputStream(name, append);
        
        this.filesystem = fs;
    }
    
    LocalFSOutputStream(LocalFSFileSystem fs, File file) throws FileNotFoundException {
        this.os = new FileOutputStream(file);
        
        this.filesystem = fs;
    }
    
    LocalFSOutputStream(LocalFSFileSystem fs, File file, boolean append) throws FileNotFoundException {
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
