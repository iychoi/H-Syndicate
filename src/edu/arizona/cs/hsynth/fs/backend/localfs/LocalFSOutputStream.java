package edu.arizona.cs.hsynth.fs.backend.localfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class LocalFSOutputStream extends FileOutputStream{
    
    private LocalFSFileSystem filesystem;
    
    LocalFSOutputStream(LocalFSFileSystem fs, String name) throws FileNotFoundException {
        super(name);
        
        this.filesystem = fs;
    }
    
    LocalFSOutputStream(LocalFSFileSystem fs, String name, boolean append) throws FileNotFoundException {
        super(name, append);
        
        this.filesystem = fs;
    }
    
    LocalFSOutputStream(LocalFSFileSystem fs, File file) throws FileNotFoundException {
        super(file);
        
        this.filesystem = fs;
    }
    
    LocalFSOutputStream(LocalFSFileSystem fs, File file, boolean append) throws FileNotFoundException {
        super(file, append);
        
        this.filesystem = fs;
    }
    
    @Override
    public void close() throws IOException {
        super.close();
        
        this.filesystem.notifyOutputStreamClosed(this);
    }
}
