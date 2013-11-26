package edu.arizona.cs.hsynth.fs.backend.localfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class LocalFSInputStream extends FileInputStream {
    
    private LocalFSFileSystem filesystem;
    
    LocalFSInputStream(LocalFSFileSystem fs, String name) throws FileNotFoundException {
        super(name);
        
        this.filesystem = fs;
    }
    
    LocalFSInputStream(LocalFSFileSystem fs, File file) throws FileNotFoundException {
        super(file);
        
        this.filesystem = fs;
    }
    
    @Override
    public void close() throws IOException {
        super.close();
        
        this.filesystem.notifyInputStreamClosed(this);
    }
}
