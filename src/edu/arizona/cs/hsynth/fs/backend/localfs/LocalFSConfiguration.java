package edu.arizona.cs.hsynth.fs.backend.localfs;

import edu.arizona.cs.hsynth.fs.Configuration;
import edu.arizona.cs.hsynth.fs.Context;
import java.io.File;

public class LocalFSConfiguration extends Configuration {
    
    public final static String FS_BACKEND_NAME = "LocalFileSystem";
    
    // read buffer size
    public static final int READ_BUFFER_SIZE = 64 * 1024;
    // write buffer size
    public static final int WRITE_BUFFER_SIZE = 64 * 1024;
    
    private File workingDir;
    private int readBufferSize;
    private int writeBufferSize;
    
    public LocalFSConfiguration() {
        this.workingDir = null;
        this.readBufferSize = READ_BUFFER_SIZE;
        this.writeBufferSize = WRITE_BUFFER_SIZE;
    }
    
    public File getWorkingDir() {
        return this.workingDir;
    }
    
    public void setWorkingDir(File dir) throws IllegalAccessException {
        if(this.lock)
            throw new IllegalAccessException("Can not modify the locked object");
        
        this.workingDir = dir;
    }
    
    @Override
    public int getReadBufferSize() {
        return this.readBufferSize;
    }
    
    @Override
    public void setReadBufferSize(int bufferSize) throws IllegalAccessException {
        if(this.lock)
            throw new IllegalAccessException("Can not modify the locked object");
        
        this.readBufferSize = bufferSize;
    }
    
    @Override
    public int getWriteBufferSize() {
        return this.writeBufferSize;
    }
    
    @Override
    public void setWriteBufferSize(int bufferSize) throws IllegalAccessException {
        if(this.lock)
            throw new IllegalAccessException("Can not modify the locked object");
        
        this.writeBufferSize = bufferSize;
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof LocalFSConfiguration))
            return false;
        
        LocalFSConfiguration other = (LocalFSConfiguration) o;
        if(!this.workingDir.equals(other.workingDir))
            return false;
        
        return true;
    }
    
    @Override
    public int hashCode() {
        return this.workingDir.hashCode() ^ FS_BACKEND_NAME.hashCode();
    }
    
    @Override
    public String getBackendName() {
        return FS_BACKEND_NAME;
    }

    @Override
    public Class getFileSystemClass() {
        return LocalFSFileSystem.class;
    }

    @Override
    public Context getContext() {
        return Context.getContext(this);
    }
}
