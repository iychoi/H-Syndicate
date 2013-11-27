package edu.arizona.cs.hsynth.fs.backend.syndicatefs;

import edu.arizona.cs.hsynth.fs.Configuration;
import edu.arizona.cs.hsynth.fs.Context;

public class SyndicateFSConfiguration extends Configuration {
    
    private final static String FS_BACKEND_NAME = "SyndicateFileSystem";
    
    // IPC backend mode default port
    public static final int DEFAULT_IPC_PORT = 9910;
    // unlimited size
    public static final int MAX_METADATA_CACHE_SIZE = 0;
    // no timeout
    public static final int CACHE_TIMEOUT_SECOND = 0;
    // read buffer size
    public static final int READ_BUFFER_SIZE = 64 * 1024;
    // write buffer size
    public static final int WRITE_BUFFER_SIZE = 64 * 1024;
    
    private int ipcPort;
    private int maxMetadataCacheSize;
    private int cacheTimeoutSecond;
    private int readBufferSize;
    private int writeBufferSize;
    
    public SyndicateFSConfiguration() {
        this.ipcPort = DEFAULT_IPC_PORT;
        this.maxMetadataCacheSize = MAX_METADATA_CACHE_SIZE;
        this.cacheTimeoutSecond = CACHE_TIMEOUT_SECOND;
        this.readBufferSize = READ_BUFFER_SIZE;
        this.writeBufferSize = WRITE_BUFFER_SIZE;
    }
    
    public int getPort() {
        return this.ipcPort;
    }
    
    public void setPort(int port) throws IllegalAccessException {
        if(this.lock)
            throw new IllegalAccessException("Can not modify the locked object");
        
        this.ipcPort = port;
    }
    
    public int getMaxMetadataCacheSize() {
        return this.maxMetadataCacheSize;
    }
    
    public void setMaxMetadataCacheSize(int max) throws IllegalAccessException {
        if(this.lock)
            throw new IllegalAccessException("Can not modify the locked object");
        
        this.maxMetadataCacheSize = max;
    }
    
    public int getCacheTimeoutSecond() {
        return this.cacheTimeoutSecond;
    }
    
    public void setCacheTimeoutSecond(int timeoutSecond) throws IllegalAccessException {
        if(this.lock)
            throw new IllegalAccessException("Can not modify the locked object");
        
        this.cacheTimeoutSecond = timeoutSecond;
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
        if (!(o instanceof SyndicateFSConfiguration))
            return false;
        
        SyndicateFSConfiguration other = (SyndicateFSConfiguration) o;
        if(this.ipcPort != other.ipcPort)
            return false;
        
        return true;
    }
    
    @Override
    public int hashCode() {
        return this.ipcPort ^ FS_BACKEND_NAME.hashCode();
    }

    @Override
    public String getBackendName() {
        return FS_BACKEND_NAME;
    }

    @Override
    public Class getFileSystemClass() {
        return SyndicateFSFileSystem.class;
    }
    
    @Override
    public Context getContext() {
        return Context.getContext(this);
    }
}
