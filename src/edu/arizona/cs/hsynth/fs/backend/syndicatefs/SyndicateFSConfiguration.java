package edu.arizona.cs.hsynth.fs.backend.syndicatefs;

import edu.arizona.cs.hsynth.fs.HSynthFSConfiguration;
import edu.arizona.cs.hsynth.fs.HSynthFSContext;
import java.io.File;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

public class SyndicateFSConfiguration extends HSynthFSConfiguration {
    
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
    
    private final static String JSON_KEY_FS_BACKEND = "fsbackend";
    private final static String JSON_KEY_READ_BUFFER_SIZE = "rbuffersize";
    private final static String JSON_KEY_WRITE_BUFFER_SIZE = "wbuffersize";
    private final static String JSON_KEY_IPC_PORT = "ipcport";
    private final static String JSON_KEY_MAX_METADATA_CACHE_SIZE = "maxmetacache";
    private final static String JSON_KEY_CACHE_TIMEOUT = "cachetimeout";
    
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
    public HSynthFSContext getContext() {
        return HSynthFSContext.getContext(this);
    }

    @Override
    public String serialize() {
        JSONObject json = new JSONObject();
        json.put(JSON_KEY_FS_BACKEND, getBackendName());
        json.put(JSON_KEY_READ_BUFFER_SIZE, (Integer) getReadBufferSize());
        json.put(JSON_KEY_WRITE_BUFFER_SIZE, (Integer) getWriteBufferSize());
        json.put(JSON_KEY_IPC_PORT, (Integer) getPort());
        json.put(JSON_KEY_MAX_METADATA_CACHE_SIZE, (Integer) getMaxMetadataCacheSize());
        json.put(JSON_KEY_CACHE_TIMEOUT, (Integer) getCacheTimeoutSecond());
        
        return json.toString();
    }

    @Override
    public void deserialize(String serializedConf) throws IllegalAccessException {
        JSONObject json = (JSONObject)JSONSerializer.toJSON(serializedConf);
        //json.get(JSON_KEY_FS_BACKEND);
        setReadBufferSize((Integer)json.get(JSON_KEY_READ_BUFFER_SIZE));
        setWriteBufferSize((Integer)json.get(JSON_KEY_WRITE_BUFFER_SIZE));
        setPort((Integer)json.get(JSON_KEY_IPC_PORT));
        setMaxMetadataCacheSize((Integer)json.get(JSON_KEY_MAX_METADATA_CACHE_SIZE));
        setCacheTimeoutSecond((Integer)json.get(JSON_KEY_CACHE_TIMEOUT));
    }
}
