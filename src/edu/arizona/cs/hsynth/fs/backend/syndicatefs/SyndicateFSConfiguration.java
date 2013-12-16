package edu.arizona.cs.hsynth.fs.backend.syndicatefs;

import edu.arizona.cs.hsynth.fs.HSynthFSBackend;
import edu.arizona.cs.hsynth.fs.HSynthFSConfiguration;
import edu.arizona.cs.hsynth.fs.HSynthFSContext;
import java.util.Hashtable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SyndicateFSConfiguration extends HSynthFSConfiguration {
    
    private static final Log LOG = LogFactory.getLog(SyndicateFSConfiguration.class);
    
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
    
    private final static String KEY_READ_BUFFER_SIZE = HSYNTH_CONF_PREFIX + "rbuffersize";
    private final static String KEY_WRITE_BUFFER_SIZE = HSYNTH_CONF_PREFIX + "wbuffersize";
    private final static String KEY_IPC_PORT = HSYNTH_CONF_PREFIX + "syndicate.ipcport";
    private final static String KEY_MAX_METADATA_CACHE_SIZE = HSYNTH_CONF_PREFIX + "syndicate.maxmetacache";
    private final static String KEY_CACHE_TIMEOUT = HSYNTH_CONF_PREFIX + "syndicate.cachetimeout";
    
    static {
        try {
            HSynthFSBackend.registerBackend(FS_BACKEND_NAME, SyndicateFSConfiguration.class);
        } catch (InstantiationException ex) {
            LOG.error(ex);
        } catch (IllegalAccessException ex) {
            LOG.error(ex);
        }
    }
    
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
    public Hashtable<String, String> getParams() {
        Hashtable<String, String> table = new Hashtable<String, String>();
        table.put(KEY_READ_BUFFER_SIZE, Integer.toString(getReadBufferSize()));
        table.put(KEY_WRITE_BUFFER_SIZE, Integer.toString(getWriteBufferSize()));
        table.put(KEY_IPC_PORT, Integer.toString(getPort()));
        table.put(KEY_MAX_METADATA_CACHE_SIZE, Integer.toString(getMaxMetadataCacheSize()));
        table.put(KEY_CACHE_TIMEOUT, Integer.toString(getCacheTimeoutSecond()));
        
        return table;
    }

    @Override
    public void load(Hashtable<String, String> params) throws IllegalAccessException {
        setReadBufferSize(Integer.parseInt(params.get(KEY_READ_BUFFER_SIZE)));
        setWriteBufferSize(Integer.parseInt(params.get(KEY_WRITE_BUFFER_SIZE)));
        setPort(Integer.parseInt(params.get(KEY_IPC_PORT)));
        setMaxMetadataCacheSize(Integer.parseInt(params.get(KEY_MAX_METADATA_CACHE_SIZE)));
        setCacheTimeoutSecond(Integer.parseInt(params.get(KEY_CACHE_TIMEOUT)));
    }
}
