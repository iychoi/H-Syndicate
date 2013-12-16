package edu.arizona.cs.hsynth.fs;

import java.util.Hashtable;

public abstract class HSynthFSConfiguration {
    /*
     * if locked, values in this class become unmodifiable
     */
    protected boolean lock = false;
    
    public static final String HSYNTH_CONF_PREFIX = "hsynth.";
    
    public HSynthFSConfiguration() {
    }
    
    /*
     * Buffer Size related
     */
    public abstract int getReadBufferSize();
    
    public abstract void setReadBufferSize(int bufferSize) throws IllegalAccessException;
    
    public abstract int getWriteBufferSize();
    
    public abstract void setWriteBufferSize(int bufferSize) throws IllegalAccessException;
    
    /*
     * HSynthFSContext related
     */
    public abstract HSynthFSContext getContext();
    
    /*
     * Utilities
     */
    public abstract String getBackendName();
    
    public abstract Class getFileSystemClass();
    
    /*
     * HSynthFSConfiguration Lock
     */
    public void lock() {
        this.lock = true;
    }
    
    public boolean isLocked() {
        return this.lock;
    }
    
    public abstract Hashtable<String, String> getParams();
    
    public abstract void load(Hashtable<String, String> params) throws IllegalAccessException;
    
    @Override
    public abstract boolean equals(Object o);
    
    @Override
    public abstract int hashCode();
}
