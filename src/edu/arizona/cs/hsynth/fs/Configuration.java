package edu.arizona.cs.hsynth.fs;

public abstract class Configuration {
    /*
     * if locked, values in this class become unmodifiable
     */
    protected boolean lock = false;
    
    public Configuration() {
    }
    
    /*
     * Buffer Size related
     */
    public abstract int getReadBufferSize();
    
    public abstract void setReadBufferSize(int bufferSize) throws IllegalAccessException;
    
    public abstract int getWriteBufferSize();
    
    public abstract void setWriteBufferSize(int bufferSize) throws IllegalAccessException;
    
    /*
     * Context related
     */
    public abstract Context getContext();
    
    /*
     * Utilities
     */
    public abstract String getBackendName();
    
    public abstract Class getFileSystemClass();
    
    /*
     * Configuration Lock
     */
    public void lock() {
        this.lock = true;
    }
    
    public boolean isLocked() {
        return this.lock;
    }
    
    @Override
    public abstract boolean equals(Object o);
    
    @Override
    public abstract int hashCode();
}
