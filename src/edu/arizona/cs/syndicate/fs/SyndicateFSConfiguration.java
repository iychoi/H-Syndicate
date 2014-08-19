package edu.arizona.cs.syndicate.fs;

public class SyndicateFSConfiguration {

    private String host_address;
    private int port;
    private int cacheSize;
    private int metadataCacheTimeout;
    
    public synchronized void setHostAddress(String host_address) {
        this.host_address = host_address;
    }
    
    public synchronized String getHostAddress() {
        return this.host_address;
    }
    
    public synchronized void setPort(int port) {
        this.port = port;
    }
    
    public synchronized int getPort() {
        return this.port;
    }

    public synchronized void setMaxMetadataCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }
    
    public synchronized int getMaxMetadataCacheSize() {
        return this.cacheSize;
    }

    public synchronized void setMetadataCacheTimeout(int metadataCacheTimeout) {
        this.metadataCacheTimeout = metadataCacheTimeout;
    }
    
    public synchronized int getMetadataCacheTimeout() {
        return this.metadataCacheTimeout;
    }
}
