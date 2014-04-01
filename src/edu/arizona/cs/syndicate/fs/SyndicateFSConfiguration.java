package edu.arizona.cs.syndicate.fs;

public class SyndicateFSConfiguration {

    private String host_address;
    private int port;
    private int cacheSize;
    private int metadataCacheTimeout;
    
    public void setHostAddress(String host_address) {
        this.host_address = host_address;
    }
    
    public String getHostAddress() {
        return this.host_address;
    }
    
    public void setPort(int port) {
        this.port = port;
    }
    
    public int getPort() {
        return this.port;
    }

    public void setMaxMetadataCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }
    
    public int getMaxMetadataCacheSize() {
        return this.cacheSize;
    }

    public void setMetadataCacheTimeout(int metadataCacheTimeout) {
        this.metadataCacheTimeout = metadataCacheTimeout;
    }
    
    public int getMetadataCacheTimeout() {
        return this.metadataCacheTimeout;
    }
}
