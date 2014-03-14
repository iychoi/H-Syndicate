package edu.arizona.cs.syndicate.fs;

public class SyndicateFSConfiguration {

    private String host;
    private int port;
    private int cacheSize;
    private int metadataCacheTimeout;
    
    public String getHost() {
        return this.host;
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

    public void setHost(String host) {
        this.host = host;
    }
}
