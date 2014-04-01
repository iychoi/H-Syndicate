package org.apache.hadoop.fs.hsynth;

public class SlavesMonitorResults<T> {
    private String host;
    private String hostname;
    private T result;
    
    public SlavesMonitorResults(String host, String hostname) {
        this.host = host;
        this.hostname = hostname;
    }
    
    public SlavesMonitorResults(String host, String hostname, T result) {
        this.host = host;
        this.hostname = hostname;
        this.result = result;
    }
    
    public void setResult(T result) {
        this.result = result;
    }
    
    public String getHost() {
        return this.host;
    }
    
    public String getHostname() {
        return this.hostname;
    }
    
    public T getResult() {
        return this.result;
    }
}
