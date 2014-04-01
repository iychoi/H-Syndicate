package org.apache.hadoop.fs.hsynth;

public class HSynthUGMonitorResults<T> {
    private String address;
    private String hostname;
    private T result;
    
    public HSynthUGMonitorResults(String address, String hostname) {
        this.address = address;
        this.hostname = hostname;
    }
    
    public HSynthUGMonitorResults(String address, String hostname, T result) {
        this.address = address;
        this.hostname = hostname;
        this.result = result;
    }
    
    public void setResult(T result) {
        this.result = result;
    }
    
    public String getHost() {
        return this.address;
    }
    
    public String getHostname() {
        return this.hostname;
    }
    
    public T getResult() {
        return this.result;
    }
}
