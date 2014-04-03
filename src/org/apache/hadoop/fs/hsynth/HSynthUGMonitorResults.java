package org.apache.hadoop.fs.hsynth;

public class HSynthUGMonitorResults<T> {
    private String hostname;
    private T result;
    
    public HSynthUGMonitorResults(String hostname) {
        this.hostname = hostname;
    }
    
    public HSynthUGMonitorResults(String hostname, T result) {
        this.hostname = hostname;
        this.result = result;
    }
    
    public void setResult(T result) {
        this.result = result;
    }
    
    public String getHostname() {
        return this.hostname;
    }
    
    public T getResult() {
        return this.result;
    }
}
