package org.apache.hadoop.fs.hsyndicate;

public class HSyndicateUGMonitorResults<T> {
    private String hostname;
    private T result;
    
    public HSyndicateUGMonitorResults(String hostname) {
        this.hostname = hostname;
    }
    
    public HSyndicateUGMonitorResults(String hostname, T result) {
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
