package org.apache.hadoop.fs.hsynth;

import edu.arizona.cs.syndicate.fs.ASyndicateFileSystem;
import edu.arizona.cs.syndicate.fs.SyndicateFSConfiguration;
import edu.arizona.cs.syndicate.fs.SyndicateFSPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.hsynth.util.HSynthConfigUtil;

public class SliversMonitor {
    private static final Log LOG = LogFactory.getLog(SliversMonitor.class);
    
    private static List<String> hosts = new ArrayList<String>();
    private static Hashtable<String, ASyndicateFileSystem> syndicateFSs = new Hashtable<String, ASyndicateFileSystem>();
    
    public SliversMonitor(Configuration conf) throws IOException {
        String[] slivers = HSynthConfigUtil.listHSynthHost(conf);
        for(String slave : slivers) {
            if(!syndicateFSs.containsKey(slave)) {
                hosts.add(slave);
                
                ASyndicateFileSystem fs = createHSynthFS(conf, slave);
                syndicateFSs.put(slave, fs);
            }
        }
    }
    
    private static ASyndicateFileSystem createHSynthFS(Configuration conf, String host) throws IOException {
        SyndicateFSConfiguration sconf = HSynthConfigUtil.createSyndicateConf(conf, host);
        try {
            return FileSystemFactory.getInstance(sconf);
        } catch (InstantiationException ex) {
            throw new IOException(ex.getCause());
        }
    }
    
    public List<SliversMonitorResults<String[]>> listExtendedAttrs(SyndicateFSPath path) throws IOException {
        List<SliversMonitorResults<String[]>> attrs = new ArrayList<SliversMonitorResults<String[]>>();
        
        for(String host : hosts) {
            ASyndicateFileSystem fs = syndicateFSs.get(host);
            if(fs != null) {
                String[] attr = fs.listExtendedAttrs(path);

                SliversMonitorResults<String[]> result = new SliversMonitorResults<String[]>(fs.getConfiguration().getHost());
                result.setResult(attr);

                attrs.add(result);
            }
        }
        return attrs;
    }
    
    public List<SliversMonitorResults<String>> getExtendedAttrs(SyndicateFSPath path, String name) throws IOException {
        List<SliversMonitorResults<String>> attrs = new ArrayList<SliversMonitorResults<String>>();
        
        for(String host : hosts) {
            ASyndicateFileSystem fs = syndicateFSs.get(host);
            if(fs != null) {
                String attr = fs.getExtendedAttr(path, name);
                
                SliversMonitorResults<String> result = new SliversMonitorResults<String>(fs.getConfiguration().getHost());
                result.setResult(attr);

                attrs.add(result);
            }
        }
        return attrs;
    }
}

