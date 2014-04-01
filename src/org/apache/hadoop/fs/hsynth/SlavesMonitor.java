package org.apache.hadoop.fs.hsynth;

import edu.arizona.cs.syndicate.fs.ASyndicateFileSystem;
import edu.arizona.cs.syndicate.fs.SyndicateFSConfiguration;
import edu.arizona.cs.syndicate.fs.SyndicateFSPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.hsynth.util.HSynthConfigUtils;

public class SlavesMonitor {
    private static final Log LOG = LogFactory.getLog(SlavesMonitor.class);
    
    private static List<String> slaves = new ArrayList<String>();
    private static Hashtable<String, ASyndicateFileSystem> syndicateFSs = new Hashtable<String, ASyndicateFileSystem>();
    private static Hashtable<String, String> slave_hostnames = new Hashtable<String, String>();
    
    public SlavesMonitor(Configuration conf) throws IOException {
        String[] hosts = HSynthConfigUtils.listHSynthHost(conf);
        String[] hostnames = HSynthConfigUtils.listHSynthHostname(conf);
        
        for(int i=0;i<hosts.length;i++) {
            String host = hosts[i];
            // todo later, host name can be obtained automatically
            String hostname = hostnames[i];
            
            if(!syndicateFSs.containsKey(host)) {
                slaves.add(host);
                slave_hostnames.put(host, hostname);
                
                ASyndicateFileSystem fs = createHSynthFS(conf, host);
                syndicateFSs.put(host, fs);
            }
        }
    }
    
    private static ASyndicateFileSystem createHSynthFS(Configuration conf, String host) throws IOException {
        SyndicateFSConfiguration sconf = HSynthConfigUtils.createSyndicateConf(conf, host);
        try {
            return FileSystemFactory.getInstance(sconf);
        } catch (InstantiationException ex) {
            throw new IOException(ex.getCause());
        }
    }
    
    public List<SlavesMonitorResults<byte[]>> getLocalCachedBlockInfo(SyndicateFSPath path) throws IOException {
        List<SlavesMonitorResults<byte[]>> bitmaps = new ArrayList<SlavesMonitorResults<byte[]>>();
        
        for(String host : slaves) {
            ASyndicateFileSystem fs = syndicateFSs.get(host);
            if(fs != null) {
                byte[] bitmap = fs.getLocalCacheBlocks(path);
                if(bitmap != null) {
                    int caches = 0;
                    for(int i=0;i<bitmap.length;i++) {
                        if(bitmap[i] == 1) {
                            caches++;
                        }
                    }
                    LOG.info("host : " + host + " has " + caches + " caches of " + path);
                }
                
                String hostname = slave_hostnames.get(host);
                SlavesMonitorResults<byte[]> result = new SlavesMonitorResults<byte[]>(host, hostname);
                result.setResult(bitmap);

                bitmaps.add(result);
            }
        }
        return bitmaps;
    }
    
    public List<SlavesMonitorResults<String[]>> listExtendedAttrs(SyndicateFSPath path) throws IOException {
        List<SlavesMonitorResults<String[]>> attrs = new ArrayList<SlavesMonitorResults<String[]>>();
        
        for(String host : slaves) {
            ASyndicateFileSystem fs = syndicateFSs.get(host);
            if(fs != null) {
                String[] attr = fs.listExtendedAttrs(path);

                String hostname = slave_hostnames.get(host);
                SlavesMonitorResults<String[]> result = new SlavesMonitorResults<String[]>(host, hostname);
                result.setResult(attr);

                attrs.add(result);
            }
        }
        return attrs;
    }
    
    public List<SlavesMonitorResults<String>> getExtendedAttrs(SyndicateFSPath path, String name) throws IOException {
        List<SlavesMonitorResults<String>> attrs = new ArrayList<SlavesMonitorResults<String>>();
        
        for(String host : slaves) {
            ASyndicateFileSystem fs = syndicateFSs.get(host);
            if(fs != null) {
                String attr = fs.getExtendedAttr(path, name);
                
                String hostname = slave_hostnames.get(host);
                SlavesMonitorResults<String> result = new SlavesMonitorResults<String>(host, hostname);
                result.setResult(attr);

                attrs.add(result);
            }
        }
        return attrs;
    }
}

