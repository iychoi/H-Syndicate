package org.apache.hadoop.fs.hsynth;

import org.apache.hadoop.fs.hsynth.util.SyndicateFileSystemFactory;
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

public class HSynthUGMonitor {
    private static final Log LOG = LogFactory.getLog(HSynthUGMonitor.class);
    
    private static List<String> usergateway_hostnames = new ArrayList<String>();
    private static Hashtable<String, ASyndicateFileSystem> syndicateFSs = new Hashtable<String, ASyndicateFileSystem>();
    
    public HSynthUGMonitor(Configuration conf) throws IOException {
        String[] gateway_hostnames = HSynthConfigUtils.listHSynthUGHostname(conf);
        
        for(int i=0;i<gateway_hostnames.length;i++) {
            String gateway_hostname = gateway_hostnames[i];
            
            if(!syndicateFSs.containsKey(gateway_hostname)) {
                usergateway_hostnames.add(gateway_hostname);
                
                ASyndicateFileSystem fs = createHSynthFS(conf, gateway_hostname);
                syndicateFSs.put(gateway_hostname, fs);
            }
        }
    }
    
    private static ASyndicateFileSystem createHSynthFS(Configuration conf, String address) throws IOException {
        SyndicateFSConfiguration sconf = HSynthConfigUtils.createSyndicateConf(conf, address);
        try {
            return SyndicateFileSystemFactory.getInstance(sconf);
        } catch (InstantiationException ex) {
            throw new IOException(ex.getCause());
        }
    }
    
    public List<HSynthUGMonitorResults<byte[]>> getLocalCachedBlockInfo(SyndicateFSPath path) throws IOException {
        List<HSynthUGMonitorResults<byte[]>> bitmaps = new ArrayList<HSynthUGMonitorResults<byte[]>>();
        
        for(String gateway_hostname : usergateway_hostnames) {
            ASyndicateFileSystem fs = syndicateFSs.get(gateway_hostname);
            if(fs != null) {
                byte[] bitmap = fs.getLocalCacheBlocks(path);
                int sum_caches = 0;
                
                if(bitmap != null) {
                    for(int i=0;i<bitmap.length;i++) {
                        if(bitmap[i] == 1) {
                            sum_caches++;
                        }
                    }
                }
                
                LOG.info("UserGateway : " + gateway_hostname + " has " + sum_caches + " caches of " + path);
                
                HSynthUGMonitorResults<byte[]> result = new HSynthUGMonitorResults<byte[]>(gateway_hostname);
                result.setResult(bitmap);

                bitmaps.add(result);
            }
        }
        return bitmaps;
    }
    
    public List<HSynthUGMonitorResults<String[]>> listExtendedAttrs(SyndicateFSPath path) throws IOException {
        List<HSynthUGMonitorResults<String[]>> attrs = new ArrayList<HSynthUGMonitorResults<String[]>>();
        
        for(String gateway_hostname : usergateway_hostnames) {
            ASyndicateFileSystem fs = syndicateFSs.get(gateway_hostname);
            if(fs != null) {
                String[] attr_names = fs.listExtendedAttrs(path);

                HSynthUGMonitorResults<String[]> result = new HSynthUGMonitorResults<String[]>(gateway_hostname);
                result.setResult(attr_names);

                attrs.add(result);
            }
        }
        return attrs;
    }
    
    public List<HSynthUGMonitorResults<String>> getExtendedAttrs(SyndicateFSPath path, String attr_name) throws IOException {
        List<HSynthUGMonitorResults<String>> attrs = new ArrayList<HSynthUGMonitorResults<String>>();
        
        for(String gateway_hostname : usergateway_hostnames) {
            ASyndicateFileSystem fs = syndicateFSs.get(gateway_hostname);
            if(fs != null) {
                String attr_value = fs.getExtendedAttr(path, attr_name);
                
                HSynthUGMonitorResults<String> result = new HSynthUGMonitorResults<String>(gateway_hostname);
                result.setResult(attr_value);

                attrs.add(result);
            }
        }
        return attrs;
    }
}

