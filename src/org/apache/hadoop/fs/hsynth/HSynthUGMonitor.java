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
    
    private static List<String> usergateway_addresses = new ArrayList<String>();
    private static Hashtable<String, ASyndicateFileSystem> syndicateFSs = new Hashtable<String, ASyndicateFileSystem>();
    private static Hashtable<String, String> usergateway_hostnames = new Hashtable<String, String>();
    
    public HSynthUGMonitor(Configuration conf) throws IOException {
        String[] gateway_addresses = HSynthConfigUtils.listHSynthUGAddresses(conf);
        String[] gateway_hostnames = HSynthConfigUtils.listHSynthUGHostname(conf);
        
        for(int i=0;i<gateway_addresses.length;i++) {
            String gateway_address = gateway_addresses[i];
            // todo later, host name can be obtained automatically
            String gateway_hostname = gateway_hostnames[i];
            
            if(!syndicateFSs.containsKey(gateway_address)) {
                usergateway_addresses.add(gateway_address);
                usergateway_hostnames.put(gateway_address, gateway_hostname);
                
                ASyndicateFileSystem fs = createHSynthFS(conf, gateway_address);
                syndicateFSs.put(gateway_address, fs);
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
        
        for(String gateway_addr : usergateway_addresses) {
            ASyndicateFileSystem fs = syndicateFSs.get(gateway_addr);
            if(fs != null) {
                byte[] bitmap = fs.getLocalCacheBlocks(path);
                if(bitmap != null) {
                    int caches = 0;
                    for(int i=0;i<bitmap.length;i++) {
                        if(bitmap[i] == 1) {
                            caches++;
                        }
                    }
                    LOG.info("UserGateway : " + gateway_addr + " has " + caches + " caches of " + path);
                }
                
                String gateway_hostname = usergateway_hostnames.get(gateway_addr);
                HSynthUGMonitorResults<byte[]> result = new HSynthUGMonitorResults<byte[]>(gateway_addr, gateway_hostname);
                result.setResult(bitmap);

                bitmaps.add(result);
            }
        }
        return bitmaps;
    }
    
    public List<HSynthUGMonitorResults<String[]>> listExtendedAttrs(SyndicateFSPath path) throws IOException {
        List<HSynthUGMonitorResults<String[]>> attrs = new ArrayList<HSynthUGMonitorResults<String[]>>();
        
        for(String gateway_addr : usergateway_addresses) {
            ASyndicateFileSystem fs = syndicateFSs.get(gateway_addr);
            if(fs != null) {
                String[] attr_names = fs.listExtendedAttrs(path);

                String gateway_hostname = usergateway_hostnames.get(gateway_addr);
                HSynthUGMonitorResults<String[]> result = new HSynthUGMonitorResults<String[]>(gateway_addr, gateway_hostname);
                result.setResult(attr_names);

                attrs.add(result);
            }
        }
        return attrs;
    }
    
    public List<HSynthUGMonitorResults<String>> getExtendedAttrs(SyndicateFSPath path, String attr_name) throws IOException {
        List<HSynthUGMonitorResults<String>> attrs = new ArrayList<HSynthUGMonitorResults<String>>();
        
        for(String gateway_addr : usergateway_addresses) {
            ASyndicateFileSystem fs = syndicateFSs.get(gateway_addr);
            if(fs != null) {
                String attr_value = fs.getExtendedAttr(path, attr_name);
                
                String gateway_hostname = usergateway_hostnames.get(gateway_addr);
                HSynthUGMonitorResults<String> result = new HSynthUGMonitorResults<String>(gateway_addr, gateway_hostname);
                result.setResult(attr_value);

                attrs.add(result);
            }
        }
        return attrs;
    }
}

