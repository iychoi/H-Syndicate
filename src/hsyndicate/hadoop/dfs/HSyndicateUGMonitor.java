/*
   Copyright 2015 The Trustees of Princeton University

   Licensed under the Apache License, Version 2.0 (the "License" );
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package hsyndicate.hadoop.dfs;

import hsyndicate.fs.SyndicateFSConfiguration;
import hsyndicate.fs.SyndicateFSPath;
import hsyndicate.fs.SyndicateFileSystem;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import hsyndicate.hadoop.utils.HSyndicateConfigUtils;
import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

public class HSyndicateUGMonitor implements Closeable {
    private static final Log LOG = LogFactory.getLog(HSyndicateUGMonitor.class);
    
    private List<String> usergateway_hostnames = new ArrayList<String>();
    private Map<String, SyndicateFileSystem> syndicateFSs = new HashMap<String, SyndicateFileSystem>();
    
    public HSyndicateUGMonitor(Configuration conf) throws IOException {
        String[] gateway_hostnames = HSyndicateConfigUtils.listSyndicateUGHostsWithPort(conf);
        
        for (String gateway_hostname : gateway_hostnames) {
            if(!syndicateFSs.containsKey(gateway_hostname)) {
                usergateway_hostnames.add(gateway_hostname);
                
                SyndicateFileSystem fs = createHSyndicateFS(conf, gateway_hostname);
                syndicateFSs.put(gateway_hostname, fs);
            }
        }
    }
    
    @Override
    public synchronized void close() throws IOException {
        this.usergateway_hostnames.clear();
        for(SyndicateFileSystem fs : this.syndicateFSs.values()) {
            fs.close();
        }
        this.syndicateFSs.clear();
    }
    
    private static SyndicateFileSystem createHSyndicateFS(Configuration conf, String address) throws IOException {
        SyndicateFSConfiguration sconf = HSyndicateConfigUtils.createSyndicateConf(conf, address);
        try {
            return new SyndicateFileSystem(sconf);
        } catch (InstantiationException ex) {
            throw new IOException(ex.getCause());
        }
    }
    
    public synchronized List<String> getUserGatewayHosts() {
        return usergateway_hostnames;
    }
    
    public synchronized List<HSyndicateUGMonitorResults<byte[]>> getLocalCachedBlockInfo(SyndicateFSPath path) throws IOException {
        List<HSyndicateUGMonitorResults<byte[]>> bitmaps = new ArrayList<HSyndicateUGMonitorResults<byte[]>>();
        
        for(String gateway_hostname : usergateway_hostnames) {
            SyndicateFileSystem fs = syndicateFSs.get(gateway_hostname);
            if(fs != null) {
                byte[] bitmap = fs.getLocalCachedBlocks(path);
                int sum_caches = 0;
                
                if(bitmap != null) {
                    for(int i=0;i<bitmap.length;i++) {
                        if(bitmap[i] == 1) {
                            sum_caches++;
                        }
                    }
                }
                
                LOG.info("UserGateway : " + gateway_hostname + " has " + sum_caches + " caches of " + path);
                
                HSyndicateUGMonitorResults<byte[]> result = new HSyndicateUGMonitorResults<byte[]>(gateway_hostname);
                result.setResult(bitmap);

                bitmaps.add(result);
            }
        }
        return bitmaps;
    }
}

