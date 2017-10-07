/*
   Copyright 2016 The Trustees of University of Arizona

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
package hsyndicate.hadoop.utils;

import hsyndicate.fs.SyndicateFSConfiguration;
import hsyndicate.utils.IPUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class HSyndicateConfigUtils {
    
    public static final Log LOG = LogFactory.getLog(HSyndicateConfigUtils.class);
    
    public static final String CONFIG_SYNDICATE_USER_GATEWAY_HOSTS = "fs.hsyndicate.hosts";
    public static final String CONFIG_SYNDICATE_USER_GATEWAY_DEFAULT_PORT = "fs.hsyndicate.port";
    public static final String CONFIG_SYNDICATE_USER_GATEWAY_SESSION_PREFIX = "fs.hsyndicate.session";
    
    private static String autoDetectedDataNodes = null;
    
    public static String getSyndicateUGHosts(Configuration conf) {
        String ug_hosts = conf.get(CONFIG_SYNDICATE_USER_GATEWAY_HOSTS, null);
        if(ug_hosts == null) {
            // use auto detect
            if(autoDetectedDataNodes == null) {
                try {
                    autoDetectedDataNodes = DFSNodeInfoUtils.getDataNodesCommaSeparated(conf);
                } catch (IOException ex) {
                    LOG.info("failed to read DFS data node info.", ex);
                    autoDetectedDataNodes = null;
                }
            }
            return autoDetectedDataNodes;
        } else {
            return ug_hosts;
        }
    }
    
    public static String[] listSyndicateUGHosts(Configuration conf) {
        String hosts = getSyndicateUGHosts(conf);
        if(hosts == null) {
            return null;
        } else {
            return hosts.split(",");
        }
    }
    
    public static int getSyndicateUGDefaultPort(Configuration conf) {
        return conf.getInt(CONFIG_SYNDICATE_USER_GATEWAY_DEFAULT_PORT, 8888);
    }
    
    public static String[] listSyndicateUGHostsWithPort(Configuration conf) {
        String[] hosts = listSyndicateUGHosts(conf);
        int port = getSyndicateUGDefaultPort(conf);
        
        List<String> hostList = new ArrayList<String>();
        if(hosts != null) {
            for(String host : hosts) {
                if(host.indexOf(":") > 0) {
                    hostList.add(host);
                } else {
                    if(port > 0) {
                        hostList.add(String.format("%s:%d", host, port));
                    } else {
                        hostList.add(host);
                    }
                }
            }
        }
        
        return hostList.toArray(new String[0]);
    }
    
    private static String pickClosestUGHostWithPort(Configuration conf) {
        String[] UGHostsWithPort = listSyndicateUGHosts(conf);
        if(UGHostsWithPort == null || UGHostsWithPort.length == 0) {
            // error
            return null;
        }
        
        String selectedAddr = IPUtils.selectLocalIPAddress(UGHostsWithPort);
        if(selectedAddr != null && !selectedAddr.isEmpty()) {
            return selectedAddr;
        }
        
        // pick random
        List<String> contactPoints = new ArrayList<String>();
        for(String host : UGHostsWithPort) {
            contactPoints.add(host);
        }

        Collections.shuffle(contactPoints);
        return contactPoints.get(0);
    }
    
    public static String getSyndicateUGSessionKey(Configuration conf, String sessionName) {
        try {
            char[] sessionKey = conf.getPassword(String.format("%s.%s.key", CONFIG_SYNDICATE_USER_GATEWAY_SESSION_PREFIX, sessionName));
            if(sessionKey != null) {
                return new String(sessionKey);
            }
            LOG.info(String.format("could not read a session key of %s", sessionName));
            return null;
        } catch (IOException ex) {
            LOG.error("failed to read a session key of " + sessionName, ex);
            return null;
        }
    }
    
    public static SyndicateFSConfiguration createSyndicateConf(Configuration conf) throws IOException {
        String ugAddress = pickClosestUGHostWithPort(conf);
        return createSyndicateConf(conf, ugAddress);
    }
    
    public static SyndicateFSConfiguration createSyndicateConf(Configuration conf, String ugAddress) throws IOException {
        SyndicateFSConfiguration sconf = new SyndicateFSConfiguration();
        
        // default port
        int defaultPort = HSyndicateConfigUtils.getSyndicateUGDefaultPort(conf);
        if(defaultPort > 0) {
            sconf.setPort(defaultPort);
        }
        
        if(ugAddress != null && !ugAddress.isEmpty()) {
            String hostname = ugAddress;
            int pos = hostname.indexOf(":");
            if(pos > 0) {
                hostname = hostname.substring(0, pos);
            }

            // host
            sconf.setHost(hostname);

            if(pos > 0) {
                int port = Integer.parseInt(ugAddress.substring(pos + 1));
                if(port > 0) {
                    sconf.setPort(port);
                }
            }
        }
        
        return sconf;
    }
}
