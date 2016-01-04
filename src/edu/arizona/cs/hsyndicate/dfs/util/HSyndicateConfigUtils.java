/*
 * Copyright 2015 iychoi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.arizona.cs.hsyndicate.dfs.util;

import edu.arizona.cs.hsyndicate.fs.SyndicateFSConfiguration;
import edu.arizona.cs.hsyndicate.util.IPUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class HSyndicateConfigUtils {
    
    public static final Log LOG = LogFactory.getLog(HSyndicateConfigUtils.class);
    
    public static final String CONFIG_HSYNDICATE_USER_GATEWAY_HOSTS = "fs.hsyndicate.usergateway.hosts";
    public static final String CONFIG_HSYNDICATE_USER_GATEWAY_DEFAULT_PORT = "fs.hsyndicate.usergateway.port";

    private static String autoDetectedDataNodes = null;
    
    public static String getHSyndicateUGHosts(Configuration conf) {
        String ug_hosts = conf.get(CONFIG_HSYNDICATE_USER_GATEWAY_HOSTS, null);
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
    
    public static String[] listHSyndicateUGHosts(Configuration conf) {
        String hosts = getHSyndicateUGHosts(conf);
        if(hosts == null) {
            return null;
        } else {
            return hosts.split(",");
        }
    }
    
    public static int getHSyndicateUGDefaultPort(Configuration conf) {
        return conf.getInt(CONFIG_HSYNDICATE_USER_GATEWAY_DEFAULT_PORT, 0);
    }
    
    public static String[] listHSyndicateUGHostsWithPort(Configuration conf) {
        String[] hosts = listHSyndicateUGHosts(conf);
        int port = getHSyndicateUGDefaultPort(conf);
        
        List<String> hostList = new ArrayList<String>();
        if(hosts != null) {
            for(String host : hosts) {
                if(host.indexOf(":") > 0) {
                    hostList.add(host);
                } else {
                    if(port > 0) {
                        hostList.add(host + ":" + port);
                    } else {
                        hostList.add(host);
                    }
                }
            }
        }
        
        return hostList.toArray(new String[0]);
    }
    
    private static String pickClosestUGHostWithPort(Configuration conf) {
        String[] UGHostsWithPort = listHSyndicateUGHosts(conf);
        if(UGHostsWithPort == null || UGHostsWithPort.length == 0) {
            int port = HSyndicateConfigUtils.getHSyndicateUGDefaultPort(conf);
            if(port > 0) {
                return "localhost:" + port;
            } else {
                return "localhost";
            }
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
    
    public static SyndicateFSConfiguration createSyndicateConf(Configuration conf) throws IOException {
        String ug_address = pickClosestUGHostWithPort(conf);
        
        return createSyndicateConf(conf, ug_address);
    }
    
    public static SyndicateFSConfiguration createSyndicateConf(Configuration conf, String ug_address) throws IOException {
        SyndicateFSConfiguration sconf = new SyndicateFSConfiguration();

        String hostname = ug_address;
        int pos = hostname.indexOf(":");
        if(pos > 0) {
            hostname = hostname.substring(0, pos);
        }
        
        // host
        sconf.setHost(hostname);
        
        if(pos > 0) {
            int port = Integer.parseInt(ug_address.substring(pos + 1));
            if(port > 0) {
                sconf.setPort(port);
            } else {
                port = HSyndicateConfigUtils.getHSyndicateUGDefaultPort(conf);
                if(port > 0) {
                    sconf.setPort(port);
                }
            }
        } else {
            int port = HSyndicateConfigUtils.getHSyndicateUGDefaultPort(conf);
            if(port > 0) {
                sconf.setPort(port);
            }
        }
        
        return sconf;
    }
}
