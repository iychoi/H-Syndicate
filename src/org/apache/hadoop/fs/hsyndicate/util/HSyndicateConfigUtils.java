package org.apache.hadoop.fs.hsyndicate.util;

import edu.arizona.cs.syndicate.fs.SyndicateFSConfiguration;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class HSyndicateConfigUtils {
    
    public static final Log LOG = LogFactory.getLog(HSyndicateConfigUtils.class);
    
    public static final String CONFIG_HSYNDICATE_USER_GATEWAY_HOSTNAMES = "fs.hsyndicate.usergateway.hostnames";
    public static final String CONFIG_HSYNDICATE_USER_GATEWAY_IPC_PORT = "fs.hsyndicate.usergateway.port";
    public static final String CONFIG_HSYNDICATE_METADATA_CACHE_SIZE = "fs.hsyndicate.metadata.cache.size";
    public static final String CONFIG_HSYNDICATE_METADATA_CACHE_TIMEOUT = "fs.hsyndicate.metadata.cache.timeout";
    public static final String CONFIG_HSYNDICATE_DEFAULT_INPUT_BUFFER_SIZE = "fs.hsyndicate.input.buffer.size";
    public static final String CONFIG_HSYNDICATE_DEFAULT_OUTPUT_BUFFER_SIZE = "fs.hsyndicate.output.buffer.size";

    public static final int DEFAULT_USER_GATEWAY_IPC_PORT = 7910;
    // unlimited size
    public static final int DEFAULT_MAX_METADATA_CACHE_SIZE = 0;
    // no timeout
    public static final int DEFAULT_METADATA_CACHE_TIMEOUT = 0;
    public static final int DEFAULT_BUFFER_SIZE = 1024 * 800;
    
    private static String autoDetectedDataNodes = null;
    
    public static String getHSyndicateUGHostnames(Configuration conf) {
        String ug_hostnames = conf.get(CONFIG_HSYNDICATE_USER_GATEWAY_HOSTNAMES, null);
        if(ug_hostnames == null) {
            // use auto detect
            if(autoDetectedDataNodes == null) {
                try {
                    autoDetectedDataNodes = DFSNodeInfoUtils.getDataNodesCommaSeparated(conf);
                } catch (IOException ex) {
                    LOG.info("failed to read DFS data node info.");
                    autoDetectedDataNodes = null;
                }
            }
            return autoDetectedDataNodes;
        } else {
            return ug_hostnames;
        }
    }
    
    public static String[] listHSyndicateUGHostnames(Configuration conf) {
        return getHSyndicateUGHostnames(conf).split(",");
    }
    
    public static String getHSyndicateUGAddress(Configuration conf, int index) {
        String[] gateway_hostnames = listHSyndicateUGHostnames(conf);
        
        if(index >= gateway_hostnames.length) {
            return null;
        } else {
            return gateway_hostnames[index];
        }
    }
    
    public static void setHSyndicateUGHostnames(Configuration conf, String ug_hostnames) {
        conf.set(CONFIG_HSYNDICATE_USER_GATEWAY_HOSTNAMES, ug_hostnames);
    }
    
    public static int getHSyndicateUGIPCPort(Configuration conf) {
        return conf.getInt(CONFIG_HSYNDICATE_USER_GATEWAY_IPC_PORT, DEFAULT_USER_GATEWAY_IPC_PORT);
    }
    
    public static void setHSyndicateUGIPCPort(Configuration conf, int port) {
        conf.setInt(CONFIG_HSYNDICATE_USER_GATEWAY_IPC_PORT, port);
    }
    
    public static int getHSyndicateMaxMetadataCacheSize(Configuration conf) {
        return conf.getInt(CONFIG_HSYNDICATE_METADATA_CACHE_SIZE, DEFAULT_MAX_METADATA_CACHE_SIZE);
    }
    
    public static void setHSyndicateMaxMetadataCacheSize(Configuration conf, int size) {
        conf.setInt(CONFIG_HSYNDICATE_METADATA_CACHE_SIZE, size);
    }
    
    public static int getHSyndicateMetadataCacheTimeout(Configuration conf) {
        return conf.getInt(CONFIG_HSYNDICATE_METADATA_CACHE_TIMEOUT, DEFAULT_METADATA_CACHE_TIMEOUT);
    }
    
    public static void setHSyndicateMetadataCacheTimeout(Configuration conf, int timeout_sec) {
        conf.setInt(CONFIG_HSYNDICATE_METADATA_CACHE_TIMEOUT, timeout_sec);
    }
    
    public static int getHSyndicateInputBufferSize(Configuration conf) {
        return conf.getInt(CONFIG_HSYNDICATE_DEFAULT_INPUT_BUFFER_SIZE, DEFAULT_BUFFER_SIZE);
    }
    
    public static void setHSyndicateInputBufferSize(Configuration conf, int buffer_size) {
        conf.setInt(CONFIG_HSYNDICATE_DEFAULT_INPUT_BUFFER_SIZE, buffer_size);
    }
    
    public static int getHSyndicateOutputBufferSize(Configuration conf) {
        return conf.getInt(CONFIG_HSYNDICATE_DEFAULT_OUTPUT_BUFFER_SIZE, DEFAULT_BUFFER_SIZE);
    }
    
    public static void setHSyndicateOutputBufferSize(Configuration conf, int buffer_size) {
        conf.setInt(CONFIG_HSYNDICATE_DEFAULT_OUTPUT_BUFFER_SIZE, buffer_size);
    }
    
    public static SyndicateFSConfiguration createSyndicateConf(Configuration conf, String ug_address) throws IOException {
        SyndicateFSConfiguration sconf = new SyndicateFSConfiguration();

        // host
        sconf.setHostAddress(ug_address);
        
        // port
        int port = HSyndicateConfigUtils.getHSyndicateUGIPCPort(conf);
        sconf.setPort(port);
        
        int metadataCacheSize = HSyndicateConfigUtils.getHSyndicateMaxMetadataCacheSize(conf);
        sconf.setMaxMetadataCacheSize(metadataCacheSize);
        
        int metadataCacheTimeout = HSyndicateConfigUtils.getHSyndicateMetadataCacheTimeout(conf);
        sconf.setMetadataCacheTimeout(metadataCacheTimeout);
        
        return sconf;
    }
}
