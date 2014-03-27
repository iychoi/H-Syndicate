package org.apache.hadoop.fs.hsynth.util;

import edu.arizona.cs.syndicate.fs.SyndicateFSConfiguration;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class HSynthConfigUtil {
    
    public static final Log LOG = LogFactory.getLog(HSynthConfigUtil.class);
    
    public static final String CONFIG_HSYNTH_HOSTS = "fs.hsynth.hosts";
    public static final String CONFIG_HSYNTH_PORT = "fs.hsynth.port";
    public static final String CONFIG_HSYNTH_METADATA_CACHE_SIZE = "fs.hsynth.metadata.cache.size";
    public static final String CONFIG_HSYNTH_METADATA_CACHE_TIMEOUT = "fs.hsynth.metadata.cache.timeout";
    public static final String CONFIG_HSYNTH_DEFAULT_INPUT_BUFFER_SIZE = "fs.hsynth.input.buffer.size";
    public static final String CONFIG_HSYNTH_DEFAULT_OUTPUT_BUFFER_SIZE = "fs.hsynth.output.buffer.size";

    public static final int DEFAULT_PORT = 7910;
    // unlimited size
    public static final int DEFAULT_MAX_METADATA_CACHE_SIZE = 0;
    // no timeout
    public static final int DEFAULT_METADATA_CACHE_TIMEOUT = 0;
    public static final int DEFAULT_BUFFER_SIZE = 1024 * 800;
    
    public static String getHSynthHosts(Configuration conf) {
        return conf.get(CONFIG_HSYNTH_HOSTS);
    }
    
    public static void setHSynthHosts(Configuration conf, String hosts) {
        conf.set(CONFIG_HSYNTH_HOSTS, hosts);
    }
    
    public static int getHSynthPort(Configuration conf) {
        return conf.getInt(CONFIG_HSYNTH_PORT, DEFAULT_PORT);
    }
    
    public static void setHSynthPort(Configuration conf, int port) {
        conf.setInt(CONFIG_HSYNTH_PORT, port);
    }
    
    public static int getHSynthMaxMetadataCacheSize(Configuration conf) {
        return conf.getInt(CONFIG_HSYNTH_METADATA_CACHE_SIZE, DEFAULT_MAX_METADATA_CACHE_SIZE);
    }
    
    public static void setHSynthMaxMetadataCacheSize(Configuration conf, int size) {
        conf.setInt(CONFIG_HSYNTH_METADATA_CACHE_SIZE, size);
    }
    
    public static int getHSynthMetadataCacheTimeout(Configuration conf) {
        return conf.getInt(CONFIG_HSYNTH_METADATA_CACHE_TIMEOUT, DEFAULT_METADATA_CACHE_TIMEOUT);
    }
    
    public static void setHSynthMetadataCacheTimeout(Configuration conf, int timeout_sec) {
        conf.setInt(CONFIG_HSYNTH_METADATA_CACHE_TIMEOUT, timeout_sec);
    }
    
    public static int getHSynthInputBufferSize(Configuration conf) {
        return conf.getInt(CONFIG_HSYNTH_DEFAULT_INPUT_BUFFER_SIZE, DEFAULT_BUFFER_SIZE);
    }
    
    public static void setHSynthInputBufferSize(Configuration conf, int buffer_size) {
        conf.setInt(CONFIG_HSYNTH_DEFAULT_INPUT_BUFFER_SIZE, buffer_size);
    }
    
    public static int getHSynthOutputBufferSize(Configuration conf) {
        return conf.getInt(CONFIG_HSYNTH_DEFAULT_OUTPUT_BUFFER_SIZE, DEFAULT_BUFFER_SIZE);
    }
    
    public static void setHSynthOutputBufferSize(Configuration conf, int buffer_size) {
        conf.setInt(CONFIG_HSYNTH_DEFAULT_OUTPUT_BUFFER_SIZE, buffer_size);
    }
    
    public static SyndicateFSConfiguration createSyndicateConf(Configuration conf, String host) throws IOException {
        SyndicateFSConfiguration sconf = new SyndicateFSConfiguration();

        // host
        sconf.setHost(host);
        
        // port
        int port = HSynthConfigUtil.getHSynthPort(conf);
        sconf.setPort(port);
        
        int metadataCacheSize = HSynthConfigUtil.getHSynthMaxMetadataCacheSize(conf);
        sconf.setMaxMetadataCacheSize(metadataCacheSize);
        
        int metadataCacheTimeout = HSynthConfigUtil.getHSynthMetadataCacheTimeout(conf);
        sconf.setMetadataCacheTimeout(metadataCacheTimeout);
        
        return sconf;
    }
}
