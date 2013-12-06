package edu.arizona.cs.hsynth.hadoop.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

public class HSynthConfigUtil {
    
    public static final Log LOG = LogFactory.getLog(HSynthConfigUtil.class);
    
    private static edu.arizona.cs.hsynth.fs.HSynthFSConfiguration hsynthFSConfigurationInstance;
    
    public static final String HSYNTHFS_CONFIGURATION = "hsynth.conf.class";
    public static final String HSYNTHFS_CONFIGURATION_SERIALIZED = "hsynth.conf.serializedconf";
    
    public synchronized static edu.arizona.cs.hsynth.fs.HSynthFSConfiguration getHSynthFSConfigurationInstance(Configuration conf) throws InstantiationException {
        if(hsynthFSConfigurationInstance == null) {
            Class<? extends edu.arizona.cs.hsynth.fs.HSynthFSConfiguration> confclazz = getHSynthFSConfigurationClass(conf);
            if(confclazz == null) {
                LOG.error("null hsynth fs configuration");
                throw new InstantiationException("null hsynth fs configuration");
            }
            
            String serializedConf = getHSynthFSConfigurationString(conf);
            if(serializedConf == null) {
                LOG.error("null serialized hsynth fs configuration");
                throw new InstantiationException("null serialized hsynth fs configuration");
            }
            
            edu.arizona.cs.hsynth.fs.HSynthFSConfiguration fsconf = (edu.arizona.cs.hsynth.fs.HSynthFSConfiguration)ReflectionUtils.newInstance(confclazz, conf);
            try {
                fsconf.deserialize(serializedConf);
            } catch (IllegalAccessException ex) {
                LOG.error(ex);
                throw new InstantiationException(ex.toString());
            }
            
            hsynthFSConfigurationInstance = fsconf;
        }
        
        return hsynthFSConfigurationInstance;
    }
    
    public static void setHSynthFSConfiguration(Configuration conf, edu.arizona.cs.hsynth.fs.HSynthFSConfiguration fsconf) {
        setHSynthFSConfigurationClass(conf, fsconf.getClass());
        setHSynthFSConfigurationString(conf, fsconf.serialize());
    }
    
    private static Class<? extends edu.arizona.cs.hsynth.fs.HSynthFSConfiguration> getHSynthFSConfigurationClass(Configuration conf) {
        return conf.getClass(HSYNTHFS_CONFIGURATION, null, edu.arizona.cs.hsynth.fs.HSynthFSConfiguration.class);
    }

    private static void setHSynthFSConfigurationClass(Configuration conf, Class<? extends edu.arizona.cs.hsynth.fs.HSynthFSConfiguration> val) {
        conf.setClass(HSYNTHFS_CONFIGURATION, val, edu.arizona.cs.hsynth.fs.HSynthFSConfiguration.class);
    }
    
    private static String getHSynthFSConfigurationString(Configuration conf) {
        return conf.get(HSYNTHFS_CONFIGURATION_SERIALIZED, null);
    }
    
    private static void setHSynthFSConfigurationString(Configuration conf, String val) {
        conf.set(HSYNTHFS_CONFIGURATION_SERIALIZED, val);
    }
}
