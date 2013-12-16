package edu.arizona.cs.hsynth.hadoop.util;

import edu.arizona.cs.hsynth.fs.HSynthFSBackend;
import edu.arizona.cs.hsynth.fs.HSynthFSConfiguration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

public class HSynthConfigUtil {
    
    public static final Log LOG = LogFactory.getLog(HSynthConfigUtil.class);
    
    private static edu.arizona.cs.hsynth.fs.HSynthFSConfiguration hsynthFSConfigurationInstance;
    
    public static final String HSYNTHFS_BACKEND = "hsynth.backend";
    
    public synchronized static edu.arizona.cs.hsynth.fs.HSynthFSConfiguration getHSynthFSConfigurationInstance(Configuration conf) throws InstantiationException {
        if(hsynthFSConfigurationInstance == null) {
            String backendString = getHSynthFSBackend(conf);
            if(backendString == null || backendString.trim().equals("")) {
                LOG.error("hsynth fs backend is not specified");
                throw new InstantiationException("hsynth fs backend is not specified");
            }
            
            Class<? extends edu.arizona.cs.hsynth.fs.HSynthFSConfiguration> confclazz = HSynthFSBackend.findBackendConfigurationByName(backendString);
            if(confclazz == null) {
                LOG.error("null hsynth fs configuration");
                throw new InstantiationException("null hsynth fs configuration");
            }
            
            Hashtable<String, String> params = getHSynthFSParams(conf);
            if(params == null) {
                LOG.error("null hsynth fs params");
                throw new InstantiationException("null serialized hsynth fs params");
            }
            
            edu.arizona.cs.hsynth.fs.HSynthFSConfiguration fsconf = (edu.arizona.cs.hsynth.fs.HSynthFSConfiguration)ReflectionUtils.newInstance(confclazz, conf);
            try {
                fsconf.load(params);
            } catch (IllegalAccessException ex) {
                LOG.error(ex);
                throw new InstantiationException(ex.toString());
            }
            
            hsynthFSConfigurationInstance = fsconf;
        }
        
        return hsynthFSConfigurationInstance;
    }
    
    public static void setHSynthFSConfiguration(Configuration conf, edu.arizona.cs.hsynth.fs.HSynthFSConfiguration fsconf) {
        setHSynthFSBackend(conf, fsconf.getBackendName());
        setHSynthFSParams(conf, fsconf.getParams());
    }
    
    private static String getHSynthFSBackend(Configuration conf) {
        return conf.get(HSYNTHFS_BACKEND);
    }
    
    private static void setHSynthFSBackend(Configuration conf, String backend) {
        conf.set(HSYNTHFS_BACKEND, backend);
    }
    
    private static Hashtable<String, String> getHSynthFSParams(Configuration conf) {
        Hashtable<String, String> params = new Hashtable<String, String>();
        
        Iterator<Map.Entry<String, String>> iter = conf.iterator();
        while(iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            if(entry.getKey().startsWith(HSynthFSConfiguration.HSYNTH_CONF_PREFIX)) {
                params.put(entry.getKey(), entry.getValue());
            }
        }
        
        return params;
    }
    
    private static void setHSynthFSParams(Configuration conf, Hashtable<String, String> params) {
        for(String key : params.keySet()) {
            conf.set(key, params.get(key));
        }
    }
}
