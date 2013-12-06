package edu.arizona.cs.hsynth.hadoop;

import edu.arizona.cs.hsynth.hadoop.util.HSynthConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class HSynthConfig extends Configuration {

    public static final Log LOG = LogFactory.getLog(HSynthConfig.class);
    
    public HSynthConfig(Configuration hadoopConfig) {
        super(hadoopConfig);
    }
    
    public edu.arizona.cs.hsynth.fs.HSynthFSConfiguration getFSConfiguration() throws InstantiationException {
        return HSynthConfigUtil.getHSynthFSConfigurationInstance(this);
    }
    
    public void setHSynthFSConfiguration(edu.arizona.cs.hsynth.fs.HSynthFSConfiguration fsconf) {
        HSynthConfigUtil.setHSynthFSConfiguration(this, fsconf);
    }
}
