/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.apache.hadoop.fs.hsynth.test;

import edu.arizona.cs.syndicate.fs.SyndicateFSPath;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.hsynth.SliversMonitor;
import org.apache.hadoop.fs.hsynth.SliversMonitorResults;
import org.apache.hadoop.fs.hsynth.util.HSynthBlockUtils;
import org.apache.hadoop.fs.hsynth.util.HSynthConfigUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author iychoi
 */
public class testHSynthLocalCache extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(testHSynthLocalCache.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new testHSynthLocalCache(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        
        String hosts = HSynthConfigUtils.getHSynthHosts(conf);
        hosts = "localhost," + hosts;
        HSynthConfigUtils.setHSynthHosts(conf, hosts);
        
        SliversMonitor monitor = new SliversMonitor(conf);
        SyndicateFSPath path = new SyndicateFSPath(args[0]);
/*        
        List<SliversMonitorResults<String[]>> listExtendedAttrs = monitor.listExtendedAttrs(path);
        for(SliversMonitorResults<String[]> attrs : listExtendedAttrs) {
            if(attrs != null) {
                // Host
                LOG.info("===========================================");
                LOG.info("host : " + attrs.getHostname());
                LOG.info("===========================================");
                
                if(attrs.getResult() != null) {
                    // Length

                    LOG.info("path " + path.getPath() + " len " + attrs.getResult().length);

                    // items
                    for(String attr : attrs.getResult()) {
                        LOG.info("item : " + attr);
                    }
                }
            }
        }
*/      
        
        List<SliversMonitorResults<byte[]>> bitmaps = monitor.getLocalCachedBlockInfo(path);
        for(SliversMonitorResults<byte[]> bitmap : bitmaps) {
            LOG.info(bitmap.getHostname() + " - " + path);
            byte[] map = bitmap.getResult();
        }
        return 0;
    }
}