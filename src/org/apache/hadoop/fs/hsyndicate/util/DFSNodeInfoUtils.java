package org.apache.hadoop.fs.hsyndicate.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;

public class DFSNodeInfoUtils {
    
    public static final Log LOG = LogFactory.getLog(DFSNodeInfoUtils.class);
    
    public static String[] getDataNodes(Configuration conf) throws IOException {
        List<String> datanodes = new ArrayList<String>();
        ClientProtocol client = DFSClient.createNamenode(conf);
        DatanodeInfo[] datanodeReport = client.getDatanodeReport(FSConstants.DatanodeReportType.LIVE);
        for(DatanodeInfo nodeinfo : datanodeReport) {
            datanodes.add(nodeinfo.getHostName().trim());
        }
        
        return datanodes.toArray(new String[0]);
    }
    
    public static String getDataNodesCommaSeparated(Configuration conf) throws IOException {
        StringBuilder sb = new StringBuilder();
        ClientProtocol client = DFSClient.createNamenode(conf);
        DatanodeInfo[] datanodeReport = client.getDatanodeReport(FSConstants.DatanodeReportType.LIVE);
        for(DatanodeInfo nodeinfo : datanodeReport) {
            if(sb.length() != 0) {
                sb.append(",");
            }
            sb.append(nodeinfo.getHostName().trim());
        }
        
        return sb.toString();
    }
}
