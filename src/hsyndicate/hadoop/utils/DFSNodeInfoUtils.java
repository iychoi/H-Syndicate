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
package hsyndicate.hadoop.utils;

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
