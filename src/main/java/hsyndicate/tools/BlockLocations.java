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
package hsyndicate.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class BlockLocations {
    public static String makeCommaSeparated(String[] arr) {
        if(arr == null) {
            return "<null>";
        }
        
        if(arr.length == 0) {
            return "<empty>";
        }
        
        StringBuilder sb = new StringBuilder();
        for(String a : arr) {
            if(sb.length() != 0) {
                sb.append(", ");
            }
            
            if(a == null) {
                return "<null>";
            }

            if(a.isEmpty()) {
                return "<empty>";
            }
        
            sb.append(a);
        }
        return sb.toString();
    }
    
    public static void main(String[] args) throws Exception {
        Path p = new Path(args[0]);
        Configuration conf = new Configuration();
        FileSystem fs = p.getFileSystem(conf);
        FileStatus f = fs.getFileStatus(p);
        BlockLocation[] bla = fs.getFileBlockLocations(f, 0, f.getLen());
        
        System.out.println("File : " + f.getPath().toString());
        for(BlockLocation bl : bla) {
            System.out.println("Offset : " + bl.getOffset());
            System.out.println("Len : " + bl.getLength());
            System.out.println("Hosts : " + makeCommaSeparated(bl.getHosts()));
            System.out.println("Names : " + makeCommaSeparated(bl.getNames()));
            System.out.println("TopologyPaths : " + makeCommaSeparated(bl.getTopologyPaths()));
        }
    }
}
