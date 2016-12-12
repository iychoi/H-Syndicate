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
package hsyndicate.hadoop.connector.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

public class HSyndicateCompressionCodecUtil {
    public static CompressionCodec getCompressionCodec(Configuration conf, hsyndicate.fs.SyndicateFSPath path) {
        // caution : file variable contains fake path 
        Path file = new Path(path.getPath());
        CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(file);
        return codec;
    }
    
    public static CompressionCodec getCompressionCodec(CompressionCodecFactory factory, hsyndicate.fs.SyndicateFSPath path) {
        // caution : file variable contains fake path 
        Path file = new Path(path.getPath());
        CompressionCodec codec = factory.getCodec(file);
        return codec;
    }
}
