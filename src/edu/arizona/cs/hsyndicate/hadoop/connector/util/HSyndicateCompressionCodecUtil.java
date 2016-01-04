package edu.arizona.cs.hsyndicate.hadoop.connector.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

public class HSyndicateCompressionCodecUtil {
    public static CompressionCodec getCompressionCodec(Configuration conf, edu.arizona.cs.hsyndicate.fs.SyndicateFSPath path) {
        // caution : file variable contains fake path 
        Path file = new Path(path.getPath());
        CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(file);
        return codec;
    }
    
    public static CompressionCodec getCompressionCodec(CompressionCodecFactory factory, edu.arizona.cs.hsyndicate.fs.SyndicateFSPath path) {
        // caution : file variable contains fake path 
        Path file = new Path(path.getPath());
        CompressionCodec codec = factory.getCodec(file);
        return codec;
    }
}
