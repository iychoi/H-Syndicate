package edu.arizona.cs.hsynth.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

public class HSynthCompressionCodecUtil {
    public static CompressionCodec getCompressionCodec(Configuration conf, edu.arizona.cs.hsynth.fs.HSynthFSPath path) {
        // caution : file variable contains fake path 
        Path file = new Path(path.getPath());
        CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(file);
        return codec;
    }
    
    public static CompressionCodec getCompressionCodec(CompressionCodecFactory factory, edu.arizona.cs.hsynth.fs.HSynthFSPath path) {
        // caution : file variable contains fake path 
        Path file = new Path(path.getPath());
        CompressionCodec codec = factory.getCodec(file);
        return codec;
    }
}
