package edu.arizona.cs.hsynth.hadoop.example.filesystem;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class FastaPathFilter implements PathFilter {

    @Override
    public boolean accept(Path path) {
        if(path.getName().toLowerCase().endsWith(".fa.gz")) {
            return true;
        } else if(path.getName().toLowerCase().endsWith(".fa")) {
            return true;
        } else if(path.getName().toLowerCase().endsWith(".ffn.gz")) {
            return true;
        } else if(path.getName().toLowerCase().endsWith(".ffn")) {
            return true;
        }
        return false;
    }
    
}
