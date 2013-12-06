package edu.arizona.cs.hsynth.hadoop.example;

import edu.arizona.cs.hsynth.fs.HSynthFSPath;
import edu.arizona.cs.hsynth.fs.HSynthFSPathFilter;

public class FastaPathFilter implements HSynthFSPathFilter {

    @Override
    public boolean accept(HSynthFSPath path) {
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
