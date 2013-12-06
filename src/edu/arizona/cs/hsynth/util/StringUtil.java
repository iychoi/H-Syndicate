package edu.arizona.cs.hsynth.util;

import edu.arizona.cs.hsynth.fs.HSynthFSPath;

public class StringUtil {
    
    public static HSynthFSPath stringToPath(String str) {
        if(str == null) {
            return null;
        }
        
        return new HSynthFSPath(str);
    }
    
    public static HSynthFSPath[] stringToPath(String[] str) {
        if (str == null) {
            return null;
        }
        
        HSynthFSPath[] path = new HSynthFSPath[str.length];
        for (int i=0;i<str.length;i++) {
            path[i] = new HSynthFSPath(str[i]);
        }
        return path;
    }
}
