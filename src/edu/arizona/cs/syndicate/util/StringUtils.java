package edu.arizona.cs.syndicate.util;

import edu.arizona.cs.syndicate.fs.SyndicateFSPath;

public class StringUtils {
    
    public static SyndicateFSPath stringToPath(String str) {
        if(str == null) {
            return null;
        }
        
        return new SyndicateFSPath(str);
    }
    
    public static SyndicateFSPath[] stringToPath(String[] str) {
        if (str == null) {
            return null;
        }
        
        SyndicateFSPath[] path = new SyndicateFSPath[str.length];
        for (int i=0;i<str.length;i++) {
            path[i] = new SyndicateFSPath(str[i]);
        }
        return path;
    }
    
    public static String makeCommaSeparated(String[] str) {
        if (str == null) {
            return null;
        }
        
        StringBuilder sb = new StringBuilder();
        for(String s : str) {
            if(sb.length() != 0) {
                sb.append(",");
            }
            sb.append(s);
        }
        
        return sb.toString();
    }
}
