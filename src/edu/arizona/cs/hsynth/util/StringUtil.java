package edu.arizona.cs.hsynth.util;

import edu.arizona.cs.hsynth.fs.Path;
import java.util.ArrayList;
import java.util.List;

public class StringUtil {
    
    public static char MULTI_FILEPATH_SEPARATOR_CH = ',';

    public static String generatePathString(Path... paths) {
        StringBuilder sb = new StringBuilder();
        
        if(paths.length > 0) {
            sb.append(paths[0].getPath());
        }
        
        for(int i=1;i<paths.length;i++) {
            sb.append(MULTI_FILEPATH_SEPARATOR_CH);
            sb.append(paths[i].getPath());
        }
        
        return sb.toString();
    }
    
    public static String addPathString(String dirs, Path path) {
        if(dirs == null || dirs.isEmpty()) {
            return path.getPath();
        } else {
            return dirs + MULTI_FILEPATH_SEPARATOR_CH + path.getPath();
        }
    }
    
    public static Path stringToPath(String str) {
        if(str == null) {
            return null;
        }
        
        return new Path(str);
    }
    
    public static Path[] stringToPath(String[] str) {
        if (str == null) {
            return null;
        }
        
        Path[] path = new Path[str.length];
        for (int i=0;i<str.length;i++) {
            path[i] = new Path(str[i]);
        }
        return path;
    }
    
    public static String[] getPathStrings(String pathstrs) {
        int length = pathstrs.length();
        int pathStart = 0;
        List<String> pathStrings = new ArrayList<String>();
        
        if(length == 0)
            return null;

        for (int i = 0; i < length; i++) {
            char ch = pathstrs.charAt(i);
            
            if(ch == MULTI_FILEPATH_SEPARATOR_CH) {
                pathStrings.add(pathstrs.substring(pathStart, i));
                pathStart = i + 1;
            }
        }
        pathStrings.add(pathstrs.substring(pathStart, length));

        String[] resultarr = new String[pathStrings.size()];
        resultarr = pathStrings.toArray(resultarr);
        
        return resultarr;
    }
}
