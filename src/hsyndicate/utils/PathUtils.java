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
package hsyndicate.utils;

import hsyndicate.fs.SyndicateFSPath;

public class PathUtils {
    
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
