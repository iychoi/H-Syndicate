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
package hsyndicate.fs;

import com.google.common.primitives.UnsignedLong;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class SyndicateLocalBlockCache {
    private static final Log LOG = LogFactory.getLog(SyndicateLocalBlockCache.class);
    
    public static final String LOCAL_BLOCK_CACHE_FILENAME_PATTERN_STRING = "(-?\\d+)\\.(-?\\d+)";
    public Pattern filenamePattern = Pattern.compile(LOCAL_BLOCK_CACHE_FILENAME_PATTERN_STRING);
    
    public class SyndicateLocalBlockCacheFileFilter implements FilenameFilter {
        @Override
        public boolean accept(File file, String filename) {
            Matcher matcher = filenamePattern.matcher(filename);
            return matcher.matches();
        }
    }
    
    public UnsignedLong getBlockID(String filename) throws IOException {
        Matcher matcher = filenamePattern.matcher(filename);
        if(matcher.matches()) {
            String blockId = matcher.group(1);
            return UnsignedLong.valueOf(blockId);
        } else {
            throw new IOException("Unable to parse BlockID from " + filename);
        }
    }
    
    public long getBlockVersion(String filename) throws IOException {
        Matcher matcher = filenamePattern.matcher(filename);
        if(matcher.matches()) {
            String blockVer = matcher.group(2);
            return Long.parseLong(blockVer);
        } else {
            throw new IOException("Unable to parse BlockID from " + filename);
        }
    }
    
    public FilenameFilter getFilenameFilter() {
        return new SyndicateLocalBlockCacheFileFilter();
    }
    
    public boolean isValidFileName(String filename) {
        Matcher matcher = filenamePattern.matcher(filename);
        return matcher.matches();
    }
}
