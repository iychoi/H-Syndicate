/*
   Copyright 2015 The Trustees of Princeton University

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BlockUtils {
    private static final Log LOG = LogFactory.getLog(BlockUtils.class);
    
    public static int getBlocks(long filesize, long blocksize) {
        int blocknum = (int) ((filesize / blocksize) + 1);
        if(filesize % blocksize == 0) {
            blocknum--;
        }
        return blocknum;
    }
    
    public static int getBlockID(long offset, long blocksize) {
        int blockid = (int) (offset / blocksize);
        if(offset != 0 && offset % blocksize == 0) {
            blockid++;
        }
        return blockid;
    }
    
    public static long getBlockStartOffset(int blockID, long blocksize) {
        return blockID * blocksize;
    }
    
    public static long getBlockLength(long filesize, long blocksize, int blockID) {
        long blockstart = getBlockStartOffset(blockID, blocksize);
        
        if(blockstart + blocksize  <= filesize) {
            return blocksize;
        } else {
            return filesize - blockstart;
        }
    }
    
    public static boolean checkBlockPresence(int blockID, byte[] bitmap) {
        if(bitmap[blockID] == 1) {
            return true;
        }
        return false;
    }
}
