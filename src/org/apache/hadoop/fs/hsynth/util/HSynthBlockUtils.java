package org.apache.hadoop.fs.hsynth.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HSynthBlockUtils {
    private static final Log LOG = LogFactory.getLog(HSynthBlockUtils.class);
    
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
