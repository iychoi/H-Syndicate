package edu.arizona.cs.syndicate.fs;

import edu.arizona.cs.syndicate.fs.client.Stat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SyndicateFSFileStatus {

    private static final Log LOG = LogFactory.getLog(SyndicateFSFileStatus.class);
    
    private SyndicateFileSystem filesystem;
    private SyndicateFSPath path;
    private Stat stat;
    private boolean dirty;
    private boolean sizeModified;
    private long localFileSize;

    public SyndicateFSFileStatus(SyndicateFileSystem fs, SyndicateFSPath path, Stat stat) {
        this.filesystem = fs;
        this.path = path;
        this.stat = stat;
        this.dirty = false;
        this.sizeModified = false;
        this.localFileSize = 0;
    }
    
    public synchronized SyndicateFileSystem getFileSystem() {
        return this.filesystem;
    }
    
    public synchronized SyndicateFSPath getPath() {
        return this.path;
    }
    
    public synchronized boolean isDirectory() {
        if((this.stat.getMode() & Stat.S_IFDIR) == Stat.S_IFDIR)
            return true;
        return false;
    }

    public synchronized boolean isFile() {
        if((this.stat.getMode() & Stat.S_IFREG) == Stat.S_IFREG)
            return true;
        return false;
    }

    public synchronized long getSize() {
        if(this.sizeModified)
            return this.localFileSize;
        else
            return this.stat.getSize();
    }

    public synchronized long getBlockSize() {
        return this.stat.getBlksize();
    }

    public synchronized long getBlocks() {
        if(this.sizeModified) {
            long blockSize = this.stat.getBlocks();
            long blocks = this.localFileSize / blockSize;
            if(this.localFileSize % blockSize != 0) {
                blocks++;
            }
            return blocks;
        } else {
            return this.stat.getBlocks();
        }
    }

    public synchronized long getLastAccess() {
        return this.stat.getAtim();
    }

    public synchronized long getLastModification() {
        return this.stat.getMtim();
    }

    public synchronized boolean isDirty() {
        return this.dirty;
    }

    public synchronized void setDirty() {
        this.dirty = true;
    }

    synchronized void notifySizeChanged(long size) {
        this.localFileSize = size;
        this.sizeModified = true;
    }
}
