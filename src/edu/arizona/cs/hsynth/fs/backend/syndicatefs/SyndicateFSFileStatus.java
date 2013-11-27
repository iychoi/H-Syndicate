package edu.arizona.cs.hsynth.fs.backend.syndicatefs;

import edu.arizona.cs.hsynth.fs.Path;
import edu.arizona.cs.hsynth.fs.backend.syndicatefs.client.message.SyndicateFSStat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SyndicateFSFileStatus {

    private static final Log LOG = LogFactory.getLog(SyndicateFSFileStatus.class);
    
    private SyndicateFSFileSystem filesystem;
    private Path path;
    private SyndicateFSStat stat;
    private boolean dirty;
    private boolean sizeModified;
    private long localFileSize;

    public SyndicateFSFileStatus(SyndicateFSFileSystem fs, Path path, SyndicateFSStat stat) {
        this.filesystem = fs;
        this.path = path;
        this.stat = stat;
        this.dirty = false;
        this.sizeModified = false;
        this.localFileSize = 0;
    }
    
    public SyndicateFSFileSystem getFileSystem() {
        return this.filesystem;
    }
    
    public Path getPath() {
        return this.path;
    }
    
    public boolean isDirectory() {
        if((this.stat.getMode() & SyndicateFSStat.S_IFDIR) == SyndicateFSStat.S_IFDIR)
            return true;
        return false;
    }

    public boolean isFile() {
        if((this.stat.getMode() & SyndicateFSStat.S_IFREG) == SyndicateFSStat.S_IFREG)
            return true;
        return false;
    }

    public long getSize() {
        if(this.sizeModified)
            return this.localFileSize;
        else
            return this.stat.getSize();
    }

    public long getBlockSize() {
        return this.stat.getBlksize();
    }

    public long getBlocks() {
        if(this.sizeModified) {
            long blockSize = this.stat.getBlocks();
            long blocks = this.localFileSize / blockSize;
            if(this.localFileSize % blockSize != 0)
                blocks++;
            return blocks;
        } else {
            return this.stat.getBlocks();
        }
    }

    public long getLastAccess() {
        return this.stat.getAtim();
    }

    public long getLastModification() {
        return this.stat.getMtim();
    }

    public boolean isDirty() {
        return this.dirty;
    }

    public void setDirty() {
        this.dirty = true;
    }

    void notifySizeChanged(long size) {
        this.localFileSize = size;
        this.sizeModified = true;
    }
}
