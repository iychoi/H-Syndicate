/*
 * Copyright 2015 iychoi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.arizona.cs.hsyndicate.fs;

import edu.arizona.cs.hsyndicate.fs.datatypes.Stat;
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
        return this.stat.getAtime();
    }

    public synchronized long getLastModification() {
        return this.stat.getMtime();
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
