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
package hsyndicate.fs;

import hsyndicate.rest.datatypes.StatRaw;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SyndicateFSFileStatus {

    private static final Log LOG = LogFactory.getLog(SyndicateFSFileStatus.class);
    
    private SyndicateFileSystem filesystem;
    private SyndicateFSPath path;
    private StatRaw statRaw;
    private boolean dirty;
    private boolean sizeModified;
    private long localFileSize;

    public SyndicateFSFileStatus(SyndicateFileSystem fs, SyndicateFSPath path) {
        this.filesystem = fs;
        this.path = path;
        this.statRaw = null;
        this.dirty = true;
        this.sizeModified = false;
        this.localFileSize = 0;
    }
    
    public SyndicateFSFileStatus(SyndicateFileSystem fs, SyndicateFSPath path, StatRaw statRaw) {
        this.filesystem = fs;
        this.path = path;
        this.statRaw = statRaw;
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
        if(this.statRaw == null) {
            return false;
        }
        return this.statRaw.isDirectory();
    }

    public synchronized boolean isFile() {
        if(this.statRaw == null) {
            return true;
        }
        return this.statRaw.isFile();
    }

    public synchronized long getSize() {
        if(this.sizeModified) {
            return this.localFileSize;
        } else {
            if(this.statRaw == null) {
                return 0;
            }
            return this.statRaw.getSize();
        }
    }

    public synchronized long getBlockSize() {
        return this.filesystem.getBlockSize();
    }

    public synchronized long getBlocks() {
        long blockSize = getBlockSize();
        long size = getSize();
        long blocks = size / blockSize;
        if(size % blockSize != 0) {
            blocks++;
        }
        
        return blocks;
    }

    public synchronized long getLastAccess() {
        if(this.statRaw == null) {
            return 0;
        }
        return this.statRaw.getMtime();
    }

    public synchronized long getLastModification() {
        if(this.statRaw == null) {
            return 0;
        }
        return this.statRaw.getMtime();
    }
    
    public synchronized int getMode() {
        if(this.statRaw == null) {
            return 0;
        }
        return this.statRaw.getMode();
    }
    
    public synchronized int getUserMode() {
        if(this.statRaw == null) {
            return 0;
        }
        return this.statRaw.getUserMode();
    }
    
    public synchronized int getGroupMode() {
        if(this.statRaw == null) {
            return 0;
        }
        return this.statRaw.getGroupMode();
    }
    
    public synchronized int getOthersMode() {
        if(this.statRaw == null) {
            return 0;
        }
        return this.statRaw.getOthersMode();
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
