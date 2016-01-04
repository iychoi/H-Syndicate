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

import com.sun.jersey.api.client.ClientResponse;
import edu.arizona.cs.hsyndicate.fs.client.SyndicateUGRestClient;
import edu.arizona.cs.hsyndicate.fs.datatypes.DirectoryEntries;
import edu.arizona.cs.hsyndicate.fs.datatypes.FileInfo;
import edu.arizona.cs.hsyndicate.fs.datatypes.Stat;
import edu.arizona.cs.hsyndicate.fs.datatypes.Xattr;
import edu.arizona.cs.hsyndicate.fs.datatypes.XattrKeyList;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SyndicateFileSystem extends AHSyndicateFileSystemBase {

    private static final Log LOG = LogFactory.getLog(SyndicateFileSystem.class);

    private static final int DEFAULT_FILESTATUS_TIMETOLIVE = 60000; // 60 sec
    
    private SyndicateUGRestClient client;
    
    private List<SyndicateFSInputStream> openInputStream = new ArrayList<SyndicateFSInputStream>();
    private List<SyndicateFSOutputStream> openOutputStream = new ArrayList<SyndicateFSOutputStream>();
    
    private Map<SyndicateFSPath, SyndicateFSFileStatus> filestatus_cache = new PassiveExpiringMap<SyndicateFSPath, SyndicateFSFileStatus>(DEFAULT_FILESTATUS_TIMETOLIVE);
    
    public SyndicateFileSystem(SyndicateFSConfiguration configuration) throws InstantiationException {
        initialize(configuration);
    }
    
    @Override
    protected void initialize(SyndicateFSConfiguration conf) throws InstantiationException {
        if(conf == null) {
            LOG.error("FileSystem Initialize failed : configuration is null");
            throw new IllegalArgumentException("Can not initialize the filesystem from null configuration");
        }
        
        super.raiseOnBeforeCreateEvent();

        super.initialize(conf);
        
        this.client = new SyndicateUGRestClient(conf.getHost(), conf.getPort());

        super.raiseOnAfterCreateEvent();
    }
    
    public SyndicateUGRestClient getUGRestClient() {
        return this.client;
    }
    
    private SyndicateFSFileStatus getFileStatus(SyndicateFSPath abspath) {
        if(abspath == null) {
            LOG.error("Can not get FileStatus from null abspath");
            throw new IllegalArgumentException("Can not get FileStatus from null abspath");
        }
        
        // check memory cache
        SyndicateFSFileStatus cached_status = this.filestatus_cache.get(abspath);
        
        if(cached_status != null && !cached_status.isDirty()) {
            return cached_status;
        }
        
        /*
        // check parent dir's FileStatus recursively
        if(abspath.getParent() != null) {
            SyndicateFSFileStatus parentStatus = getFileStatus(abspath.getParent());
            if(parentStatus == null) {
                LOG.error("parentStatus is null");
                return null;
            } 
            
            if(!parentStatus.isDirectory()) {
                LOG.error("parentStatus is not a directory");
                return null;
            }
        }
        */
        
        // not in memory cache
        Stat stat = null;
        try {
            Future<ClientResponse> statFuture = this.client.getStat(abspath.getPath());
            if(statFuture != null) {
                stat = this.client.processGetStat(statFuture);
            } else {
                return null;
            }
        } catch (IOException ex) {
            LOG.error(ex);
            return null;
        }
        
        SyndicateFSFileStatus status = new SyndicateFSFileStatus(this, abspath, stat);
        this.filestatus_cache.put(abspath, status);
        
        return status;
    }
    
    private SyndicateFSFileHandle getFileHandle(SyndicateFSFileStatus status, boolean readonly) throws IOException {
        if(status == null) {
            LOG.error("Can not get FileHandle from null status");
            throw new IllegalArgumentException("Can not get FileHandle from null status");
        }
        if(status.isDirty()) {
            LOG.error("Can not get FileHandle from dirty status");
            throw new IllegalArgumentException("Can not get FileHandle from dirty status");
        }
        
        FileInfo fi = null;
        Future<ClientResponse> openFuture = null;
        if(readonly) {
            openFuture = this.client.open(status.getPath().getPath(), "r", 0x666);
        } else {
            openFuture = this.client.open(status.getPath().getPath(), "w", 0x666);
        }
        
        if(openFuture != null) {
            fi = this.client.processOpen(openFuture);
            return new SyndicateFSFileHandle(this, status, fi, readonly);
        } else {
            LOG.error("Can not get file handle from status");
            throw new IOException("Can not get file handle from status");
        }
    }
    
    private SyndicateFSFileHandle createNewFile(SyndicateFSPath abspath) throws IOException {
        if(abspath == null) {
            LOG.error("abspath is null");
            throw new IllegalArgumentException("abspath is null");
        }
        
        if(abspath.getParent() != null) {
            SyndicateFSFileStatus parent = getFileStatus(abspath.getParent());
            if(parent == null) {
                LOG.error("Parent directory does not exist");
                throw new IOException("Parent directory does not exist");
            }
            
            if(!parent.isDirectory()) {
                LOG.error("Parent directory does not exist");
                throw new IOException("Parent directory does not exist");
            }
        }
        
        Future<ClientResponse> openFuture = this.client.open(abspath.getPath(), "w", 0x666);
        if(openFuture != null) {
            FileInfo fi = this.client.processOpen(openFuture);
            Future<ClientResponse> closeFuture = this.client.close(abspath.getPath(), fi);
            if(closeFuture != null) {
                this.client.processClose(closeFuture);
            } else {
                LOG.error("Can not close file - " + abspath.getPath());
                throw new IOException("Can not close file - " + abspath.getPath());
            }
        } else {
            LOG.error("Can not open file - " + abspath.getPath());
            throw new IOException("Can not open file - " + abspath.getPath());
        }
        
        SyndicateFSFileStatus status = getFileStatus(abspath);
        return getFileHandle(status, false);
    }
    
    @Override
    public boolean exists(SyndicateFSPath path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = getFileStatus(absPath);
        if(status != null) {
            return true;
        }
        return false;
    }

    @Override
    public boolean isDirectory(SyndicateFSPath path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = getFileStatus(absPath);
        if(status != null) {
            return status.isDirectory();
        }
        return false;
    }

    @Override
    public boolean isFile(SyndicateFSPath path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = getFileStatus(absPath);
        if(status != null) {
            return status.isFile();
        }
        return false;
    }

    @Override
    public long getSize(SyndicateFSPath path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = getFileStatus(absPath);
        if(status != null) {
            return status.getSize();
        }
        return 0;
    }
    
    @Override
    public long getBlockSize() {
        SyndicateFSFileStatus status = getFileStatus(getRootPath());
        if(status != null) {
            return status.getBlockSize();
        }
        return 0;
    }
    
    @Override
    public String[] listExtendedAttrs(SyndicateFSPath path) throws IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = getFileStatus(absPath);
        if(status == null) {
            LOG.error("file not exist");
            throw new IOException("file not exist : " + path.getPath());
        }
        
        Future<ClientResponse> listXattrFuture = this.client.listXattr(absPath.getPath());
        if(listXattrFuture != null) {
            XattrKeyList processListXattr = this.client.processListXattr(listXattrFuture);
            return processListXattr.getKeys().toArray(new String[0]);
        } else {
            LOG.error("cannot list extended attributes - " + absPath.getPath());
            throw new IOException("cannot list extended attributes - " + absPath.getPath());
        }
    }

    @Override
    public String getExtendedAttr(SyndicateFSPath path, String name) throws IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = getFileStatus(absPath);
        if(status == null) {
            LOG.error("file not exist");
            throw new IOException("file not exist : " + path.getPath());
        }
        
        Future<ClientResponse> getXattrFuture = this.client.getXattr(absPath.getPath(), name);
        if(getXattrFuture != null) {
            Xattr processGetXattr = this.client.processGetXattr(getXattrFuture);
            return processGetXattr.getValue();
        } else {
            LOG.error("cannot get extended attributes - " + absPath.getPath());
            throw new IOException("cannot get extended attributes - " + absPath.getPath());
        }
    }

    @Override
    public boolean delete(SyndicateFSPath path) throws FileNotFoundException, IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = getFileStatus(absPath);
        if(status == null) {
            LOG.error("file not exist");
            throw new FileNotFoundException("file not exist : " + path.getPath());
        }
        
        if(status.isFile()) {
            Future<ClientResponse> unlinkFuture = this.client.unlink(absPath.getPath());
            if(unlinkFuture != null) {
                this.client.processUnlink(unlinkFuture);
            } else {
                LOG.error("cannot delete file - " + absPath.getPath());
                throw new IOException("cannot delete file - " + absPath.getPath());
            }
        } else if(status.isDirectory()) {
            Future<ClientResponse> removeDirFuture = this.client.removeDir(absPath.getPath());
            if(removeDirFuture != null) {
                this.client.processRemoveDir(removeDirFuture);
            } else {
                LOG.error("cannot delete directory - " + absPath.getPath());
                throw new IOException("cannot delete directory - " + absPath.getPath());
            }
        } else {
            LOG.error("Can not delete from unknown status");
            throw new IOException("Can not delete from unknown status");
        }
        
        this.filestatus_cache.remove(absPath);
        return true;
    }

    @Override
    public void rename(SyndicateFSPath path, SyndicateFSPath newpath) throws FileNotFoundException, IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        if(newpath == null) {
            LOG.error("newpath is null");
            throw new IllegalArgumentException("newpath is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        SyndicateFSPath absNewPath = getAbsolutePath(newpath);
        
        SyndicateFSFileStatus status = getFileStatus(absPath);
        SyndicateFSFileStatus newStatus = getFileStatus(absNewPath);
        SyndicateFSFileStatus newStatusParent = getFileStatus(absNewPath.getParent());
        
        if(status == null) {
            LOG.error("source file does not exist : " + path.getPath());
            throw new FileNotFoundException("source file does not exist : " + path.getPath());
        }
        
        if(newStatus != null) {
            LOG.error("target file already exists : " + newpath.getPath());
            throw new IOException("target file already exists : " + newpath.getPath());
        }
        
        if(newStatusParent == null) {
            LOG.error("parent directory of target file does not exist : " + newpath.getPath());
            throw new IOException("parent directory of target file does not exist : " + newpath.getPath());
        }
        
        Future<ClientResponse> renameFuture = this.client.rename(absPath.getPath(), absNewPath.getPath());
        if(renameFuture != null) {
            this.client.processRename(renameFuture);
        } else {
            LOG.error("cannot rename file - " + absPath.getPath());
            throw new IOException("cannot rename file - " + absPath.getPath());
        }
        
        this.filestatus_cache.remove(absPath);
    }

    @Override
    public void mkdir(SyndicateFSPath path) throws IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = getFileStatus(absPath.getParent());
        if(status == null) {
            LOG.error("parent directory does not exist : " + absPath.getPath());
            throw new IOException("parent directory does not exist : " + absPath.getPath());
        }
        
        Future<ClientResponse> makeDirFuture = this.client.makeDir(absPath.getPath(), 0x700);
        if(makeDirFuture != null) {
            this.client.processMakeDir(makeDirFuture);
        } else {
            LOG.error("cannot make a directory - " + absPath.getPath());
            throw new IOException("cannot make a directory - " + absPath.getPath());
        }
    }
    
    @Override
    public void truncate(SyndicateFSPath path, long offset) throws FileNotFoundException, IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = getFileStatus(absPath);
        if(status == null) {
            LOG.error("cannot find the file to truncate : " + absPath.getPath());
            throw new FileNotFoundException("cannot find the file to truncate : " + absPath.getPath());
        }
        
        Future<ClientResponse> truncateFuture = this.client.truncate(absPath.getPath(), offset);
        if(truncateFuture != null) {
            this.client.processTruncate(truncateFuture);
        } else {
            LOG.error("cannot truncate a file - " + absPath.getPath());
            throw new IOException("cannot truncate a file - " + absPath.getPath());
        }
        
        this.filestatus_cache.remove(absPath);
    }

    @Override
    public SyndicateFSInputStream getFileInputStream(SyndicateFSPath path) throws FileNotFoundException, IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = getFileStatus(absPath);
        if(status == null) {
            LOG.error("Can not open the file to read : " + absPath.getPath());
            throw new FileNotFoundException("Can not open the file to read : " + absPath.getPath());
        }
        
        SyndicateFSFileHandle handle = getFileHandle(status, true);
        if(handle == null) {
            LOG.error("Can not open the file to read : " + absPath.getPath());
            throw new IOException("Can not open the file to read : " + absPath.getPath());
        }
        
        SyndicateFSInputStream is = new SyndicateFSInputStream(handle);
        this.openInputStream.add(is);
        return is;
    }

    @Override
    public SyndicateFSOutputStream getFileOutputStream(SyndicateFSPath path) throws IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = getFileStatus(absPath);
        
        if(status != null) {
            if(!status.isFile()) {
                LOG.error("Can not open the file to write (is directory) : " + absPath.getPath());
                throw new IOException("Can not open the file to write (is directory) : " + absPath.getPath());
            }
            
            truncate(absPath, 0);
            
            SyndicateFSFileHandle handle = getFileHandle(status, false);
            if(handle == null) {
                LOG.error("Can not open the file to write : " + absPath.getPath());
                throw new IOException("Can not open the file to write : " + absPath.getPath());
            }
            
            SyndicateFSOutputStream os = new SyndicateFSOutputStream(handle);
            this.openOutputStream.add(os);
            return os;
        } else {
            // create new file
            SyndicateFSFileHandle handle = createNewFile(absPath);
            if(handle == null) {
                LOG.error("Can not create a file to write : " + absPath.getPath());
                throw new IOException("Can not create a file to write : " + absPath.getPath());
            }

            SyndicateFSOutputStream os = new SyndicateFSOutputStream(handle);
            this.openOutputStream.add(os);
            return os;
        }
    }

    @Override
    public String[] readDirectoryEntries(SyndicateFSPath path, ISyndicateFSFilenameFilter filter) throws FileNotFoundException, IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = getFileStatus(absPath);
        if(status == null) {
            LOG.error("directory does not exist : " + absPath.getPath());
            throw new FileNotFoundException("directory does not exist : " + absPath.getPath());
        }
        
        List<String> entries = null;
        Future<ClientResponse> readDirFuture = this.client.readDir(absPath.getPath());
        if(readDirFuture != null) {
            DirectoryEntries processReadDir = this.client.processReadDir(readDirFuture);
            entries = processReadDir.getEntries();
        } else {
            LOG.error("failed to read directory - " + absPath.getPath());
            throw new IOException("failed to read directory - " + absPath.getPath());
        }
        
        if(filter == null) {
            return entries.toArray(new String[0]);
        } else {
            List<String> arr = new ArrayList<String>();
            for(String entry : entries) {
                if(filter.accept(absPath, entry)) {
                    arr.add(entry);
                }
            }
            
            String[] entries_filtered = arr.toArray(new String[0]);
            return entries_filtered;
        }
    }

    @Override
    public synchronized String[] readDirectoryEntries(SyndicateFSPath path, ISyndicateFSPathFilter filter) throws FileNotFoundException, IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = getFileStatus(absPath);
        if(status == null) {
            LOG.error("directory does not exist : " + absPath.getPath());
            throw new FileNotFoundException("directory does not exist : " + absPath.getPath());
        }
        
        List<String> entries = null;
        Future<ClientResponse> readDirFuture = this.client.readDir(absPath.getPath());
        if(readDirFuture != null) {
            DirectoryEntries processReadDir = this.client.processReadDir(readDirFuture);
            entries = processReadDir.getEntries();
        } else {
            LOG.error("failed to read directory - " + absPath.getPath());
            throw new IOException("failed to read directory - " + absPath.getPath());
        }
        
        if(filter == null) {
            return entries.toArray(new String[0]);
        } else {
            List<String> arr = new ArrayList<String>();
            for(String entry : entries) {
                if(filter.accept(new SyndicateFSPath(absPath, entry))) {
                    arr.add(entry);
                }
            }
            
            String[] entries_filtered = arr.toArray(new String[0]);
            return entries_filtered;
        }
    }
    
    synchronized void notifyInputStreamClosed(SyndicateFSInputStream inputStream) {
        if(inputStream == null) {
            LOG.error("inputStream is null");
            throw new IllegalArgumentException("inputStream is null");
        }
        
        this.openInputStream.remove(inputStream);
    }
    
    synchronized void notifyOutputStreamClosed(SyndicateFSOutputStream outputStream) {
        if(outputStream == null) {
            LOG.error("outputStream is null");
            throw new IllegalArgumentException("outputStream is null");
        }
        
        this.openOutputStream.remove(outputStream);
    }
    
    @Override
    public synchronized void close() throws IOException {
        if(this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        // call handler
        super.raiseOnBeforeDestroyEvent();
        
        // close all open files
        for(SyndicateFSInputStream is : this.openInputStream) {
            try {
                is.close();
            } catch (IOException ex) {
                LOG.error(ex);
            }
        }
        
        for(OutputStream os : this.openOutputStream) {
            try {
                os.close();
            } catch (IOException ex) {
                LOG.error(ex);
            }
        }
        
        this.filestatus_cache.clear();
        
        this.client.close();
        
        super.close();
        
        super.raiseOnAfterDestroyEvent();
    }
}
