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
import com.sun.jersey.api.client.ClientResponse;
import hsyndicate.hadoop.utils.HSyndicateConfigUtils;
import hsyndicate.rest.client.SyndicateUGHttpClient;
import hsyndicate.rest.common.RestfulException;
import hsyndicate.rest.datatypes.DirectoryEntries;
import hsyndicate.rest.datatypes.FileDescriptor;
import hsyndicate.rest.datatypes.StatRaw;
import hsyndicate.rest.datatypes.Statvfs;
import hsyndicate.rest.datatypes.Xattr;
import hsyndicate.rest.datatypes.XattrKeyList;
import hsyndicate.utils.BlockUtils;
import hsyndicate.utils.DateTimeUtils;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

public class SyndicateFileSystem extends AHSyndicateFileSystemBase {

    private static final Log LOG = LogFactory.getLog(SyndicateFileSystem.class);

    private static final int DEFAULT_FILESTATUS_TIMETOLIVE = 60000; // 60 sec
    private static final int DEFAULT_STATVFS_TIMETOLIVE = 60000; // 60 sec
    private static final int DEFAULT_UGRESTCLIENT_TIMETOLIVE = 600000; // 600 sec
    private static final long DEFAULT_BLOCK_SIZE = 1024*1024; // 1MB
    private static final String LOCAL_CACHED_BLOCKS_XATTR_NAME = "user.syndicate_cached_blocks";
    private static final String LOCAL_CACHED_FILE_PATH_XATTR_NAME = "user.syndicate_cached_file_path";
    
    private Map<String, SyndicateUGHttpClient> ugRestClients = new PassiveExpiringMap<String, SyndicateUGHttpClient>(DEFAULT_UGRESTCLIENT_TIMETOLIVE);
    
    private List<SyndicateFSInputStream> openInputStream = new ArrayList<SyndicateFSInputStream>();
    private List<SyndicateFSOutputStream> openOutputStream = new ArrayList<SyndicateFSOutputStream>();
    
    private Map<SyndicateFSPath, SyndicateFSFileStatus> fileStatusCache = new PassiveExpiringMap<SyndicateFSPath, SyndicateFSFileStatus>(DEFAULT_FILESTATUS_TIMETOLIVE);
    private Map<SyndicateFSPath, Statvfs> statVfsCache = new PassiveExpiringMap<SyndicateFSPath, Statvfs>(DEFAULT_STATVFS_TIMETOLIVE);
    
    public SyndicateFileSystem(SyndicateFSConfiguration syndicateFsConf, Configuration hadoopConf) throws InstantiationException {
        initialize(syndicateFsConf, hadoopConf);
    }
    
    @Override
    protected void initialize(SyndicateFSConfiguration syndicateFsConf, Configuration hadoopConf) throws InstantiationException {
        if(syndicateFsConf == null) {
            LOG.error("FileSystem Initialize failed : syndicateFsConf is null");
            throw new IllegalArgumentException("Can not initialize the filesystem from null configuration");
        }
        
        if(hadoopConf == null) {
            LOG.error("FileSystem Initialize failed : hadoopConf is null");
            throw new IllegalArgumentException("Can not initialize the filesystem from null configuration");
        }
        
        super.raiseOnBeforeCreateEvent();

        super.initialize(syndicateFsConf, hadoopConf);
        
        super.raiseOnAfterCreateEvent();
    }
    
    public synchronized SyndicateUGHttpClient getUGRestClient(String sessionName) throws InstantiationException {
        if(sessionName == null || sessionName.isEmpty()) {
            LOG.error("Can not get UG Rest Client from null session name");
            throw new IllegalArgumentException("Can not get UG Rest Client from null session name");
        }
        
        SyndicateUGHttpClient client = this.ugRestClients.get(sessionName);
        
        if(client == null) {
            String sessionKey = HSyndicateConfigUtils.getSyndicateUGSessionKey(this.hadoopConf, sessionName);
            client = new SyndicateUGHttpClient(this.syndicateFsConf.getHost(), this.syndicateFsConf.getPort(), sessionName, sessionKey);
            
            this.ugRestClients.put(sessionName, client);
        }
        return client;
    }
    
    private StatRaw makeRootStat() {
        StatRaw rootStat = new StatRaw();
        rootStat.setType(2); // dir
        rootStat.setSize(4096); // root dir size
        rootStat.setMtimeSec(DateTimeUtils.getCurrentTime() / 1000);
        rootStat.setMode((4 << 6 | 4 << 3 | 4)); // r--r--r--
        return rootStat;
    }
    
    private SyndicateFSFileStatus getFileStatus(SyndicateFSPath absPath) throws IOException {
        if(absPath == null) {
            LOG.error("Can not get FileStatus from null abspath");
            throw new IllegalArgumentException("Can not get FileStatus from null abspath");
        }
        
        // check memory cache
        SyndicateFSFileStatus cachedStatus = this.fileStatusCache.get(absPath);
        if(cachedStatus != null && !cachedStatus.isDirty()) {
            return cachedStatus;
        }
        
        // not in memory cache
        StatRaw statRaw = null;
        
        if(absPath.getParent() == null) {
            // root
            statRaw = makeRootStat();
        } else {
            try {
                SyndicateUGHttpClient client = getUGRestClient(absPath.getSessionName());
                Future<ClientResponse> statFuture = client.getStat(absPath.getPath());
                if(statFuture != null) {
                    statRaw = client.processGetStat(statFuture);
                } else {
                    throw new IOException("Can not process REST operations");
                }
            } catch (FileNotFoundException ex) {
                // silent
                return null;
            } catch (Exception ex) {
                LOG.error("exception occurred", ex);
                throw new IOException(ex);
            }
        }
        
        SyndicateFSFileStatus status = new SyndicateFSFileStatus(this, absPath, statRaw);
        this.fileStatusCache.put(absPath, status);
        
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
        
        LOG.info("opening a file - " + status.getPath().getPath());
        
        try {
            SyndicateUGHttpClient client = getUGRestClient(status.getPath().getSessionName());
        
            FileDescriptor fi = null;
            Future<ClientResponse> openFuture = null;
            if(readonly) {
                openFuture = client.open(status.getPath().getPath(), "r");
            } else {
                openFuture = client.open(status.getPath().getPath(), "w");
            }
        
            if(openFuture != null) {
                fi = client.processOpen(openFuture);
                return new SyndicateFSFileHandle(this, status, fi, readonly);
            } else {
                throw new IOException("Can not process REST operations");
            }
        } catch (Exception ex) {
            LOG.error("exception occurred", ex);
            throw new IOException(ex);
        }
    }
    
    private synchronized SyndicateFSFileHandle createNewFile(SyndicateFSPath absPath) throws IOException {
        if(absPath == null) {
            LOG.error("abspath is null");
            throw new IllegalArgumentException("abspath is null");
        }
        
        if(absPath.getParent() == null) {
            LOG.error("Cannot create a root");
            throw new IOException("Cannot create a root");
        }
        
        if(absPath.depth() < 1) {
            LOG.error("Cannot create a file at root");
            throw new IOException("Cannot create a file at root");
        }
        
        SyndicateFSFileStatus parent = getFileStatus(absPath.getParent());
        if(parent == null) {
            LOG.error("Parent directory does not exist");
            throw new IOException("Parent directory does not exist");
        }

        if(!parent.isDirectory()) {
            LOG.error("Parent directory does not exist");
            throw new IOException("Parent directory does not exist");
        }
        
        LOG.info("creating a file - " + absPath.getPath());
        
        try {
            SyndicateUGHttpClient client = getUGRestClient(absPath.getSessionName());
            Future<ClientResponse> openFuture = client.open(absPath.getPath(), "w");
            if(openFuture != null) {
                FileDescriptor fi = client.processOpen(openFuture);
                SyndicateFSFileStatus status = new SyndicateFSFileStatus(this, absPath);
                return new SyndicateFSFileHandle(this, status, fi, false);
            } else {
                throw new IOException("Can not process REST operations");
            }
        } catch (Exception ex) {
            LOG.error("exception occurred", ex);
            throw new IOException(ex);
        }
    }
    
    @Override
    public synchronized boolean exists(SyndicateFSPath path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = null;
        try {
            status = getFileStatus(absPath);
        } catch (IOException ex) {
            LOG.error("exception occurred", ex);
        }
        
        if(status != null) {
            return true;
        }
        return false;
    }

    @Override
    public synchronized boolean isDirectory(SyndicateFSPath path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = null;
        try {
            status = getFileStatus(absPath);
        } catch (IOException ex) {
            LOG.error("exception occurred", ex);
        }
        
        if(status != null) {
            return status.isDirectory();
        }
        return false;
    }

    @Override
    public synchronized boolean isFile(SyndicateFSPath path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = null;
        try {
            status = getFileStatus(absPath);
        } catch (IOException ex) {
            LOG.error("exception occurred", ex);
        }
        
        if(status != null) {
            return status.isFile();
        }
        return false;
    }

    @Override
    public synchronized long getSize(SyndicateFSPath path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = null;
        try {
            status = getFileStatus(absPath);
        } catch (IOException ex) {
            LOG.error("exception occurred", ex);
        }
        
        if(status != null) {
            return status.getSize();
        }
        return 0;
    }
    
    @Override
    public synchronized long getLastModifiedTime(SyndicateFSPath path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = null;
        try {
            status = getFileStatus(absPath);
        } catch (IOException ex) {
            LOG.error("exception occurred", ex);
        }
        
        if(status != null) {
            return status.getLastModification();
        }
        return 0;
    }
    
    @Override
    public synchronized long getLastAccessTime(SyndicateFSPath path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = null;
        try {
            status = getFileStatus(absPath);
        } catch (IOException ex) {
            LOG.error("exception occurred", ex);
        }
        
        if(status != null) {
            return status.getLastAccess();
        }
        return 0;
    }
    
    private FsAction parseMode(int mode) {
        FsAction action = FsAction.NONE;
        if((mode & 0x04) == 0x04) {
            action = action.or(FsAction.READ);
        }
        if((mode & 0x02) == 0x02) {
            action = action.or(FsAction.WRITE);
        }
        if((mode & 0x01) == 0x01) {
            action = action.or(FsAction.EXECUTE);
        }
        return action;
    }
    
    @Override
    public synchronized FsPermission getPermission(SyndicateFSPath path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = null;
        try {
            status = getFileStatus(absPath);
        } catch (IOException ex) {
            LOG.error("exception occurred", ex);
        }
        
        if(status != null) {
            int userMode = status.getUserMode();
            int groupMode = status.getGroupMode();
            int othersMode = status.getOthersMode();
            
            return new FsPermission(parseMode(userMode), parseMode(groupMode), parseMode(othersMode));
        }
        return new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.NONE);
    }
    
    @Override
    public synchronized String getOwner(SyndicateFSPath path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = null;
        try {
            status = getFileStatus(absPath);
        } catch (IOException ex) {
            LOG.error("exception occurred", ex);
        }
        
        // TODO: status does not provide owner info yet
        if(status != null) {
            return "syndicate";
        }
        return "syndicate";
    }
    
    @Override
    public synchronized String getGroup(SyndicateFSPath path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = null;
        try {
            status = getFileStatus(absPath);
        } catch (IOException ex) {
            LOG.error("exception occurred", ex);
        }
        
        // TODO: status does not provide permission info yet
        if(status != null) {
            return "syndicate";
        }
        return "syndicate";
    }
    
    @Override
    public synchronized int getReplication(SyndicateFSPath path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        return 1;
    }
    
    @Override
    public synchronized long getBlockSize() {
        return DEFAULT_BLOCK_SIZE;
    }
    
    @Override
    public long getBlockSize(SyndicateFSPath path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        
        // check memory cache
        Statvfs cachedStatVfs = this.statVfsCache.get(absPath);
        if(cachedStatVfs != null) {
            long bsize = cachedStatVfs.getBsize();
            if(bsize > 0) {
                return bsize;
            }
        }
        
        if(absPath.getParent() == null) {
            // root
            return getBlockSize();
        }
        
        // not in memory cache
        // query
        Statvfs statvfs = null;
        try {
            SyndicateUGHttpClient client = getUGRestClient(absPath.getSessionName());
            Future<ClientResponse> statvfsFuture = client.getStatvfs();
            if(statvfsFuture != null) {
                statvfs = client.processGetStatvfs(statvfsFuture);
                this.statVfsCache.put(absPath, statvfs);
                long bsize = statvfs.getBsize();
                if(bsize > 0) {
                    return bsize;
                }
            } else {
                LOG.error("Can not process REST operations");
            }
        } catch (Exception ex) {
            LOG.error("exception occurred", ex);
        }
        
        // use default
        return getBlockSize();
    }
    
    @Override
    public synchronized String[] listExtendedAttrs(SyndicateFSPath path) throws IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        if(absPath.getParent() == null) {
            // root
            return new String[0];
        }
        
        SyndicateFSFileStatus status = getFileStatus(absPath);
        if(status == null) {
            LOG.error("file not exist");
            throw new IOException("file not exist : " + path.getPath());
        }
        
        try {
            SyndicateUGHttpClient client = getUGRestClient(absPath.getSessionName());
            Future<ClientResponse> listXattrFuture = client.listXattr(absPath.getPath());
            if(listXattrFuture != null) {
                XattrKeyList processListXattr = client.processListXattr(listXattrFuture);
                return processListXattr.getKeys().toArray(new String[0]);
            } else {
                throw new IOException("Can not process REST operations");
            }
        } catch (Exception ex) {
            LOG.error("exception occurred", ex);
            throw new IOException(ex);
        }
    }

    @Override
    public synchronized String getExtendedAttr(SyndicateFSPath path, String name) throws IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        if(absPath.getParent() == null) {
            // root
            throw new IOException(String.format("Attribute not exist : %s - %s", absPath.getPath(), name));
        }
        
        SyndicateFSFileStatus status = getFileStatus(absPath);
        if(status == null) {
            LOG.error("file not exist");
            throw new IOException("file not exist : " + path.getPath());
        }
        
        try {
            SyndicateUGHttpClient client = getUGRestClient(absPath.getSessionName());
            Future<ClientResponse> getXattrFuture = client.getXattr(absPath.getPath(), name);
            if(getXattrFuture != null) {
                Xattr processGetXattr = client.processGetXattr(getXattrFuture);
                return processGetXattr.getValue();
            } else {
                throw new IOException("Can not process REST operations");
            }
        } catch (Exception ex) {
            LOG.error("exception occurred", ex);
            throw new IOException(ex);
        }
    }

    @Override
    public synchronized boolean delete(SyndicateFSPath path) throws FileNotFoundException, IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        if(absPath.getParent() == null) {
            // root
            throw new IOException("Can not delete root directory");
        }
        
        SyndicateFSFileStatus status = getFileStatus(absPath);
        if(status == null) {
            LOG.error("file not exist");
            throw new FileNotFoundException("file not exist : " + path.getPath());
        }
        
        if(absPath.depth() < 1) {
            throw new IOException("Can not delete top-level directories");
        }
        
        try {
            SyndicateUGHttpClient client = getUGRestClient(absPath.getSessionName());
            if(status.isFile()) {
                Future<ClientResponse> unlinkFuture = client.unlink(absPath.getPath());
                if(unlinkFuture != null) {
                    client.processUnlink(unlinkFuture);
                } else {
                    throw new IOException("Can not process REST operations");
                }
            } else if(status.isDirectory()) {
                Future<ClientResponse> removeDirFuture = client.removeDir(absPath.getPath());
                if(removeDirFuture != null) {
                    client.processRemoveDir(removeDirFuture);
                } else {
                    throw new IOException("Can not process REST operations");
                }
            } else {
                LOG.error("Can not delete from unknown status");
                throw new IOException("Can not delete from unknown status");
            }
        } catch (Exception ex) {
            LOG.error("exception occurred", ex);
            throw new IOException(ex);
        }
        this.fileStatusCache.remove(absPath);
        return true;
    }

    @Override
    public synchronized void rename(SyndicateFSPath path, SyndicateFSPath newpath) throws FileNotFoundException, IOException {
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
        
        if(absPath.depth() < 1) {
            LOG.error("source path is unmodifiable : " + absPath.getPath());
            throw new IOException("source path is unmodifiable : " + absPath.getPath());
        }
        
        if(absNewPath.depth() < 1) {
            LOG.error("dest path is not writable : " + absNewPath.getPath());
            throw new IOException("dest path is not writable : " + absNewPath.getPath());
        }
        
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
        
        try {
            SyndicateUGHttpClient client = getUGRestClient(absPath.getSessionName());
            Future<ClientResponse> renameFuture = client.rename(absPath.getPath(), absNewPath.getPath());
            if(renameFuture != null) {
                client.processRename(renameFuture);
            } else {
                throw new IOException("Can not process REST operations");
            }
        } catch (Exception ex) {
            LOG.error("exception occurred", ex);
            throw new IOException(ex);
        }
        
        this.fileStatusCache.remove(absPath);
    }

    @Override
    public synchronized void mkdir(SyndicateFSPath path) throws IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        
        if(absPath.depth() < 1) {
            LOG.error("dest path is not writable : " + absPath.getPath());
            throw new IOException("dest path is not writable : " + absPath.getPath());
        }
        
        SyndicateFSFileStatus status = getFileStatus(absPath.getParent());
        if(status == null) {
            LOG.error("parent directory does not exist : " + absPath.getPath());
            throw new IOException("parent directory does not exist : " + absPath.getPath());
        }
        
        try {
            SyndicateUGHttpClient client = getUGRestClient(absPath.getSessionName());
            Future<ClientResponse> makeDirFuture = client.makeDir(absPath.getPath(), 0744);
            if(makeDirFuture != null) {
                client.processMakeDir(makeDirFuture);
            } else {
                throw new IOException("Can not process REST operations");
            }
        } catch (Exception ex) {
            LOG.error("exception occurred", ex);
            throw new IOException(ex);
        }
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
    public synchronized String[] readDirectoryEntries(SyndicateFSPath path) throws FileNotFoundException, IOException {
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
        
        List<String> entries = new ArrayList<String>();
        
        try {
            SyndicateUGHttpClient client = getUGRestClient(absPath.getSessionName());
            Future<ClientResponse> readDirFuture = client.listDir(absPath.getPath());
            if(readDirFuture != null) {
                DirectoryEntries processReadDir = client.processListDir(readDirFuture);

                // remove duplicates
                Map<String, StatRaw> entryTable = new HashMap<String, StatRaw>();
                
                // need to remove duplicates
                int entryCnt = 0;
                for(StatRaw statRaw : processReadDir.getEntries()) {
                    entryCnt++;
                    if(entryCnt <= 2) {
                        // ignore . and ..
                        continue;
                    }

                    StatRaw eStatRaw = entryTable.get(statRaw.getName());
                    if(eStatRaw == null) {
                        // first
                        entryTable.put(statRaw.getName(), statRaw);
                    } else {
                        if(eStatRaw.getVersion() <= statRaw.getVersion()) {
                            // replace
                            entryTable.remove(statRaw.getName());
                            entryTable.put(statRaw.getName(), statRaw);
                        }
                    }
                }
                
                entries.addAll(entryTable.keySet());

                // put to memory cache
                for(StatRaw statRaw : entryTable.values()) {
                    SyndicateFSPath entryPath = new SyndicateFSPath(absPath, statRaw.getName());
                    this.fileStatusCache.remove(entryPath);
                    SyndicateFSFileStatus entryStatus = new SyndicateFSFileStatus(this, absPath, statRaw);
                    this.fileStatusCache.put(entryPath, entryStatus);
                }
            } else {
                throw new IOException("Can not process REST operations");
            }
        } catch (Exception ex) {
            LOG.error("exception occurred", ex);
            throw new IOException(ex);
        }
        
        return entries.toArray(new String[0]);
    }

    @Override
    public synchronized byte[] getLocalCachedBlocks(SyndicateFSPath path) throws FileNotFoundException, IOException {
        long blocksize = getBlockSize(path);
        long filesize = this.getSize(path);
        
        int blocknum = BlockUtils.getBlocks(filesize, blocksize);
        
        String blockBitmapString = getExtendedAttr(path, LOCAL_CACHED_BLOCKS_XATTR_NAME);
        
        byte[] bitmap = new byte[blocknum];
        for(int i=0;i<blocknum;i++) {
            bitmap[i] = 0;
            
            if(blockBitmapString != null) {
                if(i < blockBitmapString.length()) {
                    // exist
                    if(blockBitmapString.charAt(i) == '1') {
                        bitmap[i] = 1;
                    }
                }
            }
        }
        return bitmap;
    }
    
    @Override
    public synchronized Map<UnsignedLong, File> listLocalCachedBlocks(SyndicateFSPath path) throws FileNotFoundException, IOException {
        String localCachePath = getExtendedAttr(path, LOCAL_CACHED_FILE_PATH_XATTR_NAME);
    
        Map<UnsignedLong, File> fileTable = new HashMap<UnsignedLong, File>();
        File dir = new File(localCachePath);
        SyndicateLocalBlockCache localBlockCache = new SyndicateLocalBlockCache();
        if(dir.exists() && dir.isDirectory()) {
            File[] blockFilesList = dir.listFiles(localBlockCache.getFilenameFilter());
            LOG.info("found local block-cache files : " + blockFilesList.length);
            
            for(File file : blockFilesList) {
                LOG.info("block-cache file : " + file.getPath());
                UnsignedLong blockId = localBlockCache.getBlockID(file.getName());
                long blockVer = localBlockCache.getBlockVersion(file.getName());

                File existFile = fileTable.get(blockId);
                if(existFile == null) {
                    fileTable.put(blockId, file);
                } else {
                    String existFilename = existFile.getName();
                    long existBlockVer = 0;
                    if(localBlockCache.isValidFileName(existFilename)) {
                        existBlockVer = localBlockCache.getBlockVersion(existFilename);
                    }
                    
                    if(existBlockVer <= blockVer) {
                        // remove old
                        fileTable.remove(blockId);
                        // add new
                        fileTable.put(blockId, file);
                    }
                }
            }
        }
        return fileTable;
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
                LOG.error("exception occurred", ex);
            }
        }
        
        for(OutputStream os : this.openOutputStream) {
            try {
                os.close();
            } catch (IOException ex) {
                LOG.error("exception occurred", ex);
            }
        }
        
        this.fileStatusCache.clear();

        for(String key : this.ugRestClients.keySet()) {
            SyndicateUGHttpClient client = this.ugRestClients.get(key);
            client.close();
        }
        this.ugRestClients.clear();
        
        super.close();
        
        super.raiseOnAfterDestroyEvent();
    }
}
