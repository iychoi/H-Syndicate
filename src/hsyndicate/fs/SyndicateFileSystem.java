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

import com.google.common.primitives.UnsignedLong;
import com.sun.jersey.api.client.ClientResponse;
import hsyndicate.rest.client.SyndicateUGHttpClient;
import hsyndicate.rest.common.RestfulException;
import hsyndicate.rest.datatypes.DirectoryEntries;
import hsyndicate.rest.datatypes.FileDescriptor;
import hsyndicate.rest.datatypes.StatRaw;
import hsyndicate.rest.datatypes.Statvfs;
import hsyndicate.rest.datatypes.Xattr;
import hsyndicate.rest.datatypes.XattrKeyList;
import hsyndicate.utils.BlockUtils;
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
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

public class SyndicateFileSystem extends AHSyndicateFileSystemBase {

    private static final Log LOG = LogFactory.getLog(SyndicateFileSystem.class);

    private static final int DEFAULT_FILESTATUS_TIMETOLIVE = 60000; // 60 sec
    private static final String LOCAL_CACHED_BLOCKS_XATTR_NAME = "user.syndicate_cached_blocks";
    private static final String LOCAL_CACHED_FILE_PATH_XATTR_NAME = "user.syndicate_cached_file_path";
    
    
    private SyndicateUGHttpClient client;
    
    private List<SyndicateFSInputStream> openInputStream = new ArrayList<SyndicateFSInputStream>();
    private List<SyndicateFSOutputStream> openOutputStream = new ArrayList<SyndicateFSOutputStream>();
    
    private Map<SyndicateFSPath, SyndicateFSFileStatus> filestatusCache = new PassiveExpiringMap<SyndicateFSPath, SyndicateFSFileStatus>(DEFAULT_FILESTATUS_TIMETOLIVE);
    private long fsBlockSize = 0;
    
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
        
        this.client = new SyndicateUGHttpClient(conf.getHost(), conf.getPort());

        super.raiseOnAfterCreateEvent();
    }
    
    public synchronized SyndicateUGHttpClient getUGRestClient() {
        return this.client;
    }
    
    private SyndicateFSFileStatus getFileStatus(SyndicateFSPath abspath) throws IOException {
        if(abspath == null) {
            LOG.error("Can not get FileStatus from null abspath");
            throw new IllegalArgumentException("Can not get FileStatus from null abspath");
        }
        
        // check memory cache
        SyndicateFSFileStatus cached_status = this.filestatusCache.get(abspath);
        
        if(cached_status != null && !cached_status.isDirty()) {
            return cached_status;
        }
        
        // not in memory cache
        StatRaw statRaw = null;
        try {
            Future<ClientResponse> statFuture = this.client.getStat(abspath.getPath());
            if(statFuture != null) {
                statRaw = this.client.processGetStat(statFuture);
            } else {
                throw new IOException("Can not create a REST client");
            }
        } catch (FileNotFoundException ex) {
            // silent
            return null;
        } catch (Exception ex) {
            LOG.error("exception occurred", ex);
            throw new IOException(ex);
        }
        
        SyndicateFSFileStatus status = new SyndicateFSFileStatus(this, abspath, statRaw);
        this.filestatusCache.put(abspath, status);
        
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
        
        FileDescriptor fi = null;
        Future<ClientResponse> openFuture = null;
        if(readonly) {
            openFuture = this.client.open(status.getPath().getPath(), "r");
        } else {
            openFuture = this.client.open(status.getPath().getPath(), "w");
        }
        
        if(openFuture != null) {
            try {
                fi = this.client.processOpen(openFuture);
                return new SyndicateFSFileHandle(this, status, fi, readonly);
            } catch (Exception ex) {
                LOG.error("exception occurred", ex);
                throw new IOException(ex);
            }
        } else {
            throw new IOException("Can not create a REST client");
        }
    }
    
    private synchronized SyndicateFSFileHandle createNewFile(SyndicateFSPath abspath) throws IOException {
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
        
        LOG.info("creating a file - " + abspath.getPath());
        
        Future<ClientResponse> openFuture = this.client.open(abspath.getPath(), "w");
        if(openFuture != null) {
            try {
                FileDescriptor fi = this.client.processOpen(openFuture);
                Future<ClientResponse> closeFuture = this.client.close(abspath.getPath(), fi);
                if(closeFuture != null) {
                    this.client.processClose(closeFuture);
                } else {
                    throw new IOException("Can not create a REST client");
                }
            } catch (Exception ex) {
                LOG.error("exception occurred", ex);
                throw new IOException(ex);
            }
        } else {
            throw new IOException("Can not create a REST client");
        }
        
        SyndicateFSFileStatus status = getFileStatus(abspath);
        return getFileHandle(status, false);
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
        // check memory cache
        if(this.fsBlockSize > 0) {
            return this.fsBlockSize;
        }
        
        // query
        Statvfs statvfs = null;
        try {
            Future<ClientResponse> statvfsFuture = this.client.getStatvfs();
            if(statvfsFuture != null) {
                statvfs = this.client.processGetStatvfs(statvfsFuture);
            } else {
                LOG.error("Can not create a REST client");
            }
        } catch (Exception ex) {
            LOG.error("exception occurred", ex);
            this.fsBlockSize = 0;
        }
        
        this.fsBlockSize = statvfs.getBsize();
        return this.fsBlockSize;
    }
    
    @Override
    public synchronized String[] listExtendedAttrs(SyndicateFSPath path) throws IOException {
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
            try {
                XattrKeyList processListXattr = this.client.processListXattr(listXattrFuture);
                return processListXattr.getKeys().toArray(new String[0]);
            } catch (Exception ex) {
                LOG.error("exception occurred", ex);
                throw new IOException(ex);
            }
        } else {
            throw new IOException("Can not create a REST client");
        }
    }

    @Override
    public synchronized String getExtendedAttr(SyndicateFSPath path, String name) throws IOException {
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
            try {
                Xattr processGetXattr = this.client.processGetXattr(getXattrFuture);
                return processGetXattr.getValue();
            } catch (Exception ex) {
                LOG.error("exception occurred", ex);
                throw new IOException(ex);
            }
        } else {
            throw new IOException("Can not create a REST client");
        }
    }

    @Override
    public synchronized boolean delete(SyndicateFSPath path) throws FileNotFoundException, IOException {
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
                try {
                    this.client.processUnlink(unlinkFuture);
                } catch (RestfulException ex) {
                    LOG.error("exception occurred", ex);
                    throw new IOException(ex);
                }
            } else {
                throw new IOException("Can not create a REST client");
            }
        } else if(status.isDirectory()) {
            Future<ClientResponse> removeDirFuture = this.client.removeDir(absPath.getPath());
            if(removeDirFuture != null) {
                try {
                    this.client.processRemoveDir(removeDirFuture);
                } catch (RestfulException ex) {
                    LOG.error("exception occurred", ex);
                    throw new IOException(ex);
                }
            } else {
                throw new IOException("Can not create a REST client");
            }
        } else {
            LOG.error("Can not delete from unknown status");
            throw new IOException("Can not delete from unknown status");
        }
        
        this.filestatusCache.remove(absPath);
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
            try {
                this.client.processRename(renameFuture);
            } catch (Exception ex) {
                LOG.error("exception occurred", ex);
                throw new IOException(ex);
            }
        } else {
            throw new IOException("Can not create a REST client");
        }
        
        this.filestatusCache.remove(absPath);
    }

    @Override
    public synchronized void mkdir(SyndicateFSPath path) throws IOException {
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
            try {
                this.client.processMakeDir(makeDirFuture);
            } catch (Exception ex) {
                LOG.error("exception occurred", ex);
                throw new IOException(ex);
            }
        } else {
            throw new IOException("Can not create a REST client");
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
        Future<ClientResponse> readDirFuture = this.client.listDir(absPath.getPath());
        if(readDirFuture != null) {
            try {
                DirectoryEntries processReadDir = this.client.processListDir(readDirFuture);

                // remove duplicates
                Map<String, StatRaw> entryTable = new HashMap<String, StatRaw>();
                
                // need to remove duplicates
                for(StatRaw statRaw : processReadDir.getEntries()) {
                    if(statRaw.getName().startsWith("/")) {
                        // current directory?
                        // ignore
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
                    this.filestatusCache.remove(entryPath);
                    SyndicateFSFileStatus entryStatus = new SyndicateFSFileStatus(this, absPath, statRaw);
                    this.filestatusCache.put(entryPath, entryStatus);
                }
            } catch (Exception ex) {
                LOG.error("exception occurred", ex);
                throw new IOException(ex);
            }
        } else {
            throw new IOException("Can not create a REST client");
        }
        
        return entries.toArray(new String[0]);
    }

    @Override
    public synchronized byte[] getLocalCachedBlocks(SyndicateFSPath path) throws FileNotFoundException, IOException {
        long blocksize = getBlockSize();
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
    
    public synchronized Map<Integer, File> listLocalCachedBlocks(SyndicateFSPath path) throws FileNotFoundException, IOException {
        String localCachePath = getExtendedAttr(path, LOCAL_CACHED_FILE_PATH_XATTR_NAME);
    
        Map<Integer, File> fileTable = new HashMap<Integer, File>();
        File dir = new File(localCachePath);
        if(dir.exists() && dir.isDirectory()) {
            File[] blockFilesList = dir.listFiles();

            for(File file : blockFilesList) {
                String filename = file.getName();
                int dotidx = filename.indexOf(".");
                if(dotidx > 0) {
                    String blockId = filename.substring(0, dotidx);
                    String blockVer = filename.substring(dotidx+1);

                    int blockId_int = Integer.parseInt(blockId);
                    int blockVer_int = Integer.parseInt(blockVer);

                    File existFile = fileTable.get(blockId_int);
                    if(existFile == null) {
                        fileTable.put(blockId_int, file);
                    } else {
                        String existFilename = existFile.getName();
                        int existBlockVer_int = 0;
                        int existDotidx = existFilename.indexOf(".");
                        if(existDotidx > 0) {
                            String existBlockVer = existFilename.substring(existDotidx+1);
                            existBlockVer_int = Integer.parseInt(existBlockVer);
                        }

                        if(existBlockVer_int <= blockVer_int) {
                            // remove old
                            fileTable.remove(blockId_int);
                            // add new
                            fileTable.put(blockId_int, file);
                        }
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
        
        this.filestatusCache.clear();
        
        this.client.close();
        
        super.close();
        
        super.raiseOnAfterDestroyEvent();
    }
}
