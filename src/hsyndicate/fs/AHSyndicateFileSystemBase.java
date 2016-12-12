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
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.FsPermission;

public abstract class AHSyndicateFileSystemBase implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(AHSyndicateFileSystemBase.class);
    
    protected static final String FS_ROOT_PATH_STRING = "/";
    protected static final SyndicateFSPath FS_ROOT_PATH = new SyndicateFSPath(FS_ROOT_PATH_STRING);
    
    protected static List<ISyndicateFSEventHandler> eventHandlers = new ArrayList<ISyndicateFSEventHandler>();
    
    protected SyndicateFSConfiguration conf;
    protected SyndicateFSPath workingDir;
    
    protected boolean closed = true;
    
    public synchronized static void addEventHandler(ISyndicateFSEventHandler eventHandler) {
        if(eventHandler == null) {
            throw new IllegalArgumentException("Cannot add null event handler");
        }
        
        eventHandlers.add(eventHandler);
    }
    
    public synchronized static void removeEventHandler(ISyndicateFSEventHandler eventHandler) {
        if(eventHandler == null) {
            throw new IllegalArgumentException("Cannot remove null event handler");
        }
        
        eventHandlers.remove(eventHandler);
    }
    
    protected synchronized void raiseOnBeforeCreateEvent() {
        for(ISyndicateFSEventHandler handler : eventHandlers) {
            handler.onBeforeCreate(this.conf);
        }
    }
    
    protected synchronized void raiseOnAfterCreateEvent() {
        for(ISyndicateFSEventHandler handler : eventHandlers) {
            handler.onAfterCreate(this);
        }
    }
    
    protected synchronized void raiseOnBeforeDestroyEvent() {
        for(ISyndicateFSEventHandler handler : eventHandlers) {
            handler.onBeforeDestroy(this);
        }
    }
    
    protected synchronized void raiseOnAfterDestroyEvent() {
        for(ISyndicateFSEventHandler handler : eventHandlers) {
            handler.onBeforeCreate(this.conf);
        }
    }
    
    protected void initialize(SyndicateFSConfiguration conf) throws InstantiationException {
        this.conf = conf;
        this.workingDir = getRootPath();
        this.closed = false;
    }
    
    public synchronized boolean isClosed() {
        return this.closed;
    }
    
    public synchronized SyndicateFSConfiguration getConfiguration() {
        return this.conf;
    }
    
    public synchronized SyndicateFSPath getRootPath() {
        return FS_ROOT_PATH;
    }
    
    public synchronized SyndicateFSPath getWorkingDirectory() {
        return this.workingDir;
    }
    
    public synchronized void setWorkingDirectory(SyndicateFSPath path) {
        if(this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null) {
            this.workingDir = FS_ROOT_PATH;
        } else {
            if(path.isAbsolute()) {
                this.workingDir = new SyndicateFSPath(FS_ROOT_PATH, path);
            }
        }
    }
    
    @Override
    public synchronized void close() throws IOException {
        if(this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        this.closed = true;
    }
    
    public synchronized SyndicateFSPath getAbsolutePath(SyndicateFSPath path) {
        if(path == null) {
            throw new IllegalArgumentException("Can not get absolute file path from null path");
        }
        
        SyndicateFSPath absolute;
        if(!path.isAbsolute()) {
            // start from working dir
            absolute = new SyndicateFSPath(this.workingDir, path);
        } else {
            absolute = new SyndicateFSPath(FS_ROOT_PATH, path);
        }
        
        return absolute;
    }
    
    public abstract boolean exists(SyndicateFSPath path);
    
    public abstract boolean isDirectory(SyndicateFSPath path);
            
    public abstract boolean isFile(SyndicateFSPath path);
    
    public abstract long getSize(SyndicateFSPath path);
    
    public abstract long getLastModifiedTime(SyndicateFSPath path);
    
    public abstract long getLastAccessTime(SyndicateFSPath path);
    
    public abstract FsPermission getPermission(SyndicateFSPath path);
    
    public abstract String getOwner(SyndicateFSPath path);
    
    public abstract String getGroup(SyndicateFSPath path);
    
    public abstract int getReplication(SyndicateFSPath path);
    
    public abstract long getBlockSize();
    
    public abstract String[] listExtendedAttrs(SyndicateFSPath path) throws FileNotFoundException, IOException;
    
    public abstract String getExtendedAttr(SyndicateFSPath path, String name) throws FileNotFoundException, IOException;
    
    public abstract boolean delete(SyndicateFSPath path) throws FileNotFoundException, IOException;
    
    public abstract void rename(SyndicateFSPath path, SyndicateFSPath newpath) throws FileNotFoundException, IOException;
    
    public abstract void mkdir(SyndicateFSPath path) throws IOException;
    
    public synchronized boolean mkdirs(SyndicateFSPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("Can not create a new directory from null path");
        }
        
        //LOG.info("mkdirs - " + path.toString());
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        
        SyndicateFSPath[] ancestors = absPath.getAncestors();
        if(ancestors != null) {
            for(SyndicateFSPath ancestor : ancestors) {
                if(!exists(ancestor)) {
                    mkdir(ancestor);
                }
            }
        }
        
        if(!exists(absPath)) {
            mkdir(absPath);
            return true;
        }
        
        return true;
    }
    
    public abstract SyndicateFSInputStream getFileInputStream(SyndicateFSPath path) throws FileNotFoundException, IOException;
    
    public abstract SyndicateFSOutputStream getFileOutputStream(SyndicateFSPath path) throws IOException;
    
    public abstract String[] readDirectoryEntries(SyndicateFSPath path) throws FileNotFoundException, IOException;
    
    public synchronized String[] readDirectoryEntries(SyndicateFSPath path, ISyndicateFSFilenameFilter filter) throws FileNotFoundException, IOException {
        if(path == null) {
            throw new IllegalArgumentException("Can not list files from null path");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        
        String[] entries = readDirectoryEntries(absPath);
        if(entries == null) {
            return entries;
        }
        
        if(filter == null) {
            return entries;
        } else {
            List<String> arr = new ArrayList<String>();
            for(String entry : entries) {
                if(filter.accept(absPath, entry)) {
                    arr.add(entry);
                }
            }
            
            return arr.toArray(new String[0]);
        }
    }
    
    public synchronized String[] readDirectoryEntries(SyndicateFSPath path, ISyndicateFSPathFilter filter) throws FileNotFoundException, IOException {
        if(path == null) {
            throw new IllegalArgumentException("Can not list files from null path");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        
        String[] entries = readDirectoryEntries(absPath);
        if(entries == null) {
            return entries;
        }
        
        if(filter == null) {
            return entries;
        } else {
            List<String> arr = new ArrayList<String>();
            for(String entry : entries) {
                SyndicateFSPath entryPath = new SyndicateFSPath(absPath, entry);
                if(filter.accept(entryPath)) {
                    arr.add(entry);
                }
            }
            
            return arr.toArray(new String[0]);
        }
    }
    
    public synchronized SyndicateFSPath[] listAllFiles(SyndicateFSPath path) throws FileNotFoundException, IOException {
        return listAllFiles(path, (ISyndicateFSFilenameFilter)null);
    }
    
    public synchronized SyndicateFSPath[] listAllFiles(SyndicateFSPath path, ISyndicateFSFilenameFilter filter) throws FileNotFoundException, IOException {
        if(path == null) {
            throw new IllegalArgumentException("Can not list files from null path");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        if(!this.exists(absPath)) {
            throw new FileNotFoundException("path not found");
        }
        
        List<SyndicateFSPath> result = listAllFilesRecursive(absPath, filter);
        
        SyndicateFSPath[] paths = result.toArray(new SyndicateFSPath[0]);
        return paths;
    }
    
    public synchronized SyndicateFSPath[] listAllFiles(SyndicateFSPath path, ISyndicateFSPathFilter filter) throws FileNotFoundException, IOException {
        if(path == null) {
            throw new IllegalArgumentException("Can not list files from null path");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        if(!this.exists(absPath)) {
            throw new FileNotFoundException("path not found");
        }
        
        List<SyndicateFSPath> result = listAllFilesRecursive(absPath, filter);
        
        SyndicateFSPath[] paths = result.toArray(new SyndicateFSPath[0]);
        return paths;
    }
    
    private synchronized List<SyndicateFSPath> listAllFilesRecursive(SyndicateFSPath path, ISyndicateFSFilenameFilter filter) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("Can not list files from null path");
        }
        
        List<SyndicateFSPath> result = new ArrayList<SyndicateFSPath>();
        
        if(isFile(path)) {
            if(filter != null) {
                if(filter.accept(path.getParent(), path.getName())) {
                    result.add(path);
                }
            } else {
                result.add(path);
            }
        } else if(isDirectory(path)) {
            // entries
            String[] entries = readDirectoryEntries(path, filter);
            
            if(entries != null) {
                for(String entry : entries) {
                    SyndicateFSPath newEntryPath = new SyndicateFSPath(path, entry);
                    
                    if(filter != null) {
                        if(filter.accept(path, entry)) {
                            List<SyndicateFSPath> rec_result = listAllFilesRecursive(newEntryPath, filter);
                            result.addAll(rec_result);
                        }
                    } else {
                        List<SyndicateFSPath> rec_result = listAllFilesRecursive(newEntryPath, filter);
                        result.addAll(rec_result);
                    }
                }
            }
        }
        
        return result;
    }
    
    private synchronized List<SyndicateFSPath> listAllFilesRecursive(SyndicateFSPath path, ISyndicateFSPathFilter filter) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("Can not list files from null path");
        }
        
        List<SyndicateFSPath> result = new ArrayList<SyndicateFSPath>();
        
        if(isFile(path)) {
            if(filter != null) {
                if(filter.accept(path)) {
                    result.add(path);
                }
            } else {
                result.add(path);
            }
        } else if(isDirectory(path)) {
            // entries
            String[] entries = readDirectoryEntries(path, filter);
            
            if(entries != null) {
                for(String entry : entries) {
                    SyndicateFSPath newEntryPath = new SyndicateFSPath(path, entry);
                    
                    if(filter != null) {
                        if(filter.accept(newEntryPath)) {
                            List<SyndicateFSPath> rec_result = listAllFilesRecursive(newEntryPath, filter);
                            result.addAll(rec_result);
                        }
                    } else {
                        List<SyndicateFSPath> rec_result = listAllFilesRecursive(newEntryPath, filter);
                        result.addAll(rec_result);
                    }
                }
            }
        }
        
        return result;
    }
    
    public synchronized boolean deleteAll(SyndicateFSPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("Can not remove from null path");
        }
        
        SyndicateFSPath absPath = getAbsolutePath(path);
        
        if(this.exists(absPath)) {
            return deleteAllRecursive(absPath);
        }
        return true;
    }
    
    private synchronized boolean deleteAllRecursive(SyndicateFSPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("Can not delete files from null path");
        }
        
        if(isFile(path)) {
            // remove file
            return delete(path);
        } else if(isDirectory(path)) {
            // entries
            boolean success = true;
            
            String[] entries = readDirectoryEntries(path);
            
            if(entries != null) {
                for(String entry : entries) {
                    SyndicateFSPath newEntryPath = new SyndicateFSPath(path, entry);
                    boolean result = deleteAllRecursive(newEntryPath);
                    if(!result) {
                        success = false;
                    }
                }
            }
            
            if(success) {
                // remove dir
                return delete(path);
            } else {
                return false;
            }
        }
        return false;
    }
    
    public abstract byte[] getLocalCachedBlocks(SyndicateFSPath path) throws FileNotFoundException, IOException;
    public abstract Map<UnsignedLong, File> listLocalCachedBlocks(SyndicateFSPath path) throws FileNotFoundException, IOException;
    
    @Override
    public synchronized String toString() {
        return "Syndicate - " + this.conf.getAddress();
    }
}
