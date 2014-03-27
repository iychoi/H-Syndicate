package edu.arizona.cs.syndicate.fs;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class ASyndicateFileSystem implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(ASyndicateFileSystem.class);
    
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
        
        /*
        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if(closed == false) {
                    try {
                        LOG.info("Runtime shutdown was detected - Closing Syndicate FileSystem");
                        close();
                    } catch (IOException ex) {
                        LOG.error(ex);
                    }
                }
            }
        });
        */
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
        if(path == null)
            throw new IllegalArgumentException("Can not get absolute file path from null path");
        
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
    
    public abstract long getBlockSize();
    
    public abstract String[] listExtendedAttrs(SyndicateFSPath path) throws IOException;
    
    public abstract String getExtendedAttr(SyndicateFSPath path, String name) throws IOException;
    
    public abstract boolean delete(SyndicateFSPath path) throws FileNotFoundException, IOException;
    
    public abstract void rename(SyndicateFSPath path, SyndicateFSPath newpath) throws FileNotFoundException, IOException;
    
    public abstract void mkdir(SyndicateFSPath path) throws IOException;
    
    public synchronized boolean mkdirs(SyndicateFSPath path) throws IOException {
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null) {
            throw new IllegalArgumentException("Can not create a new directory from null path");
        }
        
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
        
        return false;
    }
    
    public abstract InputStream getFileInputStream(SyndicateFSPath path) throws FileNotFoundException, IOException;
    
    public abstract OutputStream getFileOutputStream(SyndicateFSPath path) throws IOException;
    
    public abstract ISyndicateFSRandomAccess getRandomAccess(SyndicateFSPath path) throws IOException;
    
    public synchronized String[] readDirectoryEntries(SyndicateFSPath path) throws FileNotFoundException, IOException {
        return readDirectoryEntries(path, (ISyndicateFSFilenameFilter)null);
    }
    
    public abstract String[] readDirectoryEntries(SyndicateFSPath path, ISyndicateFSFilenameFilter filter) throws FileNotFoundException, IOException;
    
    public abstract String[] readDirectoryEntries(SyndicateFSPath path, ISyndicateFSPathFilter filter) throws FileNotFoundException, IOException;
    
    public synchronized SyndicateFSPath[] listAllFiles(SyndicateFSPath path) throws FileNotFoundException, IOException {
        return listAllFiles(path, (ISyndicateFSFilenameFilter)null);
    }
    
    public synchronized SyndicateFSPath[] listAllFiles(SyndicateFSPath path, ISyndicateFSFilenameFilter filter) throws FileNotFoundException, IOException {
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
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
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
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
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
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
    
    @Override
    public String toString() {
        return "Syndicate - " + this.conf.getHost() + ":" + this.conf.getPort();
    }
}
