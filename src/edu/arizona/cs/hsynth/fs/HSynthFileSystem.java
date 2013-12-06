package edu.arizona.cs.hsynth.fs;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class HSynthFileSystem implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(HSynthFileSystem.class);
    
    protected static final String FS_ROOT_PATH_STRING = "hsyn:///";
    protected static final HSynthFSPath FS_ROOT_PATH = new HSynthFSPath(FS_ROOT_PATH_STRING);
    
    protected static List<HSynthFSEventHandler> eventHandlers = new ArrayList<HSynthFSEventHandler>();
    
    protected HSynthFSConfiguration conf;
    protected HSynthFSPath workingDir;
    
    protected boolean closed = true;
    
    synchronized static HSynthFileSystem createInstance(HSynthFSConfiguration conf) throws InstantiationException {
        HSynthFileSystem instance = null;
        Class fs_class = conf.getFileSystemClass();
        Constructor<HSynthFileSystem> fs_constructor = null;

        try {
            Class[] argTypes = new Class[]{conf.getClass()};
            fs_constructor = fs_class.getConstructor(argTypes);
            instance = fs_constructor.newInstance(conf);
        } catch (NoSuchMethodException ex) {
            throw new InstantiationException(ex.getMessage());
        } catch (IllegalAccessException ex) {
            throw new InstantiationException(ex.getMessage());
        } catch (IllegalArgumentException ex) {
            throw new InstantiationException(ex.getMessage());
        } catch (InvocationTargetException ex) {
            throw new InstantiationException(ex.getMessage());
        }
        return instance;
    }
    
    public synchronized static void addEventHandler(HSynthFSEventHandler eventHandler) {
        if(eventHandler == null) 
            throw new IllegalArgumentException("Cannot add null event handler");
        
        eventHandlers.add(eventHandler);
    }
    
    public synchronized static void removeEventHandler(HSynthFSEventHandler eventHandler) {
        if(eventHandler == null) 
            throw new IllegalArgumentException("Cannot remove null event handler");
        
        eventHandlers.remove(eventHandler);
    }
    
    protected synchronized void raiseOnBeforeCreateEvent(HSynthFSConfiguration conf) {
        for(HSynthFSEventHandler handler : eventHandlers) {
            handler.onBeforeCreate(conf);
        }
    }
    
    protected synchronized void raiseOnAfterCreateEvent() {
        for(HSynthFSEventHandler handler : eventHandlers) {
            handler.onAfterCreate(this);
        }
    }
    
    protected synchronized void raiseOnBeforeDestroyEvent() {
        for(HSynthFSEventHandler handler : eventHandlers) {
            handler.onBeforeDestroy(this);
        }
    }
    
    protected synchronized void raiseOnAfterDestroyEvent(HSynthFSConfiguration conf) {
        for(HSynthFSEventHandler handler : eventHandlers) {
            handler.onBeforeCreate(conf);
        }
    }
    
    protected void initialize(HSynthFSConfiguration conf) {
        // set configuration unmodifiable
        conf.lock();
        
        this.conf = conf;
        this.workingDir = getRootPath();
        this.closed = false;
        
        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                LOG.info("Runtime shutdown was detected");
                
                if(closed == false) {
                    try {
                        close();
                    } catch (IOException ex) {
                        LOG.error(ex);
                    }
                }
            }
        });
    }
    
    public synchronized boolean isClosed() {
        return this.closed;
    }
    
    protected synchronized HSynthFSConfiguration getConfiguration() {
        return this.conf;
    }
    
    public synchronized HSynthFSPath getRootPath() {
        return FS_ROOT_PATH;
    }
    
    public synchronized HSynthFSPath getWorkingDirectory() {
        return this.workingDir;
    }
    
    public synchronized void setWorkingDirectory(HSynthFSPath path) {
        if(this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null) {
            this.workingDir = FS_ROOT_PATH;
        } else {
            if(path.isAbsolute()) {
                this.workingDir = new HSynthFSPath(FS_ROOT_PATH, path);
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
    
    public synchronized HSynthFSPath getAbsolutePath(HSynthFSPath path) {
        if(path == null)
            throw new IllegalArgumentException("Can not get absolute file path from null path");
        
        HSynthFSPath absolute;
        if(!path.isAbsolute()) {
            // start from working dir
            absolute = new HSynthFSPath(this.workingDir, path);
        } else {
            absolute = new HSynthFSPath(FS_ROOT_PATH, path);
        }
        
        return absolute;
    }
    
    public abstract boolean exists(HSynthFSPath path);
    
    public abstract boolean isDirectory(HSynthFSPath path);
            
    public abstract boolean isFile(HSynthFSPath path);
    
    public abstract long getSize(HSynthFSPath path);
    
    public abstract long getBlockSize();
    
    public abstract void delete(HSynthFSPath path) throws FileNotFoundException, IOException;
    
    public abstract void rename(HSynthFSPath path, HSynthFSPath newpath) throws FileNotFoundException, IOException;
    
    public abstract void mkdir(HSynthFSPath path) throws IOException;
    
    public synchronized void mkdirs(HSynthFSPath path) throws IOException {
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null)
            throw new IllegalArgumentException("Can not create a new directory from null path");
        
        HSynthFSPath absPath = getAbsolutePath(path);
        
        HSynthFSPath[] ancestors = absPath.getAncestors();
        if(ancestors != null) {
            for(HSynthFSPath ancestor : ancestors) {
                if(!exists(ancestor)) {
                    mkdir(ancestor);
                }
            }
        }
        
        if(!exists(absPath)) {
            mkdir(absPath);
        }
    }
    
    public abstract HSynthFSInputStream getFileInputStream(HSynthFSPath path) throws FileNotFoundException, IOException;
    
    public abstract HSynthFSOutputStream getFileOutputStream(HSynthFSPath path) throws IOException;
    
    public abstract HSynthFSRandomAccess getRandomAccess(HSynthFSPath path) throws IOException;
    
    public synchronized String[] readDirectoryEntries(HSynthFSPath path) throws FileNotFoundException, IOException {
        return readDirectoryEntries(path, (HSynthFSFilenameFilter)null);
    }
    
    public abstract String[] readDirectoryEntries(HSynthFSPath path, HSynthFSFilenameFilter filter) throws FileNotFoundException, IOException;
    
    public abstract String[] readDirectoryEntries(HSynthFSPath path, HSynthFSPathFilter filter) throws FileNotFoundException, IOException;
    
    public synchronized HSynthFSPath[] listAllFiles(HSynthFSPath path) throws FileNotFoundException, IOException {
        return listAllFiles(path, (HSynthFSFilenameFilter)null);
    }
    
    public synchronized HSynthFSPath[] listAllFiles(HSynthFSPath path, HSynthFSFilenameFilter filter) throws FileNotFoundException, IOException {
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null)
            throw new IllegalArgumentException("Can not list files from null path");
        
        HSynthFSPath absPath = getAbsolutePath(path);
        if(!this.exists(absPath)) {
            throw new FileNotFoundException("path not found");
        }
        
        List<HSynthFSPath> result = listAllFilesRecursive(absPath, filter);
        
        HSynthFSPath[] paths = new HSynthFSPath[result.size()];
        paths = result.toArray(paths);
        return paths;
    }
    
    public synchronized HSynthFSPath[] listAllFiles(HSynthFSPath path, HSynthFSPathFilter filter) throws FileNotFoundException, IOException {
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null)
            throw new IllegalArgumentException("Can not list files from null path");
        
        HSynthFSPath absPath = getAbsolutePath(path);
        if(!this.exists(absPath)) {
            throw new FileNotFoundException("path not found");
        }
        
        List<HSynthFSPath> result = listAllFilesRecursive(absPath, filter);
        
        HSynthFSPath[] paths = new HSynthFSPath[result.size()];
        paths = result.toArray(paths);
        return paths;
    }
    
    private synchronized List<HSynthFSPath> listAllFilesRecursive(HSynthFSPath path, HSynthFSFilenameFilter filter) throws IOException {
        if(path == null)
            throw new IllegalArgumentException("Can not list files from null path");
        
        List<HSynthFSPath> result = new ArrayList<HSynthFSPath>();
        
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
                    HSynthFSPath newEntryPath = new HSynthFSPath(path, entry);
                    
                    if(filter != null) {
                        if(filter.accept(path, entry)) {
                            List<HSynthFSPath> rec_result = listAllFilesRecursive(newEntryPath, filter);
                            result.addAll(rec_result);
                        }
                    } else {
                        List<HSynthFSPath> rec_result = listAllFilesRecursive(newEntryPath, filter);
                        result.addAll(rec_result);
                    }
                }
            }
        }
        
        return result;
    }
    
    private synchronized List<HSynthFSPath> listAllFilesRecursive(HSynthFSPath path, HSynthFSPathFilter filter) throws IOException {
        if(path == null)
            throw new IllegalArgumentException("Can not list files from null path");
        
        List<HSynthFSPath> result = new ArrayList<HSynthFSPath>();
        
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
                    HSynthFSPath newEntryPath = new HSynthFSPath(path, entry);
                    
                    if(filter != null) {
                        if(filter.accept(newEntryPath)) {
                            List<HSynthFSPath> rec_result = listAllFilesRecursive(newEntryPath, filter);
                            result.addAll(rec_result);
                        }
                    } else {
                        List<HSynthFSPath> rec_result = listAllFilesRecursive(newEntryPath, filter);
                        result.addAll(rec_result);
                    }
                }
            }
        }
        
        return result;
    }
    
    public synchronized void deleteAll(HSynthFSPath path) throws IOException {
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null) {
            throw new IllegalArgumentException("Can not remove from null path");
        }
        
        HSynthFSPath absPath = getAbsolutePath(path);
        
        if(this.exists(absPath)) {
            deleteAllRecursive(absPath);
        }
    }
    
    private synchronized void deleteAllRecursive(HSynthFSPath path) throws IOException {
        if(path == null)
            throw new IllegalArgumentException("Can not delete files from null path");
        
        if(isFile(path)) {
            // remove file
            delete(path);
        } else if(isDirectory(path)) {
            // entries
            String[] entries = readDirectoryEntries(path);
            
            if(entries != null) {
                for(String entry : entries) {
                    HSynthFSPath newEntryPath = new HSynthFSPath(path, entry);
                    deleteAllRecursive(newEntryPath);
                }
            }
            
            // remove dir
            delete(path);
        }
    }
    
    @Override
    public String toString() {
        return "FileSystem - " + this.conf.getBackendName() + " backend";
    }
}
