package edu.arizona.cs.hsynth.fs;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class FileSystem implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(FileSystem.class);
    
    protected static final String FS_ROOT_PATH_STRING = "hsyn:///";
    protected static final Path FS_ROOT_PATH = new Path(FS_ROOT_PATH_STRING);
    
    protected static List<FSEventHandler> eventHandlers = new ArrayList<FSEventHandler>();
    
    protected Configuration conf;
    protected Path workingDir;
    
    protected boolean closed = true;
    
    synchronized static FileSystem createInstance(Configuration conf) throws InstantiationException {
        FileSystem instance = null;
        Class fs_class = conf.getFileSystemClass();
        Constructor<FileSystem> fs_constructor = null;

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
    
    public synchronized static void addEventHandler(FSEventHandler eventHandler) {
        if(eventHandler == null) 
            throw new IllegalArgumentException("Cannot add null event handler");
        
        eventHandlers.add(eventHandler);
    }
    
    public synchronized static void removeEventHandler(FSEventHandler eventHandler) {
        if(eventHandler == null) 
            throw new IllegalArgumentException("Cannot remove null event handler");
        
        eventHandlers.remove(eventHandler);
    }
    
    protected synchronized void raiseOnBeforeCreateEvent(Configuration conf) {
        for(FSEventHandler handler : eventHandlers) {
            handler.onBeforeCreate(conf);
        }
    }
    
    protected synchronized void raiseOnAfterCreateEvent() {
        for(FSEventHandler handler : eventHandlers) {
            handler.onAfterCreate(this);
        }
    }
    
    protected synchronized void raiseOnBeforeDestroyEvent() {
        for(FSEventHandler handler : eventHandlers) {
            handler.onBeforeDestroy(this);
        }
    }
    
    protected synchronized void raiseOnAfterDestroyEvent(Configuration conf) {
        for(FSEventHandler handler : eventHandlers) {
            handler.onBeforeCreate(conf);
        }
    }
    
    protected void initialize(Configuration conf) {
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
    
    protected synchronized Configuration getConfiguration() {
        return this.conf;
    }
    
    public synchronized Path getRootPath() {
        return FS_ROOT_PATH;
    }
    
    public synchronized Path getWorkingDirectory() {
        return this.workingDir;
    }
    
    public synchronized void setWorkingDirectory(Path path) {
        if(this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null) {
            this.workingDir = FS_ROOT_PATH;
        } else {
            if(path.isAbsolute()) {
                this.workingDir = new Path(FS_ROOT_PATH, path);
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
    
    public synchronized Path getAbsolutePath(Path path) {
        if(path == null)
            throw new IllegalArgumentException("Can not get absolute file path from null path");
        
        Path absolute;
        if(!path.isAbsolute()) {
            // start from working dir
            absolute = new Path(this.workingDir, path);
        } else {
            absolute = new Path(FS_ROOT_PATH, path);
        }
        
        return absolute;
    }
    
    public abstract boolean exists(Path path);
    
    public abstract boolean isDirectory(Path path);
            
    public abstract boolean isFile(Path path);
    
    public abstract long getSize(Path path);
    
    public abstract long getBlockSize();
    
    public abstract void delete(Path path) throws FileNotFoundException, IOException;
    
    public abstract void rename(Path path, Path newpath) throws FileNotFoundException, IOException;
    
    public abstract void mkdir(Path path) throws IOException;
    
    public synchronized void mkdirs(Path path) throws IOException {
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null)
            throw new IllegalArgumentException("Can not create a new directory from null path");
        
        Path absPath = getAbsolutePath(path);
        
        Path[] ancestors = absPath.getAncestors();
        if(ancestors != null) {
            for(Path ancestor : ancestors) {
                if(!exists(ancestor)) {
                    mkdir(ancestor);
                }
            }
        }
        
        if(!exists(absPath)) {
            mkdir(absPath);
        }
    }
    
    public abstract InputStream getFileInputStream(Path path) throws FileNotFoundException, IOException;
    
    public abstract OutputStream getFileOutputStream(Path path) throws IOException;
    
    public abstract RandomAccess getRandomAccess(Path path) throws IOException;
    
    public synchronized String[] readDirectoryEntries(Path path) throws FileNotFoundException, IOException {
        return readDirectoryEntries(path, (FilenameFilter)null);
    }
    
    public abstract String[] readDirectoryEntries(Path path, FilenameFilter filter) throws FileNotFoundException, IOException;
    
    public abstract String[] readDirectoryEntries(Path path, PathFilter filter) throws FileNotFoundException, IOException;
    
    public synchronized Path[] listAllFiles(Path path) throws FileNotFoundException, IOException {
        return listAllFiles(path, (FilenameFilter)null);
    }
    
    public synchronized Path[] listAllFiles(Path path, FilenameFilter filter) throws FileNotFoundException, IOException {
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null)
            throw new IllegalArgumentException("Can not list files from null path");
        
        Path absPath = getAbsolutePath(path);
        if(!this.exists(absPath)) {
            throw new FileNotFoundException("path not found");
        }
        
        List<Path> result = listAllFilesRecursive(absPath, filter);
        
        Path[] paths = new Path[result.size()];
        paths = result.toArray(paths);
        return paths;
    }
    
    public synchronized Path[] listAllFiles(Path path, PathFilter filter) throws FileNotFoundException, IOException {
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null)
            throw new IllegalArgumentException("Can not list files from null path");
        
        Path absPath = getAbsolutePath(path);
        if(!this.exists(absPath)) {
            throw new FileNotFoundException("path not found");
        }
        
        List<Path> result = listAllFilesRecursive(absPath, filter);
        
        Path[] paths = new Path[result.size()];
        paths = result.toArray(paths);
        return paths;
    }
    
    private synchronized List<Path> listAllFilesRecursive(Path path, FilenameFilter filter) throws IOException {
        if(path == null)
            throw new IllegalArgumentException("Can not list files from null path");
        
        List<Path> result = new ArrayList<Path>();
        
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
                    Path newEntryPath = new Path(path, entry);
                    
                    if(filter != null) {
                        if(filter.accept(path, entry)) {
                            List<Path> rec_result = listAllFilesRecursive(newEntryPath, filter);
                            result.addAll(rec_result);
                        }
                    } else {
                        List<Path> rec_result = listAllFilesRecursive(newEntryPath, filter);
                        result.addAll(rec_result);
                    }
                }
            }
        }
        
        return result;
    }
    
    private synchronized List<Path> listAllFilesRecursive(Path path, PathFilter filter) throws IOException {
        if(path == null)
            throw new IllegalArgumentException("Can not list files from null path");
        
        List<Path> result = new ArrayList<Path>();
        
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
                    Path newEntryPath = new Path(path, entry);
                    
                    if(filter != null) {
                        if(filter.accept(newEntryPath)) {
                            List<Path> rec_result = listAllFilesRecursive(newEntryPath, filter);
                            result.addAll(rec_result);
                        }
                    } else {
                        List<Path> rec_result = listAllFilesRecursive(newEntryPath, filter);
                        result.addAll(rec_result);
                    }
                }
            }
        }
        
        return result;
    }
    
    public synchronized void deleteAll(Path path) throws IOException {
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null) {
            throw new IllegalArgumentException("Can not remove from null path");
        }
        
        Path absPath = getAbsolutePath(path);
        
        if(this.exists(absPath)) {
            deleteAllRecursive(absPath);
        }
    }
    
    private synchronized void deleteAllRecursive(Path path) throws IOException {
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
                    Path newEntryPath = new Path(path, entry);
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
