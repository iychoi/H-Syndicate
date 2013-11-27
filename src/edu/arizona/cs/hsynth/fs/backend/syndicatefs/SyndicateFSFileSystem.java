package edu.arizona.cs.hsynth.fs.backend.syndicatefs;

import edu.arizona.cs.hsynth.fs.Configuration;
import edu.arizona.cs.hsynth.fs.FileSystem;
import edu.arizona.cs.hsynth.fs.FilenameFilter;
import edu.arizona.cs.hsynth.fs.Path;
import edu.arizona.cs.hsynth.fs.PathFilter;
import edu.arizona.cs.hsynth.fs.RandomAccess;
import edu.arizona.cs.hsynth.fs.backend.syndicatefs.client.message.SyndicateFSFileInfo;
import edu.arizona.cs.hsynth.fs.backend.syndicatefs.client.message.SyndicateFSStat;
import edu.arizona.cs.hsynth.fs.cache.ICache;
import edu.arizona.cs.hsynth.fs.cache.TimeoutCache;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SyndicateFSFileSystem extends FileSystem {

    private static final Log LOG = LogFactory.getLog(SyndicateFSFileSystem.class);

    private SyndicateFSClientInterface client;
    
    private List<SyndicateFSInputStream> openInputStream = new ArrayList<SyndicateFSInputStream>();
    private List<SyndicateFSOutputStream> openOutputStream = new ArrayList<SyndicateFSOutputStream>();
    private List<SyndicateFSRandomAccess> openRandomAccess = new ArrayList<SyndicateFSRandomAccess>();
    
    private ICache<Path, SyndicateFSFileStatus> filestatus_cache;
    
    public SyndicateFSFileSystem(SyndicateFSConfiguration configuration) throws InstantiationException {
        initialize(configuration);
    }
    
    public SyndicateFSFileSystem(Configuration configuration) throws InstantiationException {
        if(!(configuration instanceof SyndicateFSConfiguration)) {
            throw new IllegalArgumentException("Configuration is not an instance of SyndicateFSConfiguration");
        }
        
        initialize((SyndicateFSConfiguration)configuration);
    }
    
    private void initialize(SyndicateFSConfiguration conf) throws InstantiationException {
        if(conf == null) {
            LOG.error("FileSystem Initialize failed : configuration is null");
            throw new IllegalArgumentException("Can not initialize the filesystem from null configuration");
        }
        
        super.raiseOnBeforeCreateEvent(conf);

        super.initialize(conf);
        
        this.client = new SyndicateFSClientInterface("localhost", conf.getPort());
        this.filestatus_cache = new TimeoutCache<Path, SyndicateFSFileStatus>(conf.getMaxMetadataCacheSize(), conf.getCacheTimeoutSecond());
        
        super.raiseOnAfterCreateEvent();
    }
    
    private SyndicateFSConfiguration getSyndicateFSConfiguration() {
        Configuration conf = getConfiguration();
        if(conf != null) {
            return (SyndicateFSConfiguration)conf;
        }
        return null;
    }
    
    private SyndicateFSFileStatus getFileStatus(Path abspath) {
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
        SyndicateFSStat stat = null;
        try {
            stat = this.client.getStat(abspath.getPath());
        } catch (IOException ex) {
            LOG.error(ex);
            return null;
        }
        
        SyndicateFSFileStatus status = new SyndicateFSFileStatus(this, abspath, stat);
        this.filestatus_cache.insert(abspath, status);
        
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
        
        SyndicateFSFileInfo fi = this.client.getFileHandle(status.getPath().getPath());
        if(fi == null) {
            LOG.error("Can not get file handle from status");
            throw new IOException("Can not get file handle from status");
        }
        
        return new SyndicateFSFileHandle(this, this.client, status, fi, readonly);
    }
    
    private SyndicateFSFileHandle createNewFile(Path abspath) throws IOException {
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
        
        SyndicateFSStat stat = client.createNewFile(abspath.getPath());
        if(stat == null) {
            LOG.error("Can not create a new file");
            throw new IOException("Can not create a new file");
        }
        
        SyndicateFSFileStatus status = new SyndicateFSFileStatus(this, abspath, stat);
        this.filestatus_cache.insert(abspath, status);
        
        return getFileHandle(status, false);
    }
    
    @Override
    public boolean exists(Path path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        Path absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = getFileStatus(absPath);
        if(status != null) {
            return true;
        }
        return false;
    }

    @Override
    public boolean isDirectory(Path path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        Path absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = getFileStatus(absPath);
        if(status != null) {
            return status.isDirectory();
        }
        return false;
    }

    @Override
    public boolean isFile(Path path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        Path absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = getFileStatus(absPath);
        if(status != null) {
            return status.isFile();
        }
        return false;
    }

    @Override
    public long getSize(Path path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        Path absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = getFileStatus(absPath);
        if(status != null) {
            return status.getSize();
        }
        return 0;
    }
    
    @Override
    public long getBlockSize() {
        SyndicateFSFileStatus status = getFileStatus(new Path("/"));
        if(status != null) {
            return status.getBlockSize();
        }
        return 0;
    }

    @Override
    public void delete(Path path) throws IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        Path absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = getFileStatus(absPath);
        if(status == null) {
            LOG.error("file not exist");
            throw new IOException("file not exist : " + path.getPath());
        }
        
        if(status.isFile()) {
            this.client.delete(absPath.getPath());   
        } else if(status.isDirectory()) {
            this.client.removeDirectory(absPath.getPath());
        } else {
            LOG.error("Can not delete from unknown status");
            throw new IOException("Can not delete from unknown status");
        }
        
        this.filestatus_cache.invalidate(absPath);
    }

    @Override
    public void rename(Path path, Path newpath) throws IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        if(newpath == null) {
            LOG.error("newpath is null");
            throw new IllegalArgumentException("newpath is null");
        }
        
        Path absPath = getAbsolutePath(path);
        Path absNewPath = getAbsolutePath(newpath);
        
        SyndicateFSFileStatus status = getFileStatus(absPath);
        SyndicateFSFileStatus newStatus = getFileStatus(absNewPath);
        SyndicateFSFileStatus newStatusParent = getFileStatus(absNewPath.getParent());
        
        if(status == null) {
            LOG.error("source file does not exist : " + path.getPath());
            throw new IOException("source file does not exist : " + path.getPath());
        }
        
        if(newStatus != null) {
            LOG.error("target file already exists : " + newpath.getPath());
            throw new IOException("target file already exists : " + newpath.getPath());
        }
        
        if(newStatusParent == null) {
            LOG.error("parent directory of target file does not exist : " + newpath.getPath());
            throw new IOException("parent directory of target file does not exist : " + newpath.getPath());
        }
        
        this.client.rename(absPath.getPath(), absNewPath.getPath());
        
        this.filestatus_cache.invalidate(absPath);
    }

    @Override
    public void mkdir(Path path) throws IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        Path absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = getFileStatus(absPath.getParent());
        if(status == null) {
            LOG.error("parent directory does not exist : " + absPath.getPath());
            throw new IOException("parent directory does not exist : " + absPath.getPath());
        }
        this.client.mkdir(absPath.getPath());
    }

    @Override
    public InputStream getFileInputStream(Path path) throws IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        Path absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = getFileStatus(absPath);
        if(status == null) {
            LOG.error("Can not open the file to read : " + absPath.getPath());
            throw new IOException("Can not open the file to read : " + absPath.getPath());
        }
        
        SyndicateFSFileHandle handle = getFileHandle(status, true);
        if(handle == null) {
            LOG.error("Can not open the file to read : " + absPath.getPath());
            throw new IOException("Can not open the file to read : " + absPath.getPath());
        }
        
        return new SyndicateFSInputStream(this, handle);
    }

    @Override
    public OutputStream getFileOutputStream(Path path) throws IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        Path absPath = getAbsolutePath(path);
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
            
            handle.truncate(0);
            
            return new SyndicateFSOutputStream(this, handle);
        } else {
            // create new file
            SyndicateFSFileHandle handle = createNewFile(absPath);
            if(handle == null) {
                LOG.error("Can not create a file to write : " + absPath.getPath());
                throw new IOException("Can not create a file to write : " + absPath.getPath());
            }

            return new SyndicateFSOutputStream(this, handle);
        }
    }

    @Override
    public RandomAccess getRandomAccess(Path path) throws IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        Path absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = getFileStatus(absPath);
        if(status == null) {
            LOG.error("Can not open the file to read : " + absPath.getPath());
            throw new IOException("Can not open the file to read : " + absPath.getPath());
        }
        
        SyndicateFSFileHandle handle = getFileHandle(status, true);
        if(handle == null) {
            LOG.error("Can not open the file to read : " + absPath.getPath());
            throw new IOException("Can not open the file to read : " + absPath.getPath());
        }
        
        return new SyndicateFSRandomAccess(this, handle);
    }

    @Override
    public String[] readDirectoryEntries(Path path, FilenameFilter filter) throws FileNotFoundException, IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        Path absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = getFileStatus(absPath);
        if(status == null) {
            LOG.error("directory does not exist : " + absPath.getPath());
            throw new IOException("directory does not exist : " + absPath.getPath());
        }
        
        String[] entries = this.client.readDirectoryEntries(absPath.getPath());
        
        if(filter == null) {
            return entries;
        } else {
            List<String> arr = new ArrayList<String>();
            for(String entry : entries) {
                if(filter.accept(absPath, entry)) {
                    arr.add(entry);
                }
            }
            
            String[] entries_filtered = new String[arr.size()];
            entries_filtered = arr.toArray(entries_filtered);
            return entries_filtered;
        }
    }

    @Override
    public String[] readDirectoryEntries(Path path, PathFilter filter) throws FileNotFoundException, IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        Path absPath = getAbsolutePath(path);
        SyndicateFSFileStatus status = getFileStatus(absPath);
        if(status == null) {
            LOG.error("directory does not exist : " + absPath.getPath());
            throw new IOException("directory does not exist : " + absPath.getPath());
        }
        
        String[] entries = this.client.readDirectoryEntries(absPath.getPath());
        
        if(filter == null) {
            return entries;
        } else {
            List<String> arr = new ArrayList<String>();
            for(String entry : entries) {
                if(filter.accept(new Path(absPath, entry))) {
                    arr.add(entry);
                }
            }
            
            String[] entries_filtered = new String[arr.size()];
            entries_filtered = arr.toArray(entries_filtered);
            return entries_filtered;
        }
    }
    
    void notifyInputStreamClosed(SyndicateFSInputStream inputStream) {
        if(inputStream == null) {
            LOG.error("inputStream is null");
            throw new IllegalArgumentException("inputStream is null");
        }
        
        this.openInputStream.remove(inputStream);
    }
    
    void notifyOutputStreamClosed(SyndicateFSOutputStream outputStream) {
        if(outputStream == null) {
            LOG.error("outputStream is null");
            throw new IllegalArgumentException("outputStream is null");
        }
        
        this.openOutputStream.remove(outputStream);
    }
    
    void notifyRandomAccessClosed(SyndicateFSRandomAccess raf) {
        if(raf == null) {
            LOG.error("RandomAccess is null");
            throw new IllegalArgumentException("RandomAccess is null");
        }
        
        this.openRandomAccess.remove(raf);
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
        
        for(SyndicateFSOutputStream os : this.openOutputStream) {
            try {
                os.close();
            } catch (IOException ex) {
                LOG.error(ex);
            }
        }
        
        for(SyndicateFSRandomAccess raf : this.openRandomAccess) {
            try {
                raf.close();
            } catch (IOException ex) {
                LOG.error(ex);
            }
        }
        
        this.filestatus_cache.clear();
        
        this.client.close();
        
        super.close();
        
        super.raiseOnAfterDestroyEvent(this.conf);
    }
}
