package edu.arizona.cs.hsynth.fs.backend.localfs;

import edu.arizona.cs.hsynth.fs.HSynthFSConfiguration;
import edu.arizona.cs.hsynth.fs.HSynthFileSystem;
import edu.arizona.cs.hsynth.fs.HSynthFSFilenameFilter;
import edu.arizona.cs.hsynth.fs.HSynthFSPath;
import edu.arizona.cs.hsynth.fs.HSynthFSPathFilter;
import edu.arizona.cs.hsynth.fs.HSynthFSRandomAccess;
import edu.arizona.cs.hsynth.fs.HSynthFSInputStream;
import edu.arizona.cs.hsynth.fs.HSynthFSOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LocalFSFileSystem extends HSynthFileSystem {

    private static final Log LOG = LogFactory.getLog(LocalFSFileSystem.class);
    
    private List<LocalFSInputStream> openInputStream = new ArrayList<LocalFSInputStream>();
    private List<LocalFSOutputStream> openOutputStream = new ArrayList<LocalFSOutputStream>();
    private List<LocalFSRandomAccess> openRandomAccess = new ArrayList<LocalFSRandomAccess>();
    
    public LocalFSFileSystem(LocalFSConfiguration configuration) {
        initialize(configuration);
    }
    
    public LocalFSFileSystem(HSynthFSConfiguration configuration) {
        if(!(configuration instanceof LocalFSConfiguration)) {
            throw new IllegalArgumentException("Configuration is not an instance of LocalFSConfiguration");
        }
        
        initialize((LocalFSConfiguration)configuration);
    }
    
    private void initialize(LocalFSConfiguration conf) {
        if(conf == null) {
            LOG.error("FileSystem Initialize failed : configuration is null");
            throw new IllegalArgumentException("Can not initialize the filesystem from null configuration");
        }
        
        super.raiseOnBeforeCreateEvent(conf);
        
        super.initialize(conf);
        
        super.raiseOnAfterCreateEvent();
    }
    
    private LocalFSConfiguration getLocalFSConfiguration() {
        HSynthFSConfiguration conf = getConfiguration();
        if(conf != null) {
            return (LocalFSConfiguration)conf;
        }
        return null;
    }
    
    private String getLocalPath(HSynthFSPath path) {
        if(path == null) {
            LOG.error("Can not get LocalAbsolutePath from null path");
            throw new IllegalArgumentException("Can not get LocalAbsolutePath from null path");
        }
        
        HSynthFSPath absPath = getAbsolutePath(path);
        String absWorkingPath = getLocalFSConfiguration().getWorkingDir().getAbsolutePath();
        
        String filePath = absPath.getPath();
        
        if(!absWorkingPath.endsWith("/")) {
            if(filePath.startsWith("/")) {
                return absWorkingPath + filePath;
            } else {
                return absWorkingPath + "/" + filePath;
            }
        } else {
            if(filePath.startsWith("/")) {
                if(filePath.length() > 1)
                    return absWorkingPath + filePath.substring(1);
                else
                    return absWorkingPath;
            } else {
                return absWorkingPath + filePath;
            }
        }
    }
    
    
    @Override
    public boolean exists(HSynthFSPath path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        return file.exists();
    }

    @Override
    public boolean isDirectory(HSynthFSPath path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        return file.isDirectory();
    }

    @Override
    public boolean isFile(HSynthFSPath path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        return file.isFile();
    }
    
    @Override
    public long getSize(HSynthFSPath path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        return file.length();
    }
    
    @Override
    public long getBlockSize() {
        // default filesystem block size = 4KB
        return 1024 * 4;
    }

    @Override
    public void delete(HSynthFSPath path) throws FileNotFoundException, IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        if(!file.delete()) {
            LOG.error("Can not delete file : " + path.getPath());
            throw new IOException("Can not delete file : " + path.getPath());
        }
    }

    @Override
    public void rename(HSynthFSPath path, HSynthFSPath newpath) throws FileNotFoundException, IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        if(newpath == null) {
            LOG.error("newpath is null");
            throw new IllegalArgumentException("newpath is null");
        }
        
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        String realToPath = getLocalPath(newpath);
        File destfile = new File(realToPath);
        if(!file.renameTo(destfile)) {
            throw new IOException("Can not rename file : " + path.getPath());
        }
    }

    @Override
    public void mkdir(HSynthFSPath path) throws IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        if(!file.mkdir()) {
            throw new IOException("Can not make directory : " + path.getPath());
        }
    }

    @Override
    public HSynthFSInputStream getFileInputStream(HSynthFSPath path) throws IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        LocalFSInputStream is = new LocalFSInputStream(this, file);
        this.openInputStream.add(is);
        return is;
    }

    @Override
    public HSynthFSOutputStream getFileOutputStream(HSynthFSPath path) throws IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        LocalFSOutputStream os = new LocalFSOutputStream(this, file);
        this.openOutputStream.add(os);
        return os;
    }
    
    @Override
    public HSynthFSRandomAccess getRandomAccess(HSynthFSPath path) throws IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        LocalFSRandomAccess ra = new LocalFSRandomAccess(this, file, "rw");
        this.openRandomAccess.add(ra);
        return ra;
    }

    @Override
    public String[] readDirectoryEntries(final HSynthFSPath path, final HSynthFSFilenameFilter filter) throws FileNotFoundException, IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        if(!file.exists()) {
            throw new FileNotFoundException("file not found : " + path.getPath());
        }
        
        return file.list(new java.io.FilenameFilter() {

            @Override
            public boolean accept(File file, String string) {
                if(filter != null) {
                    return filter.accept(path, string);
                } else {
                    return true;
                }
            }
        });
    }

    @Override
    public String[] readDirectoryEntries(final HSynthFSPath path, final HSynthFSPathFilter filter) throws FileNotFoundException, IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        if(!file.exists()) {
            throw new FileNotFoundException("file not found : " + path.getPath());
        }
        
        return file.list(new java.io.FilenameFilter() {

            @Override
            public boolean accept(File file, String string) {
                if(filter != null) {
                    HSynthFSPath newPath = new HSynthFSPath(path, string);
                    return filter.accept(newPath);
                } else {
                    return true;
                }
            }
        });
    }

    void notifyInputStreamClosed(LocalFSInputStream inputStream) {
        if(inputStream == null) {
            LOG.error("inputStream is null");
            throw new IllegalArgumentException("inputStream is null");
        }
        
        this.openInputStream.remove(inputStream);
    }
    
    void notifyOutputStreamClosed(LocalFSOutputStream outputStream) {
        if(outputStream == null) {
            LOG.error("outputStream is null");
            throw new IllegalArgumentException("outputStream is null");
        }
        
        this.openOutputStream.remove(outputStream);
    }
    
    void notifyRandomAccessClosed(LocalFSRandomAccess raf) {
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
        for(LocalFSInputStream is : this.openInputStream) {
            try {
                is.close();
            } catch (IOException ex) {
                LOG.error(ex);
            }
        }
        
        for(LocalFSOutputStream os : this.openOutputStream) {
            try {
                os.close();
            } catch (IOException ex) {
                LOG.error(ex);
            }
        }
        
        for(LocalFSRandomAccess raf : this.openRandomAccess) {
            try {
                raf.close();
            } catch (IOException ex) {
                LOG.error(ex);
            }
        }
        
        super.close();
        
        super.raiseOnAfterDestroyEvent(this.conf);
    }
}
