package edu.arizona.cs.hsynth.fs.backend.localfs;

import edu.arizona.cs.hsynth.fs.Configuration;
import edu.arizona.cs.hsynth.fs.FileSystem;
import edu.arizona.cs.hsynth.fs.FilenameFilter;
import edu.arizona.cs.hsynth.fs.Path;
import edu.arizona.cs.hsynth.fs.PathFilter;
import edu.arizona.cs.hsynth.fs.RandomAccess;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LocalFSFileSystem extends FileSystem {

    private static final Log LOG = LogFactory.getLog(LocalFSFileSystem.class);
    
    private List<LocalFSInputStream> openInputStream = new ArrayList<LocalFSInputStream>();
    private List<LocalFSOutputStream> openOutputStream = new ArrayList<LocalFSOutputStream>();
    private List<LocalFSRandomAccess> openRandomAccess = new ArrayList<LocalFSRandomAccess>();
    
    public LocalFSFileSystem(LocalFSConfiguration configuration) {
        initialize(configuration);
    }
    
    public LocalFSFileSystem(Configuration configuration) {
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
        Configuration conf = getConfiguration();
        if(conf != null) {
            return (LocalFSConfiguration)conf;
        }
        return null;
    }
    
    private String getLocalPath(Path path) {
        if(path == null) {
            LOG.error("Can not get LocalAbsolutePath from null path");
            throw new IllegalArgumentException("Can not get LocalAbsolutePath from null path");
        }
        
        Path absPath = getAbsolutePath(path);
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
    public boolean exists(Path path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        return file.exists();
    }

    @Override
    public boolean isDirectory(Path path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        return file.isDirectory();
    }

    @Override
    public boolean isFile(Path path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        return file.isFile();
    }
    
    @Override
    public long getSize(Path path) {
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
    public void delete(Path path) throws FileNotFoundException, IOException {
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
    public void rename(Path path, Path newpath) throws FileNotFoundException, IOException {
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
    public void mkdir(Path path) throws IOException {
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
    public InputStream getFileInputStream(Path path) throws IOException {
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
    public OutputStream getFileOutputStream(Path path) throws IOException {
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
    public RandomAccess getRandomAccess(Path path) throws IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        LocalFSRandomAccess ra = new LocalFSRandomAccess(this, file, "rb");
        this.openRandomAccess.add(ra);
        return ra;
    }

    @Override
    public String[] readDirectoryEntries(final Path path, final FilenameFilter filter) throws FileNotFoundException, IOException {
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
    public String[] readDirectoryEntries(final Path path, final PathFilter filter) throws FileNotFoundException, IOException {
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
                    Path newPath = new Path(path, string);
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
