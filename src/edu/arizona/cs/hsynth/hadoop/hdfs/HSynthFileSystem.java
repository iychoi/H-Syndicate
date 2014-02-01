package edu.arizona.cs.hsynth.hadoop.hdfs;

import edu.arizona.cs.hsynth.fs.HSynthFSConfiguration;
import edu.arizona.cs.hsynth.fs.HSynthFSPath;
import edu.arizona.cs.hsynth.hadoop.util.HSynthConfigUtil;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public class HSynthFileSystem extends FileSystem {

    private URI uri;
    private edu.arizona.cs.hsynth.fs.HSynthFileSystem hsynth;
    private Path workingDir;

    public HSynthFileSystem() {
    }

    @Override
    public URI getUri() {
        return this.uri;
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);
        if (this.hsynth == null) {
            this.hsynth = createDefaultHSynthFS(conf);
        }
        setConf(conf);
        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
        this.workingDir = new Path("/").makeQualified(this);
    }
    
    private static edu.arizona.cs.hsynth.fs.HSynthFileSystem createDefaultHSynthFS(Configuration conf) throws IOException {
        edu.arizona.cs.hsynth.fs.HSynthFileSystem fs = null;
        try {
            HSynthFSConfiguration hconf = HSynthConfigUtil.getHSynthFSConfigurationInstance(conf);
            fs = hconf.getContext().getFileSystem();
            return fs;
        } catch (InstantiationException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public String getName() {
        return getUri().toString();
    }

    @Override
    public Path getWorkingDirectory() {
        return this.workingDir;
    }
    
    @Override
    public void setWorkingDirectory(Path path) {
        this.workingDir = makeAbsolute(path);
    }
    
    private Path makeAbsolute(Path path) {
        if (path.isAbsolute()) {
            return path;
        }
        return new Path(this.workingDir, path);
    }
    
    private HSynthFSPath makeHSynthFSPath(Path path) {
        Path absolutePath = makeAbsolute(path);
        URI uri = absolutePath.toUri();
        return new HSynthFSPath(uri);
    }
    
    @Override
    public boolean mkdirs(Path path, FsPermission permission) throws IOException {
        HSynthFSPath hpath = makeHSynthFSPath(path);
        this.hsynth.mkdirs(hpath);
        return true;
    }
    
    @Override
    public boolean isFile(Path path) throws IOException {
        HSynthFSPath hpath = makeHSynthFSPath(path);
        return this.hsynth.isFile(hpath);
    }
    
    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        HSynthFSPath hpath = makeHSynthFSPath(f);
        if(!this.hsynth.exists(hpath)) {
            return null;
        }
        
        if(this.hsynth.isFile(hpath)) {
            return new FileStatus[]{
                new HSynthFileStatus(f.makeQualified(this), this.hsynth, hpath)
            };
        }
        
        List<FileStatus> ret = new ArrayList<FileStatus>();
        for (String p : this.hsynth.readDirectoryEntries(hpath)) {
            ret.add(getFileStatus(new Path(f, p)));
        }
        return ret.toArray(new FileStatus[0]);
    }
    
    /**
     * This optional operation is not yet supported.
     */
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        throw new IOException("Not supported");
    }
    
    @Override
    public FSDataOutputStream create(Path file, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        HSynthFSPath hpath = makeHSynthFSPath(file);
        if(this.hsynth.exists(hpath)) {
            if (overwrite) {
                delete(file);
            } else {
                throw new IOException("File already exists: " + file);
            }
        } else {
            Path parent = file.getParent();
            if(parent != null) {
                if (!mkdirs(parent)) {
                    throw new IOException("Mkdirs failed to create " + parent.toString());
                }
            }
        }
        
        return new FSDataOutputStream(this.hsynth.getFileOutputStream(hpath));
    }
    
    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        HSynthFSPath hpath = makeHSynthFSPath(path);
        if (!this.hsynth.exists(hpath)) {
            throw new IOException("No such file.");
        }
        if (this.hsynth.isDirectory(hpath)) {
            throw new IOException("Path " + path + " is a directory.");
        }
        return new FSDataInputStream(new HSynthInputStream(getConf(), hpath, this.hsynth, statistics));
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        HSynthFSPath hsrc = makeHSynthFSPath(src);
        HSynthFSPath hdst = makeHSynthFSPath(dst);
        
        if (!this.hsynth.exists(hsrc)) {
            // src path doesn't exist
            return false;
        }
        if (this.hsynth.isDirectory(hdst)) {
            hdst = new HSynthFSPath(hdst, hsrc.getName());
        }
        if (this.hsynth.exists(hdst)) {
            // dst path already exists - can't overwrite
            return false;
        }
        
        HSynthFSPath hdstParent = hdst.getParent();
        if(hdstParent != null) {
            if (!this.hsynth.exists(hdstParent) || this.hsynth.isFile(hdstParent)) {
                // dst parent doesn't exist or is a file
                return false;
            }
        }
        
        this.hsynth.rename(hsrc, hdst);
        return true;
    }
    
    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        HSynthFSPath hpath = makeHSynthFSPath(path);
        if (!this.hsynth.exists(hpath)) {
            return false;
        }
        
        if (this.hsynth.isFile(hpath)) {
            this.hsynth.delete(hpath);
        } else {
            if(recursive) {
                this.hsynth.deleteAll(hpath);
            } else {
                this.hsynth.delete(hpath);
            }
        }
        return true;
    }
    
    @Override
    public boolean delete(Path path) throws IOException {
        return delete(path, true);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        HSynthFSPath hpath = makeHSynthFSPath(f);
        if(!this.hsynth.exists(hpath)) {
            throw new FileNotFoundException(f + ": No such file or directory.");
        }
        
        return new HSynthFileStatus(f.makeQualified(this), this.hsynth, hpath);
    }
    
    @Override
    public long getDefaultBlockSize() {
        return this.hsynth.getBlockSize();
    }
    
    private static class HSynthFileStatus extends FileStatus {

        HSynthFileStatus(Path f, edu.arizona.cs.hsynth.fs.HSynthFileSystem fs, HSynthFSPath hpath) throws IOException {
            super(findLength(fs, hpath), fs.isDirectory(hpath), 1, findBlocksize(fs), 0, f);
        }

        private static long findLength(edu.arizona.cs.hsynth.fs.HSynthFileSystem fs, HSynthFSPath hpath) {
            if (!fs.isDirectory(hpath)) {
                return fs.getSize(hpath);
            }
            return 0;
        }

        private static long findBlocksize(edu.arizona.cs.hsynth.fs.HSynthFileSystem fs) {
            return fs.getBlockSize();
        }
    }
}
