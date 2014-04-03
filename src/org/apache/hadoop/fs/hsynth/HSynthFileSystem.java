package org.apache.hadoop.fs.hsynth;

import org.apache.hadoop.fs.hsynth.util.SyndicateFileSystemFactory;
import edu.arizona.cs.syndicate.fs.SyndicateFSPath;
import edu.arizona.cs.syndicate.fs.ASyndicateFileSystem;
import edu.arizona.cs.syndicate.fs.SyndicateFSConfiguration;
import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.hsynth.util.BlockUtils;
import org.apache.hadoop.fs.hsynth.util.HSynthConfigUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public class HSynthFileSystem extends FileSystem {

    private static final Log LOG = LogFactory.getLog(HSynthFileSystem.class);
    
    private URI uri;
    private ASyndicateFileSystem syndicateFS;
    private Path workingDir;
    
    private static final int BLOCK_NUM_PER_READ = 12;

    public HSynthFileSystem() {
    }

    @Override
    public URI getUri() {
        return this.uri;
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);
        if (this.syndicateFS == null) {
            this.syndicateFS = createHSynthFS(conf);
        }
        setConf(conf);
        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
        this.workingDir = new Path("/").makeQualified(this);
    }
    
    private static ASyndicateFileSystem createHSynthFS(Configuration conf) throws IOException {
        SyndicateFSConfiguration sconf = HSynthConfigUtils.createSyndicateConf(conf, "localhost");
        try {
            return SyndicateFileSystemFactory.getInstance(sconf);
        } catch (InstantiationException ex) {
            throw new IOException(ex.getCause());
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
    
    private SyndicateFSPath makeSyndicateFSPath(Path path) throws IOException {
        Path absolutePath = makeAbsolute(path);
        return createSyndicateFSPath(absolutePath.toUri());
    }
    
    private SyndicateFSPath createSyndicateFSPath(URI uri) throws IOException {
        return createSyndicateFSPath(uri.getPath());
    }
    
    private SyndicateFSPath createSyndicateFSPath(String path) throws IOException {
        return new SyndicateFSPath(path);
    }
    
    @Override
    public boolean mkdirs(Path path, FsPermission permission) throws IOException {
        SyndicateFSPath hpath = makeSyndicateFSPath(path);
        return this.syndicateFS.mkdirs(hpath);
    }
    
    @Override
    public boolean isFile(Path path) throws IOException {
        SyndicateFSPath hpath = makeSyndicateFSPath(path);
        return this.syndicateFS.isFile(hpath);
    }
    
    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        SyndicateFSPath hpath = makeSyndicateFSPath(f);
        if(!this.syndicateFS.exists(hpath)) {
            return null;
        }
        
        if(this.syndicateFS.isFile(hpath)) {
            return new FileStatus[]{
                new HSynthFileStatus(f.makeQualified(this), this.syndicateFS, hpath)
            };
        }
        
        List<FileStatus> ret = new ArrayList<FileStatus>();
        for (String p : this.syndicateFS.readDirectoryEntries(hpath)) {
            ret.add(getFileStatus(new Path(f, p)));
        }
        return ret.toArray(new FileStatus[0]);
    }
    
    /**
     * This optional operation is not yet supported.
     */
    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        throw new IOException("Not supported");
    }
    
    @Override
    public FSDataOutputStream create(Path file, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        SyndicateFSPath hpath = makeSyndicateFSPath(file);
        if(this.syndicateFS.exists(hpath)) {
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
        
        int bSize = Math.max(HSynthConfigUtils.getHSynthOutputBufferSize(getConf()), bufferSize);
        bSize = (int) Math.max(bSize, getDefaultBlockSize());
        return new FSDataOutputStream(new BufferedOutputStream(this.syndicateFS.getFileOutputStream(hpath), bSize), this.statistics);
    }
    
    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        SyndicateFSPath hpath = makeSyndicateFSPath(path);
        if (!this.syndicateFS.exists(hpath)) {
            throw new IOException("No such file.");
        }
        if (this.syndicateFS.isDirectory(hpath)) {
            throw new IOException("Path " + path + " is a directory.");
        }
        
        int bSize = Math.max(HSynthConfigUtils.getHSynthInputBufferSize(getConf()), bufferSize);
        bSize = (int) Math.max(bSize, getDefaultBlockSize());
        return new FSDataInputStream(new HSynthBufferedInputStream(new HSynthInputStream(getConf(), hpath, this.syndicateFS, this.statistics), bSize));
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        SyndicateFSPath hsrc = makeSyndicateFSPath(src);
        SyndicateFSPath hdst = makeSyndicateFSPath(dst);
        
        if (!this.syndicateFS.exists(hsrc)) {
            // src path doesn't exist
            return false;
        }
        if (this.syndicateFS.isDirectory(hdst)) {
            hdst = new SyndicateFSPath(hdst, hsrc.getName());
        }
        if (this.syndicateFS.exists(hdst)) {
            // dst path already exists - can't overwrite
            return false;
        }
        
        SyndicateFSPath hdstParent = hdst.getParent();
        if(hdstParent != null) {
            if (!this.syndicateFS.exists(hdstParent) || this.syndicateFS.isFile(hdstParent)) {
                // dst parent doesn't exist or is a file
                return false;
            }
        }
        
        this.syndicateFS.rename(hsrc, hdst);
        return true;
    }
    
    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        SyndicateFSPath hpath = makeSyndicateFSPath(path);
        if (!this.syndicateFS.exists(hpath)) {
            return false;
        }
        
        if (this.syndicateFS.isFile(hpath)) {
            return this.syndicateFS.delete(hpath);
        } else {
            // directory?
            if(recursive) {
                return this.syndicateFS.deleteAll(hpath);
            } else {
                return this.syndicateFS.delete(hpath);
            }
        }
    }
    
    @Override
    public boolean delete(Path path) throws IOException {
        return delete(path, true);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        SyndicateFSPath hpath = makeSyndicateFSPath(f);
        if(!this.syndicateFS.exists(hpath)) {
            throw new FileNotFoundException(f + ": No such file or directory.");
        }
        
        return new HSynthFileStatus(f.makeQualified(this), this.syndicateFS, hpath);
    }
    
    @Override
    public long getDefaultBlockSize() {
        return this.syndicateFS.getBlockSize() * BLOCK_NUM_PER_READ;
    }
    
    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) {
        try {
            HSynthUGMonitor monitor = new HSynthUGMonitor(this.getConf());
            SyndicateFSPath hpath = makeSyndicateFSPath(file.getPath());

            long filesize = file.getLen();
            long blocksize = getDefaultBlockSize();
            
            int startblockID = BlockUtils.getBlockID(start, blocksize);
            int endblockID = BlockUtils.getBlockID(start + len, blocksize);
            int effectiveblocklen = endblockID - startblockID + 1;

            BlockLocation[] locations = new BlockLocation[effectiveblocklen];
            List<HSynthUGMonitorResults<byte[]>> localCachedBlockInfo = monitor.getLocalCachedBlockInfo(hpath);
            
            for(int i=0;i<effectiveblocklen;i++) {
                locations[i] = new BlockLocation();
                locations[i].setOffset(BlockUtils.getBlockStartOffset(startblockID + i, blocksize));
                locations[i].setLength(BlockUtils.getBlockLength(filesize, blocksize, startblockID + i));
                
                List<String> gateway_hostnames = new ArrayList<String>();
                
                for(HSynthUGMonitorResults<byte[]> info : localCachedBlockInfo) {
                    if(info.getResult() != null) {
                        boolean hasCache = BlockUtils.checkBlockPresence((startblockID + i)*BLOCK_NUM_PER_READ, info.getResult());
                        if(hasCache) {
                            gateway_hostnames.add(info.getHostname());
                        }
                    }
                }
                
                if(gateway_hostnames.isEmpty()) {
                    gateway_hostnames.add("localhost");
                }
                
                locations[i].setHosts(gateway_hostnames.toArray(new String[0]));
                LOG.info("block " + i + " : " + locations[i].getHosts()[0]);
                locations[i].setNames(null);
            }
            
            return locations;
        } catch (Exception ex) {
            LOG.info(ex);
        }
        return null;
    }
    
    private static class HSynthFileStatus extends FileStatus {

        HSynthFileStatus(Path f, ASyndicateFileSystem fs, SyndicateFSPath hpath) throws IOException {
            super(findLength(fs, hpath), fs.isDirectory(hpath), 1, findBlocksize(fs), 0, f);
        }

        private static long findLength(ASyndicateFileSystem fs, SyndicateFSPath hpath) {
            if (!fs.isDirectory(hpath)) {
                return fs.getSize(hpath);
            }
            return 0;
        }

        private static long findBlocksize(ASyndicateFileSystem fs) {
            return fs.getBlockSize() * BLOCK_NUM_PER_READ;
        }
    }
    
    @Override
    public void close() throws IOException {
        this.syndicateFS.close();
        
        super.close();
    }
}
