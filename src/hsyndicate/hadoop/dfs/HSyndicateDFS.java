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
package hsyndicate.hadoop.dfs;

import hsyndicate.fs.SyndicateFileSystemFactory;
import hsyndicate.fs.SyndicateFSPath;
import hsyndicate.fs.AHSyndicateFileSystemBase;
import hsyndicate.fs.SyndicateFSConfiguration;
import hsyndicate.fs.SyndicateFileSystem;
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
import hsyndicate.utils.BlockUtils;
import hsyndicate.hadoop.utils.HSyndicateConfigUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public class HSyndicateDFS extends FileSystem {

    private static final Log LOG = LogFactory.getLog(HSyndicateDFS.class);
    
    private URI uri;
    private SyndicateFileSystem syndicateFS;
    private Path workingDir;
    
    public HSyndicateDFS() {
    }

    @Override
    public synchronized URI getUri() {
        return this.uri;
    }

    @Override
    public synchronized void initialize(URI uri, Configuration conf) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        super.initialize(uri, conf);
        
        if (this.syndicateFS == null) {
            this.syndicateFS = createHSyndicateFS(conf);
        }
        
        setConf(conf);
        
        this.uri = uri;
        this.workingDir = new Path("/").makeQualified(this);
    }
    
    private static SyndicateFileSystem createHSyndicateFS(Configuration conf) throws IOException {
        SyndicateFSConfiguration sconf = HSyndicateConfigUtils.createSyndicateConf(conf);
        try {
            return SyndicateFileSystemFactory.getInstance(sconf);
        } catch (InstantiationException ex) {
            throw new IOException(ex.getCause());
        }
    }
    
    @Override
    public synchronized String getName() {
        return getUri().toString();
    }

    @Override
    public synchronized Path getWorkingDirectory() {
        return this.workingDir;
    }
    
    @Override
    public synchronized void setWorkingDirectory(Path path) {
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
    public synchronized boolean mkdirs(Path path, FsPermission permission) throws IOException {
        SyndicateFSPath hpath = makeSyndicateFSPath(path);
        return this.syndicateFS.mkdirs(hpath);
    }
    
    @Override
    public synchronized boolean isFile(Path path) throws IOException {
        SyndicateFSPath hpath = makeSyndicateFSPath(path);
        return this.syndicateFS.isFile(hpath);
    }
    
    @Override
    public synchronized FileStatus[] listStatus(Path f) throws IOException {
        SyndicateFSPath hpath = makeSyndicateFSPath(f);
        if(!this.syndicateFS.exists(hpath)) {
            return null;
        }
        
        if(this.syndicateFS.isFile(hpath)) {
            return new FileStatus[]{
                new HSyndicateFileStatus(f.makeQualified(this), this.syndicateFS, hpath)
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
    public synchronized FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        throw new IOException("Not supported");
    }
    
    @Override
    public synchronized FSDataOutputStream create(Path file, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
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
        
        return new FSDataOutputStream(this.syndicateFS.getFileOutputStream(hpath), this.statistics);
    }
    
    @Override
    public synchronized FSDataInputStream open(Path path, int bufferSize) throws IOException {
        SyndicateFSPath hpath = makeSyndicateFSPath(path);
        if (!this.syndicateFS.exists(hpath)) {
            throw new IOException("No such file.");
        }
        if (this.syndicateFS.isDirectory(hpath)) {
            throw new IOException("Path " + path + " is a directory.");
        }
        
        return new FSDataInputStream(new HSyndicateInputStream(this.syndicateFS, hpath, this.statistics));
    }

    @Override
    public synchronized boolean rename(Path src, Path dst) throws IOException {
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
    public synchronized boolean delete(Path path, boolean recursive) throws IOException {
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
    public synchronized boolean delete(Path path) throws IOException {
        return delete(path, true);
    }

    @Override
    public synchronized FileStatus getFileStatus(Path f) throws IOException {
        SyndicateFSPath hpath = makeSyndicateFSPath(f);
        if(!this.syndicateFS.exists(hpath)) {
            throw new FileNotFoundException(f + ": No such file or directory.");
        }
        
        return new HSyndicateFileStatus(f.makeQualified(this), this.syndicateFS, hpath);
    }
    
    @Override
    public synchronized long getDefaultBlockSize() {
        return this.syndicateFS.getBlockSize();
    }
    
    @Override
    public synchronized BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) {
        try {
            HSyndicateUGMonitor monitor = new HSyndicateUGMonitor(this.getConf());
            SyndicateFSPath hpath = makeSyndicateFSPath(file.getPath());

            long filesize = file.getLen();
            long blocksize = getDefaultBlockSize();
            
            int startblockID = BlockUtils.getBlockID(start, blocksize);
            int endblockID = BlockUtils.getBlockID(start + len, blocksize);
            int effectiveblocklen = endblockID - startblockID + 1;

            BlockLocation[] locations = new BlockLocation[effectiveblocklen];
            List<HSyndicateUGMonitorResults<byte[]>> localCachedBlockInfo = monitor.getLocalCachedBlockInfo(hpath);
            
            for(int i=0;i<effectiveblocklen;i++) {
                locations[i] = new BlockLocation();
                locations[i].setOffset(BlockUtils.getBlockStartOffset(startblockID + i, blocksize));
                locations[i].setLength(BlockUtils.getBlockLength(filesize, blocksize, startblockID + i));
                
                List<String> gateway_hostnames = new ArrayList<String>();
                
                for(HSyndicateUGMonitorResults<byte[]> info : localCachedBlockInfo) {
                    if(info.getResult() != null) {
                        boolean hasCache = BlockUtils.checkBlockPresence(startblockID + i, info.getResult());
                        if(hasCache) {
                            gateway_hostnames.add(info.getHostname());
                        }
                    }
                }
                
                List<String> userGatewayHosts = monitor.getUserGatewayHosts();
                
                if(gateway_hostnames.isEmpty()) {
                    gateway_hostnames.addAll(userGatewayHosts);
                    //gateway_hostnames.add("localhost");
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
    
    private static class HSyndicateFileStatus extends FileStatus {

        HSyndicateFileStatus(Path p, SyndicateFileSystem fs, SyndicateFSPath hpath) throws IOException {
            super(getFileLength(fs, hpath), 
                    fs.isDirectory(hpath), 
                    fs.getReplication(hpath), 
                    fs.getBlockSize(), 
                    fs.getLastModifiedTime(hpath), 
                    fs.getLastAccessTime(hpath), 
                    fs.getPermission(hpath), 
                    fs.getOwner(hpath), 
                    fs.getGroup(hpath), 
                    p);
        }

        private static long getFileLength(SyndicateFileSystem fs, SyndicateFSPath hpath) {
            if (!fs.isDirectory(hpath)) {
                return fs.getSize(hpath);
            }
            return 0;
        }
    }
    
    @Override
    public synchronized void close() throws IOException {
        this.syndicateFS.close();
        
        super.close();
    }
}
