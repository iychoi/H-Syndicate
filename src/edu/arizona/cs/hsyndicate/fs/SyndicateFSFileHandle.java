/*
 * Copyright 2015 iychoi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.arizona.cs.hsyndicate.fs;

import com.sun.jersey.api.client.ClientResponse;
import edu.arizona.cs.hsyndicate.fs.client.SyndicateUGRestClient;
import edu.arizona.cs.hsyndicate.fs.datatypes.FileInfo;
import edu.arizona.cs.hsyndicate.util.BlockUtils;
import edu.arizona.cs.hsyndicate.util.IPUtils;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SyndicateFSFileHandle implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(SyndicateFSFileHandle.class);

    private SyndicateFileSystem filesystem;
    private SyndicateFSFileStatus status;
    private FileInfo fileinfo;
    private boolean readonly = false;
    private boolean closed = true;
    private boolean modified = false;
    private long blockSize;
    private int blockNum;
    private boolean localFileSystem;
    private String cachedFilePath;
    private Map<Integer, File> cachedBlocks;
    
    SyndicateFSFileHandle(SyndicateFileSystem fs, SyndicateFSFileStatus status, FileInfo fi, boolean readonly) {
        this.filesystem = fs;
        this.status = status;
        this.fileinfo = fi;
        this.readonly = readonly;
        this.closed = false;
        this.modified = false;

        this.blockSize = status.getBlockSize();
        this.blockNum = BlockUtils.getBlocks(status.getSize(), this.blockSize);
        
        String host = this.filesystem.getConfiguration().getHost();
        if(IPUtils.isLocalIPAddress(host)) {
            this.localFileSystem = true;
            try {
                this.cachedFilePath = fs.getLocalCachePath(status.getPath());
                this.cachedBlocks = listCachedBlocks(this.cachedFilePath);
            } catch (IOException ex) {
                LOG.info("exception raised while reading cache info : " + ex.toString());
                this.cachedFilePath = null;
                this.cachedBlocks = null;
            }
        } else {
            this.localFileSystem = false;
        }
    }
    
    public SyndicateFileSystem getFileSystem() {
        return this.filesystem;
    }
    
    public SyndicateFSPath getPath() {
        return this.status.getPath();
    }
    
    public synchronized SyndicateFSFileStatus getStatus() {
        return this.status;
    }
    
    public String getCachedFilePath() {
        return this.cachedFilePath;
    }
    
    private Map<Integer, File> listCachedBlocks(String cachedFilePath) throws IOException {
        if(cachedFilePath == null || cachedFilePath.isEmpty()) {
            throw new IllegalArgumentException("cachedFilePath is null or empty");
        }
        
        if(!this.localFileSystem) {
            throw new IllegalStateException("filesystem is not local");
        }

        File dir = new File(cachedFilePath);
        if(dir.exists() && dir.isDirectory()) {
            Map<Integer, File> table = new HashMap<Integer, File>();
            File[] blockFilesList = dir.listFiles();

            for(File file : blockFilesList) {
                String filename = file.getName();
                int dotidx = filename.indexOf(".");
                if(dotidx > 0) {
                    String blockId = filename.substring(0, dotidx);
                    String blockVer = filename.substring(dotidx+1);

                    int blockId_int = Integer.parseInt(blockId);
                    int blockVer_int = Integer.parseInt(blockVer);

                    File existFile = table.get(blockId_int);
                    if(existFile == null) {
                        table.put(blockId_int, file);
                    } else {
                        String existFilename = existFile.getName();
                        int existBlockVer_int = 0;
                        int existDotidx = existFilename.indexOf(".");
                        if(existDotidx > 0) {
                            String existBlockVer = existFilename.substring(existDotidx+1);
                            existBlockVer_int = Integer.parseInt(existBlockVer);
                        }

                        if(existBlockVer_int <= blockVer_int) {
                            // remove old
                            table.remove(blockId_int);
                            // add new
                            table.put(blockId_int, file);
                        }
                    }
                }
            }

            return table;
        } else {
            throw new IOException("directory not exists : " + cachedFilePath);
        }
    }
    
    public synchronized InputStream readFileDataBlockInputStream(int blockID) throws IOException {
        if(blockID < 0) {
            throw new IllegalArgumentException("blockID must be positive");
        }
        
        if(this.localFileSystem && this.cachedBlocks != null) {
            // check the cached file block is present
            File cachedBlockFile = this.cachedBlocks.get(blockID);
            if(cachedBlockFile != null) {
                // has cache
                if(cachedBlockFile.exists()) {
                    return new FileInputStream(cachedBlockFile);
                }
            }
        }
        
        SyndicateUGRestClient client = this.filesystem.getUGRestClient();
        Future<ClientResponse> readFuture = client.read(this.status.getPath().getPath(), this.fileinfo, BlockUtils.getBlockStartOffset(blockID, this.blockSize), (int) this.blockSize);
        if(readFuture != null) {
            InputStream readIS = client.processRead(readFuture);
            if(readIS == null) {
                LOG.error("failed to read file - " + this.status.getPath().getPath());
                throw new IOException("failed to read file - " + this.status.getPath().getPath());
            }
            
            return readIS;
        } else {
            LOG.error("failed to read file - " + this.status.getPath().getPath());
            throw new IOException("failed to read file - " + this.status.getPath().getPath());
        }
    }
    
    public synchronized SyndicateFSReadBlockData readFileDataBlock(int blockID) throws IOException {
        InputStream is = readFileDataBlockInputStream(blockID);
        
        return new SyndicateFSReadBlockData(BlockUtils.getBlockStartOffset(blockID, this.blockSize), is, (int) this.blockSize);
    }
    
    public synchronized void writeFileDataBlockInputStream(int blockID, InputStream is, int size) throws IOException {
        if(blockID < 0) {
            throw new IllegalArgumentException("blockID must be positive");
        }
        
        if(this.readonly) {
            throw new IOException("Can not write data to readonly handle");
        }
        
        SyndicateUGRestClient client = this.filesystem.getUGRestClient();
        Future<ClientResponse> writeFuture = client.write(this.status.getPath().getPath(), this.fileinfo, BlockUtils.getBlockStartOffset(blockID, this.blockSize), size, is);
        if(writeFuture != null) {
            client.processWrite(writeFuture);
        } else {
            LOG.error("failed to write file - " + this.status.getPath().getPath());
            throw new IOException("failed to write file - " + this.status.getPath().getPath());
        }
        
        if(this.status.getSize() <= BlockUtils.getBlockStartOffset(blockID, this.blockSize) + size) {
            this.status.notifySizeChanged(BlockUtils.getBlockStartOffset(blockID, this.blockSize) + size);
        }
        
        this.modified = true;
    }
    
    public synchronized void writeFileDataBlockByteArray(int blockID, byte[] buffer, int size) throws IOException {
        InputStream is = new ByteArrayInputStream(buffer, 0, size);
        writeFileDataBlockInputStream(blockID, is, size);
        IOUtils.closeQuietly(is);
    }
    
    public synchronized boolean isOpen() {
        return !this.closed;
    }
    
    public synchronized boolean isReadonly() {
        return this.readonly;
    }
    
    @Override
    public synchronized void close() throws IOException {
        if(!this.closed) {
            Future<ClientResponse> closeFuture = this.filesystem.getUGRestClient().close(this.status.getPath().getPath(), this.fileinfo);
            if(closeFuture != null) {
                this.filesystem.getUGRestClient().processClose(closeFuture);
            } else {
                LOG.error("failed to close file - " + this.status.getPath().getPath());
                throw new IOException("failed to close file - " + this.status.getPath().getPath());
            }

            this.closed = true;

            if(!this.readonly) {
                if(this.modified) {
                    this.status.setDirty();
                }
            }

            if(this.cachedBlocks != null) {
                this.cachedBlocks.clear();
            }
        }
    }
}
