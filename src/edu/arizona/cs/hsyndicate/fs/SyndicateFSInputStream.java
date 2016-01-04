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

import edu.arizona.cs.hsyndicate.util.BlockUtils;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SyndicateFSInputStream extends InputStream {

    private static final Log LOG = LogFactory.getLog(SyndicateFSInputStream.class);
    
    private SyndicateFSFileHandle handle;
    private long offset;
    private long size;
    private boolean closed;
    private Map<Integer, SyndicateFSReadBlockData> cachedBlockData = new LRUMap<Integer, SyndicateFSReadBlockData>(10);
    
    SyndicateFSInputStream(SyndicateFSFileHandle handle) {
        this.handle = handle;
        
        this.offset = 0;
        this.size = handle.getStatus().getSize();
        this.closed = false;
    }
    
    public long getPos() throws IOException {
        if(this.closed) {
            LOG.error("InputStream is already closed");
            throw new IOException("InputStream is already closed");
        }
        
        return this.offset;
    }
    
    public void seek(long l) throws IOException {
        if(this.closed) {
            LOG.error("InputStream is already closed");
            throw new IOException("InputStream is already closed");
        }
        
        if(l < 0) {
            throw new IOException("cannot seek to negative offset : " + l);
        }
        
        if(l >= this.size) {
            this.offset = this.size;
        } else {
            this.offset = l;
        }
    }
    
    @Override
    public long skip(long l) throws IOException {
        if(this.closed) {
            LOG.error("InputStream is already closed");
            throw new IOException("InputStream is already closed");
        }
        
        if(l <= 0) {
            return 0;
        }
        
        if(this.offset >= this.size) {
            return 0;
        }
        
        long lavailable = this.size - this.offset;
        if(l >= lavailable) {
            this.offset = this.size;
            return lavailable;
        } else {
            this.offset += l;
            return l;
        }
    }
    
    @Override
    public synchronized int available() throws IOException {
        if(this.closed) {
            LOG.error("InputStream is already closed");
            throw new IOException("InputStream is already closed");
        }
        
        int blockSize = (int) this.handle.getStatus().getBlockSize();
        int blockID = BlockUtils.getBlockID(this.offset, blockSize);
        int blockOffset = (int) (this.offset - BlockUtils.getBlockStartOffset(blockID, blockSize));
        
        SyndicateFSReadBlockData blockData = this.cachedBlockData.get(blockID);
        if(blockData != null) {
            if(blockData.getBufferredSize() - blockOffset == 0) {
                return blockSize - blockOffset;
            } else {
                return blockData.getBufferredSize() - blockOffset;
            }
        } else {
            return blockSize - blockOffset;
        }
    }
    
    @Override
    public synchronized int read() throws IOException {
        if(this.closed) {
            LOG.error("InputStream is already closed");
            throw new IOException("InputStream is already closed");
        }
        
        int readLen = (int) Math.min(this.handle.getStatus().getSize() - this.offset, 1);
        if(readLen > 0) {
            byte[] buffer = new byte[1];
            
            int blockSize = (int) this.handle.getStatus().getBlockSize();
            int blockID = BlockUtils.getBlockID(this.offset, blockSize);
            int blockOffset = (int) (this.offset - BlockUtils.getBlockStartOffset(blockID, blockSize));

            SyndicateFSReadBlockData blockData = this.cachedBlockData.get(blockID);
            if(blockData == null) {
                blockData = this.handle.readFileDataBlock(blockID);
                this.cachedBlockData.put(blockID, blockData);
            }

            int read = blockData.getData(blockOffset, buffer, readLen);
            if(read <= 0) {
                // EOF
                return -1;
            }

            this.offset += read;
            return buffer[0];
        } else {
            // EOF
            return -1;
        }
    }
    
    @Override
    public synchronized int read(byte[] bytes) throws IOException {
        if(this.closed) {
            LOG.error("InputStream is already closed");
            throw new IOException("InputStream is already closed");
        }
        
        int readLen = (int) Math.min(this.handle.getStatus().getSize() - this.offset, bytes.length);
        if(readLen > 0) {
            int totalReadLen = 0;
            int blockSize = (int) this.handle.getStatus().getBlockSize();
            
            while(totalReadLen < readLen) {
                int blockID = BlockUtils.getBlockID(this.offset, blockSize);
                int blockOffset = (int) (this.offset - BlockUtils.getBlockStartOffset(blockID, blockSize));
            
                SyndicateFSReadBlockData blockData = this.cachedBlockData.get(blockID);
                if(blockData == null) {
                    blockData = this.handle.readFileDataBlock(blockID);
                    this.cachedBlockData.put(blockID, blockData);
                }
                
                int toCopy = Math.min(totalReadLen - readLen, blockSize);
                int read = blockData.getData(blockOffset, bytes, totalReadLen, toCopy);
                if(read <= 0) {
                    // EOF
                    break;
                }

                this.offset += read;
                totalReadLen += read;
            }
            return totalReadLen;
        } else {
            // EOF
            return -1;
        }
    }
    
    @Override
    public synchronized int read(byte[] bytes, int off, int len) throws IOException {
        if(this.closed) {
            LOG.error("InputStream is already closed");
            throw new IOException("InputStream is already closed");
        }
        
        int readLen = (int) Math.min(this.handle.getStatus().getSize() - this.offset, len);
        if(readLen > 0) {
            int totalReadLen = 0;
            int blockSize = (int) this.handle.getStatus().getBlockSize();
            
            while(totalReadLen < readLen) {
                int blockID = BlockUtils.getBlockID(this.offset, blockSize);
                int blockOffset = (int) (this.offset - BlockUtils.getBlockStartOffset(blockID, blockSize));
            
                SyndicateFSReadBlockData blockData = this.cachedBlockData.get(blockID);
                if(blockData == null) {
                    blockData = this.handle.readFileDataBlock(blockID);
                    this.cachedBlockData.put(blockID, blockData);
                }
                
                int toCopy = Math.min(totalReadLen - readLen, blockSize);
                int read = blockData.getData(blockOffset, bytes, off + totalReadLen, toCopy);
                if(read <= 0) {
                    // EOF
                    break;
                }

                this.offset += read;
                totalReadLen += read;
            }
            return totalReadLen;
        } else {
            // EOF
            return -1;
        }
    }
    
    @Override
    public synchronized void close() throws IOException {
        if(!this.closed) {
            this.handle.close();
            this.handle.getFileSystem().notifyInputStreamClosed(this);

            for(Map.Entry<Integer, SyndicateFSReadBlockData> entry : this.cachedBlockData.entrySet()) {
                SyndicateFSReadBlockData blockData = entry.getValue();
                blockData.close();
            }

            this.cachedBlockData.clear();
        }
        this.closed = true;
    }
    
    @Override
    public synchronized void mark(int readlimit) {
    }
    
    @Override
    public synchronized void reset() throws IOException {
    }
    
    @Override
    public synchronized boolean markSupported() {
        return false;
    }
}
