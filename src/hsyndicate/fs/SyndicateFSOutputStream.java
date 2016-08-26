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
package hsyndicate.fs;

import hsyndicate.utils.BlockUtils;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SyndicateFSOutputStream extends OutputStream {

    private static final Log LOG = LogFactory.getLog(SyndicateFSOutputStream.class);
    
    private SyndicateFSFileHandle handle;
    private long blockOffset;
    private int blockSize;
    private byte[] bufferredData;
    private int bufferredSize;
    private boolean closed;
    
    SyndicateFSOutputStream(SyndicateFSFileHandle handle) {
        this.handle = handle;
        
        this.blockOffset = 0;
        this.blockSize = (int) handle.getStatus().getBlockSize();
        this.bufferredData = new byte[this.blockSize];
        this.bufferredSize = 0;
    
        this.closed = false;
    }
    
    @Override
    public synchronized void write(int i) throws IOException {
        if(this.closed) {
            LOG.error("OutputStream is already closed");
            throw new IOException("OutputStream is already closed");
        }
        
        if(this.bufferredSize >= this.blockSize) {
            flush();
        }
        
        byte b = (byte)(i & 0xff);
        this.bufferredData[this.bufferredSize] = b;
        this.bufferredSize++;
        
        if(this.bufferredSize >= this.blockSize) {
            flush();
        }
    }
    
    @Override
    public synchronized void write(byte[] bytes) throws IOException {
        if(this.closed) {
            LOG.error("OutputStream is already closed");
            throw new IOException("OutputStream is already closed");
        }
        
        if(this.bufferredSize >= this.blockSize) {
            flush();
        }
        
        int toCopy = bytes.length;
        int copied = 0;
        while(copied < toCopy) {
            int left = Math.min(toCopy - copied, this.blockSize - this.bufferredSize);
            System.arraycopy(bytes, copied, this.bufferredData, this.bufferredSize, left);
            copied += left;
            this.bufferredSize += left;
            
            if(this.bufferredSize >= this.blockSize) {
                flush();
            }
        }
    }
    
    @Override
    public synchronized void write(byte[] bytes, int offset, int len) throws IOException {
        if(this.closed) {
            LOG.error("OutputStream is already closed");
            throw new IOException("OutputStream is already closed");
        }
        
        if(this.bufferredSize >= this.blockSize) {
            flush();
        }
        
        int toCopy = len;
        int copied = 0;
        while(copied < toCopy) {
            int left = Math.min(toCopy - copied, this.blockSize - this.bufferredSize);
            System.arraycopy(bytes, offset + copied, this.bufferredData, this.bufferredSize, left);
            copied += left;
            this.bufferredSize += left;
            
            if(this.bufferredSize >= this.blockSize) {
                flush();
            }
        }
    }
    
    @Override
    public synchronized void flush() throws IOException {
        if(this.closed) {
            LOG.error("OutputStream is already closed");
            throw new IOException("OutputStream is already closed");
        }
        
        if(this.bufferredSize > 0) {
            // flush stale data
            this.handle.writeFileDataBlockByteArray(BlockUtils.getBlockID(this.blockOffset, this.blockSize), this.bufferredData, this.bufferredSize);
            
            if(this.bufferredSize >= this.blockSize) {
                this.blockOffset += this.bufferredSize;
                this.bufferredSize = 0;
            }
        }
    }
    
    @Override
    public synchronized void close() throws IOException {
        if(!this.closed) {
            flush();

            this.handle.close();
            this.handle.getFileSystem().notifyOutputStreamClosed(this);
            this.closed = true;
        }
    }
}
