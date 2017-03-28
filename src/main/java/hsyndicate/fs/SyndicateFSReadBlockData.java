/*
   Copyright 2016 The Trustees of University of Arizona

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

import hsyndicate.utils.IOUtils;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class SyndicateFSReadBlockData implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(SyndicateFSReadBlockData.class);
    
    private long offset;
    private InputStream inputStream;
    private int blockSize;
    private byte[] bufferredData;
    private int bufferredSize;
    private boolean fullyBufferred;
    
    public SyndicateFSReadBlockData(long offset, byte[] buffer, int blockSize) {
        this.offset = offset;
        this.inputStream = null;
        this.blockSize = blockSize;
        this.bufferredData = new byte[blockSize];
        System.arraycopy(buffer, 0, this.bufferredData, 0, Math.min(buffer.length, blockSize));
        this.bufferredSize = buffer.length;
        this.fullyBufferred = true;
    }
    
    public SyndicateFSReadBlockData(long offset, InputStream is, int blockSize) {
        this.offset = offset;
        this.inputStream = is;
        this.blockSize = blockSize;
        this.bufferredData = new byte[blockSize];
        this.bufferredSize = 0;
        this.fullyBufferred = false;
    }
    
    public long getOffset() {
        return this.offset;
    }
    
    public long getBlockLength() {
        return this.blockSize;
    }
    
    private synchronized int ensureLoadData(int destOffset) throws IOException {
        if(this.fullyBufferred) {
            return this.bufferredSize;
        } else if(this.bufferredSize >= destOffset) {
            return this.bufferredSize;
        } else {
            if(this.inputStream != null) {
                int toRead = destOffset - this.bufferredSize;
                int totalReadLen = 0;
                while(totalReadLen < toRead) {
                    int readLen = IOUtils.read(this.inputStream, this.bufferredData, this.bufferredSize, destOffset - this.bufferredSize);
                    if(readLen > 0) {
                        this.bufferredSize += readLen;
                        totalReadLen += readLen;
                        
                        // Need to close inputstream when all data is consumed.
                        if(this.bufferredSize >= this.blockSize) {
                            IOUtils.closeQuietly(this.inputStream);
                            this.fullyBufferred = true;
                            break;
                        }
                    } else {
                        IOUtils.closeQuietly(this.inputStream);
                        this.fullyBufferred = true;
                        break;
                    }
                }
            }
            
            return this.bufferredSize;
        }
    }
    
    public synchronized int getData(int offset, byte[] buffer, int length) throws IOException {
        if(offset + length > this.blockSize) {
            throw new IOException(String.format("cannot read requested size of data : off(%d) len(%d) > blockSize(%d)", offset, length, this.blockSize));
        }
        
        ensureLoadData(offset + length);
        
        int toCopy = Math.min(this.bufferredSize - offset, length);
        System.arraycopy(this.bufferredData, offset, buffer, 0, toCopy);
        return toCopy;
    }
    
    public synchronized int getData(int offset, byte[] buffer, int bufferStartOffset, int length) throws IOException {
        if(offset + length > this.blockSize) {
            throw new IOException(String.format("cannot read requested size of data : off(%d) len(%d) > blockSize(%d)", offset, length, this.blockSize));
        }
        
        ensureLoadData(offset + length);
        
        int toCopy = Math.min(this.bufferredSize - offset, length);
        System.arraycopy(this.bufferredData, offset, buffer, bufferStartOffset, toCopy);
        return toCopy;
    }
    
    public synchronized byte[] getData() throws IOException {
        int dataSize = ensureLoadData(this.blockSize);
        
        byte[] arr = new byte[dataSize];
        System.arraycopy(this.bufferredData, 0, arr, 0, this.bufferredSize);
        return arr;
    }
    
    public synchronized int getBufferredSize() {
        return this.bufferredSize;
    }

    @Override
    public synchronized void close() throws IOException {
        if(!this.fullyBufferred) {
            if(this.inputStream != null) {
                IOUtils.closeQuietly(this.inputStream);
            }
        }
    }
}
