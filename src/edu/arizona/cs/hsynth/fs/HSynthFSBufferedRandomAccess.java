package edu.arizona.cs.hsynth.fs;

import edu.arizona.cs.hsynth.fs.cache.TimeoutCache;
import java.io.FileNotFoundException;
import java.io.IOException;

public class HSynthFSBufferedRandomAccess implements HSynthFSRandomAccess {

    private HSynthFSRandomAccess raf;
    private HSynthFileSystem filesystem;
    private long bufferSize;
    
    private TimeoutCache<Long, Block> cachedBlocks;
    
    private long offset;
    private long length;
    
    public HSynthFSBufferedRandomAccess(HSynthFileSystem filesystem, HSynthFSRandomAccess raf, long bufferSize) throws FileNotFoundException, IOException {
        this.filesystem = filesystem;
        this.raf = raf;
        this.bufferSize = bufferSize;
        
        this.cachedBlocks = new TimeoutCache<Long, Block>(10, 0);
        this.offset = this.raf.getFilePointer();
        this.length = this.raf.length();
    }
    
    @Override
    public void close() throws IOException {
        this.cachedBlocks.clear();
        this.raf.close();
    }
    
    private long getBlockStart(long offset) {
        return (offset / this.bufferSize) * this.bufferSize;
    }
    
    private Block loadBlock(long offset) throws IOException {
        long blockStart = getBlockStart(offset);
        Block cachedblock = this.cachedBlocks.get(blockStart);
        if(cachedblock == null) {
            cachedblock = new Block(blockStart, (int) this.bufferSize);
            cachedblock.load(this.raf);
            this.cachedBlocks.insert(blockStart, cachedblock);
        }
        return cachedblock;
    }
    
    @Override
    public int read() throws IOException {
        Block currentBlock = loadBlock(this.offset);
        int read = currentBlock.read(this.offset);
        if(read >= 0) {
            this.offset++;
        }
        return read;
    }

    @Override
    public int read(byte[] bytes) throws IOException {
        int remain = (int) Math.min(this.length - this.offset, bytes.length);
        int readTotal = 0;
        while(remain > 0) {
            Block currentBlock = loadBlock(this.offset);
            int read = currentBlock.read(this.offset, bytes, readTotal, remain);
            readTotal += read;
            remain -= read;
            this.offset += read;
        }
        return readTotal;
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        int remain = (int) Math.min(this.length - this.offset, len);
        int readTotal = 0;
        while(remain > 0) {
            Block currentBlock = loadBlock(this.offset);
            int read = currentBlock.read(this.offset, bytes, readTotal + off, remain);
            readTotal += read;
            remain -= read;
            this.offset += read;
        }
        return readTotal;
    }

    @Override
    public int skip(int n) throws IOException {
        if(this.length - this.offset > n) {
            this.offset += n;
            return n;
        } else {
            int avail = (int)(this.length - this.offset);
            this.offset += avail;
            return avail;
        }
    }

    @Override
    public long getFilePointer() throws IOException {
        return this.offset;
    }

    @Override
    public long length() throws IOException {
        return this.length;
    }

    @Override
    public void seek(long l) throws IOException {
        if(this.length > l) {
            this.offset = l;
        } else {
            this.offset = this.length;
        }
    }

    private static class Block {
        private long blockStart = 0;
        private int blockSize;
        private byte[] blockBuffer;
        private int currentBlockLen = 0;
        
        Block(long blockStart, int blockSize) {
            this.blockStart = blockStart;
            this.blockSize = blockSize;
        }
        
        public boolean hasData() {
            return this.currentBlockLen > 0;
        }
        
        public void load(HSynthFSRandomAccess raf) throws IOException {
            if(raf.getFilePointer() != this.blockStart) {
                raf.seek(this.blockStart);
            }
            
            this.currentBlockLen = 0;
            long canRead = raf.length() - raf.getFilePointer();
            if(canRead <= 0) {
                return;
            }
            
            int readSize = (int) Math.min(this.blockSize, canRead);
            this.blockBuffer = new byte[readSize];
            int readLen = 0;
            while(readSize - this.currentBlockLen > 0) {
                readLen = raf.read(this.blockBuffer, this.currentBlockLen, readSize - this.currentBlockLen);
                this.currentBlockLen += readLen;
            }
        }
        
        public long getBlockStart() {
            return this.blockStart;
        }
        
        public int getBlockSize() {
            return this.blockSize;
        }
        
        public int getCurrentBlockLength() {
            return this.currentBlockLen;
        }
        
        // read block bytes
        public int read(long position) throws IOException {
            int blockPosition = (int)(position - this.blockStart);
            if(blockPosition >= this.currentBlockLen) {
                throw new IOException("Given offset is not in the block range");
            }
            return this.blockBuffer[blockPosition] & 0xff;
        }

        public int read(long position, byte[] bytes) throws IOException {
            int blockPosition = (int)(position - this.blockStart);
            if(blockPosition >= this.currentBlockLen) {
                throw new IOException("Given offset is not in the block range");
            }
            int readLen = Math.min(this.currentBlockLen - blockPosition, bytes.length);
            System.arraycopy(this.blockBuffer, (int)(position - this.blockStart), bytes, 0, readLen);
            return readLen;
        }

        public int read(long position, byte[] bytes, int off, int len) throws IOException {
            int blockPosition = (int)(position - this.blockStart);
            if(blockPosition >= this.currentBlockLen) {
                throw new IOException("Given offset is not in the block range");
            }
            int readLen = Math.min(this.currentBlockLen - blockPosition, len);
            System.arraycopy(this.blockBuffer, (int)(position - this.blockStart), bytes, off, readLen);
            return readLen;
        }
    }
}
