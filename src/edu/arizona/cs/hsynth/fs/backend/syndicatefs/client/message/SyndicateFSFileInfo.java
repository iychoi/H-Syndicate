package edu.arizona.cs.hsynth.fs.backend.syndicatefs.client.message;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SyndicateFSFileInfo {
    
    private /* uint64_t */ long handle;

    SyndicateFSFileInfo() {
        this.handle = 0;
    }

    public long getFileHandle() {
        return handle;
    }

    public void setFileHandle(long handle) {
        this.handle = handle;
    }
    
    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(getFieldSize());
        buffer.order(ByteOrder.BIG_ENDIAN);
        
        buffer.putLong(this.handle);
        
        return buffer.array();
    }
    
    public void fromBytes(byte[] bytes, int offset, int len) {
        ByteBuffer buffer = ByteBuffer.allocate(getFieldSize());
        buffer.order(ByteOrder.BIG_ENDIAN);
        
        buffer.put(bytes, offset, len);
        
        buffer.flip();
        this.handle = buffer.getLong();
    }
    
    public int getFieldSize() {
        return 8;
    }
}
