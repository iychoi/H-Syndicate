package edu.arizona.cs.hsynth.fs;

import java.io.Closeable;
import java.io.IOException;

public interface HSynthFSRandomAccess extends Closeable {
    
    public int read() throws IOException;
    
    public int read(byte[] bytes) throws IOException;
    
    public int read(byte[] bytes, int off, int len) throws IOException;
    
    public int skip(int n) throws IOException;
    
    public long getFilePointer() throws IOException;
    
    public long length() throws IOException;
    
    public void seek(long l) throws IOException;
    
    @Override
    public void close() throws IOException;
}
