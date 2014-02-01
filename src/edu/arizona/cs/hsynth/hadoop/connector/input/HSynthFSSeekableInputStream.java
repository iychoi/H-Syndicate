/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.arizona.cs.hsynth.hadoop.connector.input;

import edu.arizona.cs.hsynth.fs.HSynthFSRandomAccess;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

public class HSynthFSSeekableInputStream extends InputStream implements Seekable, PositionedReadable {

    private HSynthFSRandomAccess raf;
    private long length = 0;
    
    public HSynthFSSeekableInputStream(HSynthFSRandomAccess raf) {
        this.raf = raf;
        try {
            // for better performance
            this.length = raf.length();
        } catch (IOException ex) {}
    }

    @Override
    public int read() throws IOException {
        return this.raf.read();
    }
    
    @Override
    public int read(byte[] bytes) throws IOException {
        return this.raf.read(bytes);
    }
    
    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        return this.raf.read(bytes, off, len);
    }
    
    @Override
    public int read(long position, byte[] bytes, int off, int len) throws IOException {
        synchronized (this) {
            long oldPos = getPos();
            int nread = -1;
            try {
                seek(position);
                nread = read(bytes, off, len);
            } finally {
                seek(oldPos);
            }
            return nread;
        }
    }
    
    @Override
    public void readFully(long position, byte[] bytes, int off, int len) throws IOException {
        int nread = 0;
        while (nread < len) {
            int nbytes = read(position + nread, bytes, off + nread, len - nread);
            if (nbytes < 0) {
                throw new EOFException("End of file reached before reading fully.");
            }
            nread += nbytes;
        }
    }

    @Override
    public void readFully(long position, byte[] bytes) throws IOException {
        readFully(position, bytes, 0, bytes.length);
    }

    @Override
    public long skip(long n) throws IOException {
        return this.raf.skip((int)n);
    }
    
    @Override
    public void seek(long l) throws IOException {
        this.raf.seek(l);
    }

    @Override
    public long getPos() throws IOException {
        return this.raf.getFilePointer();
    }
    
    @Override
    public int available() throws IOException {
        if((this.length - getPos()) > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        } else {
            return (int)(this.length - getPos());
        }
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }
    
    @Override
    public void close() throws IOException {
        this.raf.close();
    }
    
    @Override
    public synchronized void mark(int readlimit) {
    }
    
    @Override
    public synchronized void reset() throws IOException {
    }
    
    @Override
    public boolean markSupported() {
        return false;
    }
}
