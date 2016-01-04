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
package edu.arizona.cs.hsyndicate.hadoop.connector.input;

import edu.arizona.cs.hsyndicate.fs.SyndicateFSInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

public class HSyndicateFSSeekableInputStream extends InputStream implements Seekable, PositionedReadable {

    private SyndicateFSInputStream is;
    
    public HSyndicateFSSeekableInputStream(SyndicateFSInputStream is) {
        this.is = is;
    }

    @Override
    public int read() throws IOException {
        return this.is.read();
    }
    
    @Override
    public int read(byte[] bytes) throws IOException {
        return this.is.read(bytes);
    }
    
    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        return this.is.read(bytes, off, len);
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
        return this.is.skip((int)n);
    }
    
    @Override
    public void seek(long l) throws IOException {
        this.is.seek(l);
    }

    @Override
    public long getPos() throws IOException {
        return this.is.getPos();
    }
    
    @Override
    public int available() throws IOException {
        return this.is.available();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }
    
    @Override
    public void close() throws IOException {
        this.is.close();
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
