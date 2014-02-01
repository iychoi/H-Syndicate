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
package edu.arizona.cs.hsynth.hadoop.connector.output;

import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.fs.Syncable;

public class HSynthFSDataOutputStream extends DataOutputStream implements Syncable {

    private OutputStream wrappedStream;

    private static class PositionCache extends FilterOutputStream {

        long position;

        public PositionCache(OutputStream out, long pos) throws IOException {
            super(out);
            position = pos;
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            position++;
        }

        @Override
        public void write(byte b[], int off, int len) throws IOException {
            out.write(b, off, len);
            position += len;                            // update position
        }

        public long getPos() throws IOException {
            return position;                            // return cached position
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }

    public HSynthFSDataOutputStream(OutputStream out) throws IOException {
        this(out, 0);
    }

    public HSynthFSDataOutputStream(OutputStream out, long startPosition) throws IOException {
        super(new PositionCache(out, startPosition));
        this.wrappedStream = out;
    }

    public long getPos() throws IOException {
        return ((PositionCache) out).getPos();
    }

    public void close() throws IOException {
        out.close();         // This invokes PositionCache.close()
    }

    /**
     * {@inheritDoc}
     */
    public void sync() throws IOException {
        if (wrappedStream instanceof Syncable) {
            ((Syncable) wrappedStream).sync();
        }
    }
}
