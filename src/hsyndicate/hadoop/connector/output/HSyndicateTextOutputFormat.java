/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package hsyndicate.hadoop.connector.output;

import hsyndicate.fs.SyndicateFSPath;
import hsyndicate.fs.AHSyndicateFileSystemBase;
import hsyndicate.fs.SyndicateFSConfiguration;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import org.apache.hadoop.conf.Configuration;
import hsyndicate.fs.SyndicateFileSystemFactory;
import hsyndicate.hadoop.utils.HSyndicateConfigUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

public class HSyndicateTextOutputFormat<K, V> extends HSyndicateFileOutputFormat<K, V> {

    protected static class LineRecordWriter<K, V> extends RecordWriter<K, V> {

        private static final String utf8 = "UTF-8";
        private static final byte[] newline;

        static {
            try {
                newline = "\n".getBytes(utf8);
            } catch (UnsupportedEncodingException uee) {
                throw new IllegalArgumentException("can't find " + utf8 + " encoding");
            }
        }
        protected DataOutputStream out;
        private final byte[] keyValueSeparator;

        public LineRecordWriter(DataOutputStream out, String keyValueSeparator) {
            this.out = out;
            try {
                this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
            } catch (UnsupportedEncodingException uee) {
                throw new IllegalArgumentException("can't find " + utf8 + " encoding");
            }
        }

        public LineRecordWriter(DataOutputStream out) {
            this(out, "\t");
        }

        /**
         * Write the object to the byte stream, handling Text as a special case.
         *
         * @param o the object to print
         * @throws IOException if the write throws, we pass it on
         */
        private void writeObject(Object o) throws IOException {
            if (o instanceof Text) {
                Text to = (Text) o;
                out.write(to.getBytes(), 0, to.getLength());
            } else {
                out.write(o.toString().getBytes(utf8));
            }
        }

        public synchronized void write(K key, V value)
                throws IOException {

            boolean nullKey = key == null || key instanceof NullWritable;
            boolean nullValue = value == null || value instanceof NullWritable;
            if (nullKey && nullValue) {
                return;
            }
            if (!nullKey) {
                writeObject(key);
            }
            if (!(nullKey || nullValue)) {
                out.write(keyValueSeparator);
            }
            if (!nullValue) {
                writeObject(value);
            }
            out.write(newline);
        }

        public synchronized void close(TaskAttemptContext context) throws IOException {
            out.close();
        }
    }

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        boolean isCompressed = getCompressOutput(context);
        String keyValueSeparator = conf.get("mapred.textoutputformat.separator", "\t");
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(context, GzipCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
            extension = codec.getDefaultExtension();
        }

        SyndicateFSPath path = getDefaultWorkFile(context, extension);
        AHSyndicateFileSystemBase fs = null;
        try {
            SyndicateFSConfiguration sconf = HSyndicateConfigUtils.createSyndicateConf(conf, "localhost");
            fs = SyndicateFileSystemFactory.getInstance(sconf);
        } catch (InstantiationException ex) {
            throw new IOException(ex);
        }

        if (!isCompressed) {
            DataOutputStream fileOut = new DataOutputStream(fs.getFileOutputStream(path));
            return new LineRecordWriter<K, V>(fileOut, keyValueSeparator);
        } else {
            DataOutputStream fileOut = new DataOutputStream(fs.getFileOutputStream(path));
            return new LineRecordWriter<K, V>(new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator);
        }
    }
}
