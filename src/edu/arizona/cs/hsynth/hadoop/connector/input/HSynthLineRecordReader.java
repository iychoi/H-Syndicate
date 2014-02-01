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

import edu.arizona.cs.hsynth.fs.HSynthFSConfiguration;
import edu.arizona.cs.hsynth.fs.HSynthFSPath;
import edu.arizona.cs.hsynth.fs.HSynthFileSystem;
import edu.arizona.cs.hsynth.hadoop.util.HSynthCompressionCodecUtil;
import edu.arizona.cs.hsynth.hadoop.util.HSynthConfigUtil;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;

public class HSynthLineRecordReader extends RecordReader<LongWritable, Text> {

    private static final Log LOG = LogFactory.getLog(HSynthLineRecordReader.class);
    
    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private int maxLineLength;
    private LongWritable key = null;
    private Text value = null;
    private byte[] recordDelimiterBytes;
    
    public HSynthLineRecordReader() {
    }

    public HSynthLineRecordReader(byte[] recordDelimiter) {
        this.recordDelimiterBytes = recordDelimiter;
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        if (!(genericSplit instanceof HSynthInputSplit)) {
            throw new IllegalArgumentException("Creation of a new RecordReader requires a HSynthInputSplit instance.");
        }

        HSynthInputSplit split = (HSynthInputSplit) genericSplit;
        Configuration conf = context.getConfiguration();

        this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        this.start = split.getStart();
        this.end = this.start + split.getLength();

        final HSynthFSPath path = split.getPath();
        compressionCodecs = new CompressionCodecFactory(conf);
        final CompressionCodec codec = HSynthCompressionCodecUtil.getCompressionCodec(this.compressionCodecs, path);
        
        HSynthFileSystem fs = null;
        try {
            HSynthFSConfiguration hconf = HSynthConfigUtil.getHSynthFSConfigurationInstance(context.getConfiguration());
            fs = hconf.getContext().getFileSystem();
        } catch (InstantiationException ex) {
            throw new IOException(ex);
        }
        
        // check file modification
        //if(!fs.getFileVersion(path).equals(split.getFileVersion())) {
        //    throw new IOException("File has been modified while Hadoop is running");
        //}

        InputStream is = fs.getFileInputStream(path);
        
        boolean skipFirstLine = false;
        
        if (codec != null) {
            if (this.recordDelimiterBytes == null) {
                this.in = new LineReader(codec.createInputStream(is), conf);
            } else {
                this.in = new LineReader(codec.createInputStream(is), conf, this.recordDelimiterBytes);
            }
            
            this.end = Long.MAX_VALUE;
        } else {
            if (this.start != 0) {
                skipFirstLine = true;
                --this.start;
                is.skip(this.start);
                //is.seek(this.start);
            }
            
            if (this.recordDelimiterBytes == null) {
                this.in = new LineReader(is, conf);
            } else {
                this.in = new LineReader(is, conf, this.recordDelimiterBytes);
            }
        }
        
        if (skipFirstLine) {
            // skip first line and re-establish "start".
            this.start += this.in.readLine(new Text(), 0, (int) Math.min((long) Integer.MAX_VALUE, this.end - this.start));
        }
        this.pos = this.start;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (this.key == null) {
            this.key = new LongWritable();
        }

        this.key.set(this.pos);

        if (this.value == null) {
            this.value = new Text();
        }

        int newSize = 0;
        while (this.pos < this.end) {
            newSize = this.in.readLine(this.value, this.maxLineLength, Math.max((int) Math.min(Integer.MAX_VALUE, this.end - this.pos), this.maxLineLength));
            if (newSize == 0) {
                break;
            }

            this.pos += newSize;
            if (newSize < this.maxLineLength) {
                break;
            }

            // line too long. try again
            LOG.info("Skipped line of size " + newSize + " at pos " + (this.pos - newSize));
        }

        if (newSize == 0) {
            this.key = null;
            this.value = null;
            return false;
        } else {
            return true;
        }
    }
    
    @Override
    public synchronized void close() throws IOException {
        if (this.in != null) {
            this.in.close();
        }
    }

    @Override
    public float getProgress() throws IOException {
        if (this.start == this.end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (this.pos - this.start) / (float) (this.end - this.start));
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }
}
