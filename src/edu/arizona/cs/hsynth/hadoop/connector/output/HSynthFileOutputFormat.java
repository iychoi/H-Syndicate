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

import edu.arizona.cs.syndicate.fs.SyndicateFSPath;
import edu.arizona.cs.syndicate.fs.ASyndicateFileSystem;
import edu.arizona.cs.syndicate.fs.SyndicateFSConfiguration;
import java.io.IOException;
import java.text.NumberFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.hsynth.util.SyndicateFileSystemFactory;
import org.apache.hadoop.fs.hsynth.util.HSynthConfigUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;

public abstract class HSynthFileOutputFormat<K, V> extends OutputFormat<K, V> {

    private static final Log LOG = LogFactory.getLog(HSynthFileOutputFormat.class);
    
    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
    protected static final String CONF_BASE_OUTPUT_NAME = "mapreduce.output.basename";
    protected static final String PART = "part";
    private static final String CONF_OUTPUT_COMPRESS = "mapred.output.compress";
    private static final String CONF_OUTPUT_COMPRESS_CODEC = "mapred.output.compression.codec";
    private static final String CONF_OUTPUT_DIR = "mapred.output.dir";
    
    static {
        NUMBER_FORMAT.setMinimumIntegerDigits(5);
        NUMBER_FORMAT.setGroupingUsed(false);
    }
    
    private HSynthOutputCommitter committer = null;
    
    public static void setCompressOutput(Job job, boolean compress) {
        job.getConfiguration().setBoolean(CONF_OUTPUT_COMPRESS, compress);
    }
    
    public static boolean getCompressOutput(JobContext job) {
        return job.getConfiguration().getBoolean(CONF_OUTPUT_COMPRESS, false);
    }
    
    public static void setOutputCompressorClass(Job job, Class<? extends CompressionCodec> codecClass) {
        setCompressOutput(job, true);
        job.getConfiguration().setClass(CONF_OUTPUT_COMPRESS_CODEC, codecClass, CompressionCodec.class);
    }
    
    public static Class<? extends CompressionCodec> getOutputCompressorClass(JobContext job, Class<? extends CompressionCodec> defaultValue) {
        Class<? extends CompressionCodec> codecClass = defaultValue;
        Configuration conf = job.getConfiguration();
        String name = conf.get(CONF_OUTPUT_COMPRESS_CODEC);
        if (name != null) {
            try {
                codecClass = conf.getClassByName(name).asSubclass(CompressionCodec.class);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Compression codec " + name + " was not found.", e);
            }
        }
        return codecClass;
    }

    @Override
    public abstract RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException;
    
    @Override
    public void checkOutputSpecs(JobContext context) throws FileAlreadyExistsException, IOException, InterruptedException {
        SyndicateFSPath outDir = getOutputPath(context);
        if (outDir == null) {
            throw new InvalidJobConfException("Output directory not set.");
        }
        
        ASyndicateFileSystem fs = null;
        try {
            SyndicateFSConfiguration sconf = HSynthConfigUtils.createSyndicateConf(context.getConfiguration(), "localhost");
            fs = SyndicateFileSystemFactory.getInstance(sconf);
        } catch (InstantiationException ex) {
            throw new IOException(ex);
        }
        
        if(fs.exists(outDir)) {
            throw new FileAlreadyExistsException("Output directory " + outDir.toString() + " already exists");
        }
    }
    
    public static void setOutputPath(Job job, SyndicateFSPath outputDir) {
        job.getConfiguration().set(CONF_OUTPUT_DIR, outputDir.toString());
    }
    
    public static SyndicateFSPath getOutputPath(JobContext context) {
        String name = context.getConfiguration().get(CONF_OUTPUT_DIR);
        return name == null ? null : new SyndicateFSPath(name);
    }
    
    public synchronized static String getUniqueFile(TaskAttemptContext context, String name, String extension) {
        TaskID taskId = context.getTaskAttemptID().getTaskID();
        int partition = taskId.getId();
        StringBuilder result = new StringBuilder();
        result.append(name);
        result.append('-');
        result.append(taskId.isMap() ? 'm' : 'r');
        result.append('-');
        result.append(NUMBER_FORMAT.format(partition));
        result.append(extension);
        return result.toString();
    }

    public SyndicateFSPath getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
        HSynthOutputCommitter committer = (HSynthOutputCommitter) getOutputCommitter(context);
        Configuration conf = context.getConfiguration();
        String uniquename = getUniqueFile(context, getOutputName(context), extension);
        return new SyndicateFSPath(committer.getOutputPath(), uniquename);
    }
    
    protected static String getOutputName(JobContext job) {
        return job.getConfiguration().get(CONF_BASE_OUTPUT_NAME, PART);
    }
    
    protected static void setOutputName(JobContext job, String name) {
        job.getConfiguration().set(CONF_BASE_OUTPUT_NAME, name);
    }
    
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
        if (this.committer == null) {
            SyndicateFSPath output = getOutputPath(context);
            
            ASyndicateFileSystem fs = null;
            try {
                SyndicateFSConfiguration sconf = org.apache.hadoop.fs.hsynth.util.HSynthConfigUtils.createSyndicateConf(context.getConfiguration(), "localhost");
                fs = SyndicateFileSystemFactory.getInstance(sconf);
            } catch (InstantiationException ex) {
                throw new IOException(ex);
            }
            
            this.committer = new HSynthOutputCommitter(fs, output, context);
        }
        return this.committer;
    }
}
