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
import edu.arizona.cs.hsynth.fs.HSynthFSPathFilter;
import edu.arizona.cs.hsynth.fs.HSynthFileSystem;
import edu.arizona.cs.hsynth.hadoop.util.HSynthConfigUtil;
import edu.arizona.cs.hsynth.util.StringUtil;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

public abstract class HSynthFileInputFormat<K extends Object, V extends Object> extends InputFormat<K, V> {

    private static final Log LOG = LogFactory.getLog(HSynthFileInputFormat.class);
    
    private static final double SPLIT_SLOP = 1.1;   // 10% slop
    
    private static final String CONF_PATH_FILTER = "hsynth.conf.input.pathFilter.class";
    private static final String CONF_MIN_SPLIT_SIZE = "hsynth.conf.input.min.split.size";
    private static final String CONF_MAX_SPLIT_SIZE = "hsynth.conf.input.max.split.size";
    private static final String CONF_NUM_INPUT_FILES = "mapreduce.input.num.files";
    private static final String CONF_INPUT_DIR = "mapred.input.dir";
    
    protected static class MultiPathFilter implements HSynthFSPathFilter {

        private List<HSynthFSPathFilter> filters;

        public MultiPathFilter(List<HSynthFSPathFilter> filters) {
            this.filters = filters;
        }

        @Override
        public boolean accept(HSynthFSPath path) {
            for (HSynthFSPathFilter filter : this.filters) {
                if (!filter.accept(path)) {
                    return false;
                }
            }
            return true;
        }
    }

    protected long getFormatMinSplitSize() {
        return 1;
    }
    
    protected boolean isSplitable(JobContext context, HSynthFSPath filename) {
        return true;
    }

    public static void setInputPathFilter(Job job, Class<? extends HSynthFSPathFilter> filter) {
        job.getConfiguration().setClass(CONF_PATH_FILTER, filter, HSynthFSPathFilter.class);
    }
    
    public static void setMinInputSplitSize(Job job, long size) {
        job.getConfiguration().setLong(CONF_MIN_SPLIT_SIZE, size);
    }
    
    public static long getMinSplitSize(JobContext job) {
        return job.getConfiguration().getLong(CONF_MIN_SPLIT_SIZE, 1L);
    }
    
    public static void setMaxInputSplitSize(Job job, long size) {
        job.getConfiguration().setLong(CONF_MAX_SPLIT_SIZE, size);
    }
    
    public static long getMaxSplitSize(JobContext context) {
        return context.getConfiguration().getLong(CONF_MAX_SPLIT_SIZE, Long.MAX_VALUE);
    }
    
    public static HSynthFSPathFilter getInputPathFilter(JobContext context) {
        Configuration conf = context.getConfiguration();
        Class<?> filterClass = conf.getClass(CONF_PATH_FILTER, null, HSynthFSPathFilter.class);
        return (filterClass != null) ? (HSynthFSPathFilter) ReflectionUtils.newInstance(filterClass, conf) : null;
    }
    
    protected List<HSynthFSPath> listFiles(JobContext context) throws IOException {
        List<HSynthFSPath> result = new ArrayList<HSynthFSPath>();
        HSynthFSPath[] dirs = getInputPaths(context);
        if(dirs == null || dirs.length == 0) {
            throw new IOException("No input paths specified in job");
        }
        
        /*
        LOG.info("input paths length : " + dirs.length);
        for(JSFSPath dir : dirs) {
            LOG.info("input path : " + dir.getPath());
        }
        */
        
        List<IOException> errors = new ArrayList<IOException>();
        
        List<HSynthFSPathFilter> filters = new ArrayList<HSynthFSPathFilter>();
        HSynthFSPathFilter jobFilter = getInputPathFilter(context);
        if (jobFilter != null) {
            filters.add(jobFilter);
        }
        HSynthFSPathFilter inputFilter = new MultiPathFilter(filters);
        
        HSynthFileSystem fs = null;
        try {
            HSynthFSConfiguration conf = HSynthConfigUtil.getHSynthFSConfigurationInstance(context.getConfiguration());
            fs = conf.getContext().getFileSystem();
        } catch (InstantiationException ex) {
            throw new IOException(ex);
        }
        
        for(int i=0;i<dirs.length;++i) {
            HSynthFSPath p = dirs[i];
            
            try {
                HSynthFSPath[] paths = fs.listAllFiles(p, inputFilter);
                if (paths != null) {
                    for(HSynthFSPath p2 : paths) {
                        result.add(p2);
                    }
                }
            } catch(FileNotFoundException ex) {
                errors.add(new IOException("Input path does not exist: " + p));
            } catch(IOException ex) {
                errors.add(ex);
            }
        }

        if (!errors.isEmpty()) {
            throw new InvalidInputException(errors);
        }
        LOG.info("Total input paths to process : " + result.size());
        return result;
    }
    
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(context));
        long maxSize = getMaxSplitSize(context);
        
        HSynthFileSystem fs = null;
        try {
            HSynthFSConfiguration conf = HSynthConfigUtil.getHSynthFSConfigurationInstance(context.getConfiguration());
            fs = conf.getContext().getFileSystem();
        } catch (InstantiationException ex) {
            throw new IOException(ex);
        }
        
        List<InputSplit> splits = new ArrayList<InputSplit>();
        List<HSynthFSPath> files = listFiles(context);
        for(HSynthFSPath path : files) {
            long length = fs.getSize(path);
            //String fileVersion = syndicateFS.getFileVersion(path);
            
            if ((length != 0) && isSplitable(context, path)) {
                long blockSize = fs.getBlockSize();
                long splitSize = computeSplitSize(blockSize, minSize, maxSize);
                long bytesLeft = length;
                while (((double) bytesLeft) / splitSize > SPLIT_SLOP) {
                    HSynthInputSplit inputSplit = new HSynthInputSplit(fs, path, length - bytesLeft, splitSize);
                    splits.add(inputSplit);
                    
                    bytesLeft -= splitSize;
                }

                if (bytesLeft != 0) {
                    HSynthInputSplit inputSplit = new HSynthInputSplit(fs, path, length - bytesLeft, bytesLeft);
                    splits.add(inputSplit);
                }
            } else if (length != 0) {
                HSynthInputSplit inputSplit = new HSynthInputSplit(fs, path, 0, length);
                splits.add(inputSplit);
            } else {
                HSynthInputSplit inputSplit = new HSynthInputSplit(fs, path, 0, length);
                splits.add(inputSplit);
            }
        }
        
        // Save the number of input files in the job-conf
        context.getConfiguration().setLong(CONF_NUM_INPUT_FILES, files.size());
    
        LOG.info("Total # of splits: " + splits.size());
        return splits;
    }
    
    protected long computeSplitSize(long blockSize, long minSize, long maxSize) {
        return Math.max(minSize, Math.min(maxSize, blockSize));
    }
    
    public static void setInputPaths(Job job, String commaSeparatedPaths) throws IOException {
        setInputPaths(job, StringUtil.stringToPath(getPathStrings(commaSeparatedPaths)));
    }
    
    public static void addInputPaths(Job job, String commaSeparatedPaths) throws IOException {
        for (String str : getPathStrings(commaSeparatedPaths)) {
            addInputPath(job, new HSynthFSPath(str));
        }
    }
    
    public static void setInputPaths(Job job, HSynthFSPath... inputPaths) throws IOException {
        Configuration conf = job.getConfiguration();
        StringBuffer str = new StringBuffer(StringUtils.escapeString(inputPaths[0].getPath()));
        for (int i = 1; i < inputPaths.length; i++) {
            str.append(StringUtils.COMMA_STR);
            HSynthFSPath path = inputPaths[i];
            str.append(StringUtils.escapeString(path.toString()));
        }

        conf.set(CONF_INPUT_DIR, str.toString());
    }
    
    public static void addInputPath(Job job, HSynthFSPath path) throws IOException {
        Configuration conf = job.getConfiguration();
        String dirStr = StringUtils.escapeString(path.toString());
        String dirs = conf.get(CONF_INPUT_DIR);
        conf.set(CONF_INPUT_DIR, dirs == null ? dirStr : dirs + StringUtils.COMMA_STR + dirStr);
    }
    
    private static String[] getPathStrings(String commaSeparatedPaths) {
        int length = commaSeparatedPaths.length();
        int curlyOpen = 0;
        int pathStart = 0;
        boolean globPattern = false;
        List<String> pathStrings = new ArrayList<String>();

        for (int i = 0; i < length; i++) {
            char ch = commaSeparatedPaths.charAt(i);
            switch (ch) {
                case '{': {
                    curlyOpen++;
                    if (!globPattern) {
                        globPattern = true;
                    }
                    break;
                }
                case '}': {
                    curlyOpen--;
                    if (curlyOpen == 0 && globPattern) {
                        globPattern = false;
                    }
                    break;
                }
                case ',': {
                    if (!globPattern) {
                        pathStrings.add(commaSeparatedPaths.substring(pathStart, i));
                        pathStart = i + 1;
                    }
                    break;
                }
            }
        }
        pathStrings.add(commaSeparatedPaths.substring(pathStart, length));

        return pathStrings.toArray(new String[0]);
    }
    
    public static HSynthFSPath[] getInputPaths(JobContext context) {
        String dirs = context.getConfiguration().get(CONF_INPUT_DIR, "");
        String[] list = StringUtils.split(dirs);
        HSynthFSPath[] result = new HSynthFSPath[list.length];
        for (int i = 0; i < list.length; i++) {
            result[i] = new HSynthFSPath(StringUtils.unEscapeString(list[i]));
        }
        return result;
    }
}
