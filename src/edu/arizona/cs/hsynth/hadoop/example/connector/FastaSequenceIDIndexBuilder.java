package edu.arizona.cs.hsynth.hadoop.example.connector;

import edu.arizona.cs.hsynth.fs.HSynthFSConfiguration;
import edu.arizona.cs.hsynth.fs.HSynthFSPath;
import edu.arizona.cs.hsynth.fs.HSynthFileSystem;
import edu.arizona.cs.hsynth.fs.backend.localfs.LocalFSConfiguration;
import edu.arizona.cs.hsynth.hadoop.connector.input.HSynthFileInputFormat;
import edu.arizona.cs.hsynth.hadoop.connector.output.HSynthFileOutputFormat;
import edu.arizona.cs.hsynth.hadoop.connector.output.HSynthMapFileOutputFormat;
import edu.arizona.cs.hsynth.hadoop.connector.output.HSynthMultipleOutputs;
import edu.arizona.cs.hsynth.hadoop.util.HSynthConfigUtil;
import java.io.File;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FastaSequenceIDIndexBuilder extends Configured implements Tool {
    
    private static final Log LOG = LogFactory.getLog(FastaSequenceIDIndexBuilder.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new FastaSequenceIDIndexBuilder(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        if(args.length != 2) {
            throw new Exception("wrong command");
        }
        
        String inputPath = args[0];
        String outputPath = args[1];
        
        Configuration conf = this.getConf();
        
        LocalFSConfiguration localfsConf = new LocalFSConfiguration();
        localfsConf.setWorkingDir(new File("./hsynthfs"));

        HSynthConfigUtil.setHSynthFSConfiguration(conf, localfsConf);
        
        // configuration
        conf.set("mapred.child.java.opts", "-Xms256M -Xmx512M");
        conf.setInt("io.file.buffer.size", 409600);
        Job job = new Job(conf, "FASTA SequenceID Index Builder");
        job.setJarByClass(FastaSequenceIDIndexBuilder.class);

        // Identity Mapper & Reducer
        job.setMapperClass(SequenceIDIndexBuilderMapper.class);
        job.setReducerClass(SequenceIDIndexBuilderReducer.class);
        job.setPartitionerClass(SequenceIDIndexBuilderPartitioner.class);

        job.setMapOutputKeyClass(MultiFileOffsetWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        // Specify key / value
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        
        // Inputs
        HSynthFileSystem fs = null;
        try {
            HSynthFSConfiguration hsynthconf = HSynthConfigUtil.getHSynthFSConfigurationInstance(conf);
            fs = hsynthconf.getContext().getFileSystem();
        } catch (InstantiationException ex) {
            throw new IOException(ex);
        }
        String[] paths = SequenceIDIndexHelper.splitCommaSeparated(inputPath);
        HSynthFSPath[] inputFiles = SequenceIDIndexHelper.getAllInputPaths(fs, paths, new FastaPathFilter());
        String[] namedOutputs = SequenceIDIndexHelper.generateNamedOutputs(inputFiles);
        
        HSynthFileInputFormat.addInputPaths(job, SequenceIDIndexHelper.makeCommaSeparated(inputFiles));
        HSynthFileInputFormat.setInputPathFilter(job, FastaPathFilter.class);
        
        job.setInputFormatClass(FastaRawInputFormat.class);
        
        // Output
        HSynthFileOutputFormat.setOutputPath(job, new HSynthFSPath(outputPath));
        
        int id = 0;
        for(String namedOutput : namedOutputs) {
            LOG.info("regist new named output : " + namedOutput);
            job.getConfiguration().setStrings(FastaSequenceIDConstants.CONF_NAMED_OUTPUT_ID_PREFIX + id, namedOutput);
            LOG.info("regist new ConfigString : " + FastaSequenceIDConstants.CONF_NAMED_OUTPUT_ID_PREFIX + id);
            job.getConfiguration().setInt(FastaSequenceIDConstants.CONF_NAMED_OUTPUT_NAME_PREFIX + namedOutput, id);
            LOG.info("regist new ConfigString : " + FastaSequenceIDConstants.CONF_NAMED_OUTPUT_NAME_PREFIX + namedOutput);
            HSynthMultipleOutputs.addNamedOutput(job, namedOutput, HSynthMapFileOutputFormat.class, LongWritable.class, Text.class);
            id++;
        }
        
        job.setNumReduceTasks(6);
        
        // Execute job and return status
        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;
    }
}
