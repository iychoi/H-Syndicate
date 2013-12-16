package edu.arizona.cs.hsynth.hadoop.example.filesystem;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
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
        FileSystem fs = FileSystem.get(conf);
        String[] paths = SequenceIDIndexHelper.splitCommaSeparated(inputPath);
        Path[] inputFiles = SequenceIDIndexHelper.getAllInputPaths(fs, paths, new FastaPathFilter());
        String[] namedOutputs = SequenceIDIndexHelper.generateNamedOutputs(inputFiles);
        
        FileInputFormat.addInputPaths(job, SequenceIDIndexHelper.makeCommaSeparated(inputFiles));
        FileInputFormat.setInputPathFilter(job, FastaPathFilter.class);
        
        job.setInputFormatClass(FastaRawInputFormat.class);
        
        // Output
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        int id = 0;
        for(String namedOutput : namedOutputs) {
            LOG.info("regist new named output : " + namedOutput);
            job.getConfiguration().setStrings(FastaSequenceIDConstants.CONF_NAMED_OUTPUT_ID_PREFIX + id, namedOutput);
            LOG.info("regist new ConfigString : " + FastaSequenceIDConstants.CONF_NAMED_OUTPUT_ID_PREFIX + id);
            job.getConfiguration().setInt(FastaSequenceIDConstants.CONF_NAMED_OUTPUT_NAME_PREFIX + namedOutput, id);
            LOG.info("regist new ConfigString : " + FastaSequenceIDConstants.CONF_NAMED_OUTPUT_NAME_PREFIX + namedOutput);
            MultipleOutputs.addNamedOutput(job, namedOutput, MapFileOutputFormat.class, LongWritable.class, Text.class);
            id++;
        }
        
        job.setNumReduceTasks(6);
        
        // Execute job and return status
        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;
    }
}
