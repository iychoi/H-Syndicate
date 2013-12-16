package edu.arizona.cs.hsynth.hadoop.connector.example;

import edu.arizona.cs.hsynth.fs.HSynthFSPath;
import edu.arizona.cs.hsynth.hadoop.input.HSynthFileInputFormat;
import edu.arizona.cs.hsynth.hadoop.util.HSynthCompressionCodecUtil;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FastaRawInputFormat extends HSynthFileInputFormat<LongWritable, FastaRawRecord> {

    @Override
    public RecordReader<LongWritable, FastaRawRecord> createRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new FastaRawRecordReader();
    }
    
    @Override
    protected boolean isSplitable(JobContext context, HSynthFSPath filename) {
        CompressionCodec codec = HSynthCompressionCodecUtil.getCompressionCodec(context.getConfiguration(), filename);
        return codec == null;
    }
}
