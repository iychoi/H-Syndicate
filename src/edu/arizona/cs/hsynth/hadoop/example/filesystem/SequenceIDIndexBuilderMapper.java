package edu.arizona.cs.hsynth.hadoop.example.filesystem;

import java.io.IOException;
import java.util.Hashtable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SequenceIDIndexBuilderMapper extends Mapper<LongWritable, FastaRawRecord, MultiFileOffsetWritable, Text> {
    
    private static final Log LOG = LogFactory.getLog(SequenceIDIndexBuilderMapper.class);
    
    private Hashtable<String, Integer> namedOutputIDCache;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.namedOutputIDCache = new Hashtable<String, Integer>();
    }
    
    @Override
    protected void map(LongWritable key, FastaRawRecord value, Context context) throws IOException, InterruptedException {
        Integer id = this.namedOutputIDCache.get(value.getFileName());
        if (id == null) {
            String namedOutput = SequenceIDIndexHelper.generateNamedOutput(value.getFileName());
            id = context.getConfiguration().getInt(FastaSequenceIDConstants.CONF_NAMED_OUTPUT_NAME_PREFIX + namedOutput, -1);
            if (id < 0) {
                throw new IOException("No named output found : " + FastaSequenceIDConstants.CONF_NAMED_OUTPUT_NAME_PREFIX + namedOutput);
            }
            this.namedOutputIDCache.put(value.getFileName(), id);
        }

        context.write(new MultiFileOffsetWritable(id, value.getRecordStartOffset()), new Text(value.getName()));
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.namedOutputIDCache = null;
    }
}
