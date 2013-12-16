package edu.arizona.cs.hsynth.hadoop.example.connector;

import edu.arizona.cs.hsynth.hadoop.output.HSynthMultipleOutputs;
import java.io.IOException;
import java.util.Hashtable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SequenceIDIndexBuilderReducer extends Reducer<MultiFileOffsetWritable, Text, LongWritable, Text> {
    
    private static final Log LOG = LogFactory.getLog(SequenceIDIndexBuilderReducer.class);
    
    private HSynthMultipleOutputs mos;
    private Hashtable<Integer, String> namedOutputCache;
    private long[] sequenceIds;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.mos = new HSynthMultipleOutputs(context);
        this.namedOutputCache = new Hashtable<Integer, String>();
        this.sequenceIds = new long[10];
        for(int i=0;i<this.sequenceIds.length;i++) {
            this.sequenceIds[i] = -1;
        }
    }
    
    @Override
    protected void reduce(MultiFileOffsetWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int id = key.getOutID();
        long offset = key.getOffset();
        
        if(this.sequenceIds.length <= id) {
            long[] newSequenceIds = new long[id+1];
            for(int i=0;i<newSequenceIds.length;i++) {
                if(i < this.sequenceIds.length) {
                    newSequenceIds[i] = this.sequenceIds[i];
                } else {
                    newSequenceIds[i] = -1;
                }
            }
            this.sequenceIds = newSequenceIds;
        }
        
        this.sequenceIds[id]++;
        
        String namedOutput = this.namedOutputCache.get(id);
        if (namedOutput == null) {
            String[] namedOutputs = context.getConfiguration().getStrings(FastaSequenceIDConstants.CONF_NAMED_OUTPUT_ID_PREFIX + id);
            if (namedOutputs.length != 1) {
                throw new IOException("no named output found");
            }
            namedOutput = namedOutputs[0];
            this.namedOutputCache.put(id, namedOutput);
        }
        
        for(Text value : values) {
            String sequenceId = value.toString();
            this.mos.write(namedOutput, new LongWritable(this.sequenceIds[id]), new Text(String.valueOf(this.sequenceIds[id]) + "," + String.valueOf(offset) + "," + sequenceId));
            //context.write(key, new Text(sb.toString()));
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.mos.close();
        this.namedOutputCache = null;
    }
}
