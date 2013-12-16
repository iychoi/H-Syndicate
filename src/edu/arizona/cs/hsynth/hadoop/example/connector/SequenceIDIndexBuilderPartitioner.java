package edu.arizona.cs.hsynth.hadoop.example.connector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Partitioner;

public class SequenceIDIndexBuilderPartitioner<K, V> extends Partitioner<K, V> {

    private static final Log LOG = LogFactory.getLog(SequenceIDIndexBuilderPartitioner.class);
    
    @Override
    public int getPartition(K key, V value, int numReduceTasks) {
        if(!(key instanceof MultiFileOffsetWritable)) {
            LOG.info("key is not an instance of MultiFileOffsetWritable");
        }
        
        MultiFileOffsetWritable obj = (MultiFileOffsetWritable) key;
        return obj.getOutID() % numReduceTasks;
    }
}
