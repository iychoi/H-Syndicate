package edu.arizona.cs.hsynth.hadoop.output;

import edu.arizona.cs.hsynth.fs.FileSystem;
import edu.arizona.cs.hsynth.fs.Path;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class HSynthOutputCommitter extends OutputCommitter {

    private static final Log LOG = LogFactory.getLog(HSynthOutputCommitter.class);
    
    private FileSystem outputFileSystem = null;
    private Path outputPath = null;
    
    public HSynthOutputCommitter(FileSystem fs, Path output, TaskAttemptContext context) {
        if(output != null) {
            this.outputPath = output;
            this.outputFileSystem = fs;
        }
    }

    @Override
    public void setupJob(JobContext context) throws IOException {
        LOG.info("Setting up job.");
        
        if(this.outputPath != null) {
            this.outputFileSystem.mkdirs(this.outputPath);
        }
    }

    @Override
    public void setupTask(TaskAttemptContext tac) throws IOException {
        LOG.info("Setting up task.");
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext tac) throws IOException {
        return true;
    }

    @Override
    public void commitTask(TaskAttemptContext tac) throws IOException {
        LOG.info("Committing task.");
    }

    @Override
    public void abortTask(TaskAttemptContext context) throws IOException {
        LOG.info("Aborting task.");
        
        try {
            if(this.outputPath != null) {
                context.progress();
                this.outputFileSystem.deleteAll(this.outputPath);
            }
        } catch (IOException ie) {
            LOG.error("Error discarding output");
        }
    }
    
    public Path getOutputPath() {
        return this.outputPath;
    }
    
    public FileSystem getOutFileSystem() {
        return this.outputFileSystem;
    }
}
