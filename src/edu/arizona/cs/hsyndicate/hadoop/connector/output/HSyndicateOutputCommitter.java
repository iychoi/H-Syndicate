package edu.arizona.cs.hsyndicate.hadoop.connector.output;

import edu.arizona.cs.syndicate.fs.ASyndicateFileSystem;
import edu.arizona.cs.syndicate.fs.SyndicateFSPath;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class HSyndicateOutputCommitter extends OutputCommitter {

    private static final Log LOG = LogFactory.getLog(HSyndicateOutputCommitter.class);

    private ASyndicateFileSystem outputFileSystem = null;
    private SyndicateFSPath outputPath = null;

    public HSyndicateOutputCommitter(ASyndicateFileSystem fs, SyndicateFSPath output, TaskAttemptContext context) {
        if (output != null) {
            this.outputPath = output;
            this.outputFileSystem = fs;
        }
    }

    @Override
    public void setupJob(JobContext context) throws IOException {
        LOG.info("Setting up job.");

        if (this.outputPath != null) {
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
            if (this.outputPath != null) {
                context.progress();
                this.outputFileSystem.deleteAll(this.outputPath);
            }
        } catch (IOException ie) {
            LOG.error("Error discarding output");
        }
    }

    public SyndicateFSPath getOutputPath() {
        return this.outputPath;
    }

    public ASyndicateFileSystem getOutFileSystem() {
        return this.outputFileSystem;
    }
}
