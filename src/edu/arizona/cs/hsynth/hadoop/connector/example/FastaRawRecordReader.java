package edu.arizona.cs.hsynth.hadoop.connector.example;

import edu.arizona.cs.hsynth.fs.HSynthFSConfiguration;
import edu.arizona.cs.hsynth.fs.HSynthFSPath;
import edu.arizona.cs.hsynth.fs.HSynthFileSystem;
import edu.arizona.cs.hsynth.hadoop.input.HSynthFSDataInputStream;
import edu.arizona.cs.hsynth.hadoop.input.HSynthFSSeekableInputStream;
import edu.arizona.cs.hsynth.hadoop.input.HSynthInputSplit;
import edu.arizona.cs.hsynth.hadoop.util.HSynthCompressionCodecUtil;
import edu.arizona.cs.hsynth.hadoop.util.HSynthConfigUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;

public class FastaRawRecordReader extends RecordReader<LongWritable, FastaRawRecord> {

    private static final Log LOG = LogFactory.getLog(FastaRawRecordReader.class);
    
    public static final char RECORD_DELIMITER = '>';
    
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private int maxLineLength;
    private String filename;
    private boolean hasNextRecord;
    private LongWritable key;
    private FastaRawRecord value;
    private Text prevLine;
    private long prevSize;
    private boolean isCompressed;
    private long uncompressedSize;

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public FastaRawRecord getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {

        HSynthInputSplit split = (HSynthInputSplit) genericSplit;
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        this.start = split.getStart();
        this.end = this.start + split.getLength();
        final HSynthFSPath file = split.getPath();
        final CompressionCodec codec = HSynthCompressionCodecUtil.getCompressionCodec(job, file);
        
        this.filename = file.getName();
        
        // open the file and seek to the start of the split
        HSynthFileSystem fs = null;
        try {
            HSynthFSConfiguration hconf = HSynthConfigUtil.getHSynthFSConfigurationInstance(job);
            fs = hconf.getContext().getFileSystem();
        } catch (InstantiationException ex) {
            throw new IOException(ex);
        }
        
        // get uncompressed length
        if (codec instanceof GzipCodec) {
            this.isCompressed = true;
            
            HSynthFSDataInputStream fileInCheckSize = new HSynthFSDataInputStream(new HSynthFSSeekableInputStream(fs.getRandomAccess(file)));
            byte[] len = new byte[4];
            try {
                LOG.info("compressed input : " + file.getName());
                LOG.info("compressed file size : " + this.end);
                fileInCheckSize.skip(this.end - 4);
                IOUtils.readFully(fileInCheckSize, len, 0, len.length);
                this.uncompressedSize = (len[3] << 24) | (len[2] << 16) + (len[1] << 8) + len[0];
                LOG.info("uncompressed file size : " + this.uncompressedSize);
            } finally {
                fileInCheckSize.close();
            }
            
            this.end = Long.MAX_VALUE;
        } else if(codec != null) {
            this.isCompressed = true;
            this.end = Long.MAX_VALUE;
            this.uncompressedSize = Long.MAX_VALUE;
        } else {
            this.isCompressed = false;
        }
        
        // get inputstream
        HSynthFSDataInputStream fileIn = new HSynthFSDataInputStream(new HSynthFSSeekableInputStream(fs.getRandomAccess(file)));
        
        if (codec != null) {
            this.in = new LineReader(codec.createInputStream(fileIn), job);
        } else {
            if (this.start != 0) {
                fileIn.seek(this.start);
            }
            this.in = new LineReader(fileIn, job);
        }
        
        // skip lines until we meet new record start
        while (this.start < this.end) {
            Text skipText = new Text();
            long newSize = this.in.readLine(skipText, this.maxLineLength,
                    Math.max((int) Math.min(Integer.MAX_VALUE, this.end - this.start),
                    this.maxLineLength));
            if (newSize == 0) {
                // EOF
                this.hasNextRecord = false;
                this.pos = this.end;
                break;
            }
            
            if (skipText.getLength() > 0 && skipText.charAt(0) == RECORD_DELIMITER) {
                this.prevLine = skipText;
                this.prevSize = newSize;
                this.hasNextRecord = true;
                this.pos = this.start;
                break;
            }
            
            this.start += newSize;
            
            if(this.start >= this.end) {
                // EOF
                this.hasNextRecord = false;
                this.pos = this.end;
                break;
            }
        }
        
        this.key = null;
        this.value = null;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // seek to new record start
        if(this.hasNextRecord) {
            this.key = new LongWritable(this.pos);
            this.value = new FastaRawRecord(this.filename);
            
            Text title = this.prevLine;
            this.pos += this.prevSize;
            
            long recordStartOffset = this.key.get();
            long nameStartOffset = recordStartOffset + 1;
            
            long dnaStartOffset = this.pos;
            long nameLen = dnaStartOffset - nameStartOffset;
            List<String> sequences = new ArrayList<String>();
            List<Long> sequenceStarts = new ArrayList<Long>();
            
            boolean foundNextRecord = false;
            while(!foundNextRecord) {
                Text newLine = new Text();
                long newSize = this.in.readLine(newLine, this.maxLineLength,
                        Math.max((int) Math.min(Integer.MAX_VALUE, this.end - this.pos),
                        this.maxLineLength));
                if (newSize == 0) {
                    // EOF
                    this.prevLine = null;
                    this.prevSize = 0;
                    this.pos = this.end;
                    break;
                }

                if (newLine.getLength() > 0 && newLine.charAt(0) == RECORD_DELIMITER) {
                    this.prevLine = newLine;
                    this.prevSize = newSize;
                    
                    if(this.pos + newSize < this.end) {
                        foundNextRecord = true;
                    } else {
                        foundNextRecord = false;
                    }
                    break;
                } else {
                    sequences.add(newLine.toString());
                    sequenceStarts.add(this.pos);
                }

                this.pos += newSize;
            }
            
            long newRecordStartOffset = this.pos;
            long recordLen = newRecordStartOffset - recordStartOffset;
            long dnaLen = newRecordStartOffset - dnaStartOffset;

            this.value.setRecordStartOffset(recordStartOffset);
            this.value.setNameStartOffset(nameStartOffset);
            this.value.setDNAStartOffset(dnaStartOffset);
            this.value.setRecordLen(recordLen);
            this.value.setNameLen(nameLen);
            this.value.setDNALen(dnaLen);
            this.value.setName(title.toString());
            
            FastaRawRecordLine[] recordLines = new FastaRawRecordLine[sequences.size()];
            for(int i=0;i<sequences.size();i++) {
                recordLines[i] = new FastaRawRecordLine(sequenceStarts.get(i), sequences.get(i));
            }
            
            this.value.setRawDNAData(recordLines);
            
            this.hasNextRecord = foundNextRecord;
            return true;
        } else {
            this.pos = this.end;
            this.prevLine = null;
            this.prevSize = 0;
            this.key = null;
            this.value = null;
            this.hasNextRecord = false;
            return false;
        }
    }

    @Override
    public float getProgress() throws IOException {
        if(this.isCompressed) {
            if (this.start == this.uncompressedSize) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (this.pos - this.start) / (float) (this.uncompressedSize - this.start));
            }
        } else {
            if (this.start == this.end) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (this.pos - this.start) / (float) (this.end - this.start));
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (this.in != null) {
            this.in.close();
        }
    }
}
