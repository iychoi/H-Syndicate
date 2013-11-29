package edu.arizona.cs.hsynth.hadoop.input;

import edu.arizona.cs.hsynth.fs.FileSystem;
import edu.arizona.cs.hsynth.fs.Path;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class HSynthInputSplit extends InputSplit implements Writable {

    private FileSystem filesystem;
    private Path path;
    private long start;
    private long length;

    public HSynthInputSplit() {
    }
    
    /*
     * Constructs a split
     */
    public HSynthInputSplit(FileSystem fs, Path path, long start, long length) {
        if(fs == null)
            throw new IllegalArgumentException("Can not create Input Split from null file system");
        if(path == null)
            throw new IllegalArgumentException("Can not create Input Split from null path");
        
        this.filesystem = fs;
        this.path = path;
        this.start = start;
        this.length = length;
    }

    public FileSystem getFileSystem() {
        return this.filesystem;
    }
    
    /*
     * The file containing this split's data
     */
    public Path getPath() {
        return this.path;
    }
    
    /*
     * The position of split start
     */
    public long getStart() {
        return this.start;
    }

    /*
     * The number of bytes in the file to process
     */
    @Override
    public long getLength() {
        return this.length;
    }

    @Override
    public String toString() {
        return this.path.getPath() + ":" + this.start + "+" + this.length;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        String fsName = this.filesystem.toString();
        return new String[] {fsName};
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, this.path.getPath());
        out.writeLong(this.start);
        out.writeLong(this.length);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.path = new Path(Text.readString(in));
        this.start = in.readLong();
        this.length = in.readLong();
    }
}
