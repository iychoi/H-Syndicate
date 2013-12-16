package edu.arizona.cs.hsynth.hadoop.connector.example;

public class FastaRawRecordLine {
    private long lineStart;
    private String line;

    public FastaRawRecordLine(long lineStart, String line) {
        this.lineStart = lineStart;
        this.line = line;
    }

    public long getLineStart() {
        return this.lineStart;
    }

    public String getLine() {
        return this.line;
    }
}
