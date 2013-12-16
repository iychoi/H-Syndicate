package edu.arizona.cs.hsynth.hadoop.example.connector;

public class FastaRawRecord {
    private String filename;
    private long record_start_offset;
    private long name_start_offset;
    private long dna_start_offset;
    private long record_len;
    private long name_len;
    private long dna_len;
    private String name;
    
    private FastaRawRecordLine[] raw_dna_data;
    
    public FastaRawRecord(String filename) {
        this.filename = filename;
    }

    public String getFileName() {
        return this.filename;
    }
    
    public void setRecordStartOffset(long offset) {
        this.record_start_offset = offset;
    }
    
    public long getRecordStartOffset() {
        return this.record_start_offset;
    }
    
    public void setNameStartOffset(long offset) {
        this.name_start_offset = offset;
    }
    
    public long getNameStartOffset() {
        return this.name_start_offset;
    }
    
    public void setDNAStartOffset(long offset) {
        this.dna_start_offset = offset;
    }
    
    public long getDNAStartOffset() {
        return this.dna_start_offset;
    }
    
    public void setRecordLen(long len) {
        this.record_len = len;
    }
    
    public long getRecordLen() {
        return this.record_len;
    }
    
    public void setNameLen(long len) {
        this.name_len = len;
    }
    
    public long getNameLen() {
        return this.name_len;
    }
    
    public void setDNALen(long len) {
        this.dna_len = len;
    }
    
    public long getDNALen() {
        return this.dna_len;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setRawDNAData(FastaRawRecordLine[] raw_dna_data) {
        this.raw_dna_data = raw_dna_data;
    }
    
    public FastaRawRecordLine[] getRawDNAData() {
        return this.raw_dna_data;
    }
}
