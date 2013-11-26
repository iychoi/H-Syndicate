package edu.arizona.cs.hsynth.fs;

public interface FilenameFilter {
    boolean accept(Path dir, String name);
}
