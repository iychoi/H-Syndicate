package edu.arizona.cs.syndicate.fs;

public interface ISyndicateFSFilenameFilter {
    boolean accept(SyndicateFSPath dir, String name);
}
