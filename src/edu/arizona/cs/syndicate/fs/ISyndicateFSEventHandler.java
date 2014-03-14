package edu.arizona.cs.syndicate.fs;

public interface ISyndicateFSEventHandler {
    void onBeforeCreate(SyndicateFSConfiguration conf);
    void onAfterCreate(ASyndicateFileSystem fs);
    
    void onBeforeDestroy(ASyndicateFileSystem fs);
    void onAfterDestroy(SyndicateFSConfiguration conf);
}
