/*
 * FileSystem Event Handler class for JSyndicateFS
 */
package edu.arizona.cs.hsynth.fs;

/**
 *
 * @author iychoi
 */
public interface FSEventHandler {
    void onBeforeCreate(Configuration conf);
    void onAfterCreate(FileSystem fs);
    
    void onBeforeDestroy(FileSystem fs);
    void onAfterDestroy(Configuration conf);
}
