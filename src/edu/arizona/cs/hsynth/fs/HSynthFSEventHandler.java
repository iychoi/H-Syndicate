/*
 * HSynthFileSystem Event Handler class for JSyndicateFS
 */
package edu.arizona.cs.hsynth.fs;

/**
 *
 * @author iychoi
 */
public interface HSynthFSEventHandler {
    void onBeforeCreate(HSynthFSConfiguration conf);
    void onAfterCreate(HSynthFileSystem fs);
    
    void onBeforeDestroy(HSynthFileSystem fs);
    void onAfterDestroy(HSynthFSConfiguration conf);
}
