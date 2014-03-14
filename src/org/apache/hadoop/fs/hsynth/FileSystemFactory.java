package org.apache.hadoop.fs.hsynth;

import edu.arizona.cs.syndicate.fs.ASyndicateFileSystem;
import edu.arizona.cs.syndicate.fs.SyndicateFSConfiguration;
import edu.arizona.cs.syndicate.fs.SyndicateFileSystem;
import java.util.Hashtable;

public class FileSystemFactory {
    private static Hashtable<String, ASyndicateFileSystem> fsCache = new Hashtable<String, ASyndicateFileSystem>();
    
    public static synchronized ASyndicateFileSystem getInstance(SyndicateFSConfiguration sconf) throws InstantiationException {
        ASyndicateFileSystem cachedFS = fsCache.get(sconf.getHost());
        if(cachedFS != null && cachedFS.isClosed()) {
            fsCache.remove(sconf.getHost());
            cachedFS = null;
        }
        
        if(cachedFS == null) {
            cachedFS = new SyndicateFileSystem(sconf);
            fsCache.put(sconf.getHost(), cachedFS);
        }
        
        return cachedFS;
    }
}
