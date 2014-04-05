package org.apache.hadoop.fs.hsyndicate.util;

import edu.arizona.cs.syndicate.fs.ASyndicateFileSystem;
import edu.arizona.cs.syndicate.fs.SyndicateFSConfiguration;
import edu.arizona.cs.syndicate.fs.SyndicateFileSystem;
import java.util.Hashtable;

public class SyndicateFileSystemFactory {
    private static Hashtable<String, ASyndicateFileSystem> fsCache = new Hashtable<String, ASyndicateFileSystem>();
    
    public static synchronized ASyndicateFileSystem getInstance(SyndicateFSConfiguration sconf) throws InstantiationException {
        ASyndicateFileSystem cachedFS = fsCache.get(sconf.getHostAddress());
        if(cachedFS != null && cachedFS.isClosed()) {
            fsCache.remove(sconf.getHostAddress());
            cachedFS = null;
        }
        
        if(cachedFS == null) {
            cachedFS = new SyndicateFileSystem(sconf);
            fsCache.put(sconf.getHostAddress(), cachedFS);
        }
        
        return cachedFS;
    }
}
