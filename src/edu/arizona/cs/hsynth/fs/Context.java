package edu.arizona.cs.hsynth.fs;

import java.io.Closeable;
import java.io.IOException;
import java.util.Hashtable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Context implements Closeable {

    private static final Log LOG = LogFactory.getLog(Context.class);
    
    private static Hashtable<Configuration, Context> cachedContexts;
    
    private Configuration configuration;
    private FileSystem filesystem;
    private boolean closed = true;
    
    static {
        cachedContexts = new Hashtable<Configuration, Context>();
    }
    
    public static Context getContext(Configuration configuration) {
        Context context = cachedContexts.get(configuration);
        if(context == null) {
            try {
                context = new Context(configuration);
            } catch (InstantiationException ex) {
                LOG.info(ex);
                return null;
            }
        }
        
        return context;
    }
    
    private Context(Configuration configuration) throws InstantiationException {
        this.configuration = configuration;
        this.filesystem = FileSystem.createInstance(configuration);
        this.closed = false;
        
        cachedContexts.put(configuration, this);
    }
    
    public Configuration getConfiguration() {
        return this.configuration;
    }
    
    public FileSystem getFileSystem() {
        return this.filesystem;
    }
    
    @Override
    public void close() throws IOException {
        if(!this.closed) {
            if(this.filesystem != null) {
                this.filesystem.close();
            }
            this.closed = true;
            
            cachedContexts.remove(this.getConfiguration());
        }
    }
}
