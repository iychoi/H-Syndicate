package edu.arizona.cs.hsynth.fs;

import java.io.Closeable;
import java.io.IOException;
import java.util.Hashtable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HSynthFSContext implements Closeable {

    private static final Log LOG = LogFactory.getLog(HSynthFSContext.class);
    
    private static Hashtable<HSynthFSConfiguration, HSynthFSContext> cachedContexts;
    
    private HSynthFSConfiguration configuration;
    private HSynthFileSystem filesystem;
    private boolean closed = true;
    
    static {
        cachedContexts = new Hashtable<HSynthFSConfiguration, HSynthFSContext>();
    }
    
    public static HSynthFSContext getContext(HSynthFSConfiguration configuration) {
        HSynthFSContext context = cachedContexts.get(configuration);
        if(context == null) {
            try {
                context = new HSynthFSContext(configuration);
            } catch (InstantiationException ex) {
                LOG.info(ex);
                return null;
            }
        }
        
        return context;
    }
    
    private HSynthFSContext(HSynthFSConfiguration configuration) throws InstantiationException {
        this.configuration = configuration;
        this.filesystem = HSynthFileSystem.createInstance(configuration);
        this.closed = false;
        
        cachedContexts.put(configuration, this);
    }
    
    public HSynthFSConfiguration getConfiguration() {
        return this.configuration;
    }
    
    public HSynthFileSystem getFileSystem() {
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
