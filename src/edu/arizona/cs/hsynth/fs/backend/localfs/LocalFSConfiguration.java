package edu.arizona.cs.hsynth.fs.backend.localfs;

import edu.arizona.cs.hsynth.fs.HSynthFSBackend;
import edu.arizona.cs.hsynth.fs.HSynthFSConfiguration;
import edu.arizona.cs.hsynth.fs.HSynthFSContext;
import java.io.File;
import java.util.Hashtable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LocalFSConfiguration extends HSynthFSConfiguration {

    private static final Log LOG = LogFactory.getLog(LocalFSConfiguration.class);
    
    public final static String FS_BACKEND_NAME = "LocalFileSystem";
    // read buffer size
    public static final int READ_BUFFER_SIZE = 64 * 1024;
    // write buffer size
    public static final int WRITE_BUFFER_SIZE = 64 * 1024;
    
    private File workingDir;
    private int readBufferSize;
    private int writeBufferSize;
    
    private final static String KEY_READ_BUFFER_SIZE = HSYNTH_CONF_PREFIX + "rbuffersize";
    private final static String KEY_WRITE_BUFFER_SIZE = HSYNTH_CONF_PREFIX + "wbuffersize";
    private final static String KEY_WORKING_DIR = HSYNTH_CONF_PREFIX + "localfs.workingdir";

    static {
        try {
            HSynthFSBackend.registerBackend(FS_BACKEND_NAME, LocalFSConfiguration.class);
        } catch (InstantiationException ex) {
            LOG.error(ex);
        } catch (IllegalAccessException ex) {
            LOG.error(ex);
        }
    }
    
    public LocalFSConfiguration() {
        this.workingDir = null;
        this.readBufferSize = READ_BUFFER_SIZE;
        this.writeBufferSize = WRITE_BUFFER_SIZE;
    }

    public File getWorkingDir() {
        return this.workingDir;
    }

    public void setWorkingDir(File dir) throws IllegalAccessException {
        if (this.lock) {
            throw new IllegalAccessException("Can not modify the locked object");
        }

        this.workingDir = dir;
    }

    @Override
    public int getReadBufferSize() {
        return this.readBufferSize;
    }

    @Override
    public void setReadBufferSize(int bufferSize) throws IllegalAccessException {
        if (this.lock) {
            throw new IllegalAccessException("Can not modify the locked object");
        }

        this.readBufferSize = bufferSize;
    }

    @Override
    public int getWriteBufferSize() {
        return this.writeBufferSize;
    }

    @Override
    public void setWriteBufferSize(int bufferSize) throws IllegalAccessException {
        if (this.lock) {
            throw new IllegalAccessException("Can not modify the locked object");
        }

        this.writeBufferSize = bufferSize;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof LocalFSConfiguration)) {
            return false;
        }

        LocalFSConfiguration other = (LocalFSConfiguration) o;
        if (!this.workingDir.equals(other.workingDir)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return this.workingDir.hashCode() ^ FS_BACKEND_NAME.hashCode();
    }

    @Override
    public String getBackendName() {
        return FS_BACKEND_NAME;
    }

    @Override
    public Class getFileSystemClass() {
        return LocalFSFileSystem.class;
    }

    @Override
    public HSynthFSContext getContext() {
        return HSynthFSContext.getContext(this);
    }

    @Override
    public Hashtable<String, String> getParams() {
        Hashtable<String, String> table = new Hashtable<String, String>();
        table.put(KEY_READ_BUFFER_SIZE, Integer.toString(getReadBufferSize()));
        table.put(KEY_WRITE_BUFFER_SIZE, Integer.toString(getWriteBufferSize()));
        table.put(KEY_WORKING_DIR, getWorkingDir().getAbsolutePath());

        return table;
    }

    @Override
    public void load(Hashtable<String, String> params) throws IllegalAccessException {
        setReadBufferSize(Integer.parseInt(params.get(KEY_READ_BUFFER_SIZE)));
        setWriteBufferSize(Integer.parseInt(params.get(KEY_WRITE_BUFFER_SIZE)));
        setWorkingDir(new File(params.get(KEY_WORKING_DIR)));
    }
}
