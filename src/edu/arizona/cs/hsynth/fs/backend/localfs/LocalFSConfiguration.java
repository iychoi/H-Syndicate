package edu.arizona.cs.hsynth.fs.backend.localfs;

import edu.arizona.cs.hsynth.fs.Configuration;
import edu.arizona.cs.hsynth.fs.Context;
import java.io.File;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

public class LocalFSConfiguration extends Configuration {

    public final static String FS_BACKEND_NAME = "LocalFileSystem";
    // read buffer size
    public static final int READ_BUFFER_SIZE = 64 * 1024;
    // write buffer size
    public static final int WRITE_BUFFER_SIZE = 64 * 1024;
    
    private File workingDir;
    private int readBufferSize;
    private int writeBufferSize;
    
    private final static String JSON_KEY_FS_BACKEND = "fsbackend";
    private final static String JSON_KEY_READ_BUFFER_SIZE = "rbuffersize";
    private final static String JSON_KEY_WRITE_BUFFER_SIZE = "wbuffersize";
    private final static String JSON_KEY_WORKING_DIR = "workingdir";

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
    public Context getContext() {
        return Context.getContext(this);
    }

    @Override
    public String serialize() {
        JSONObject json = new JSONObject();
        json.put(JSON_KEY_FS_BACKEND, getBackendName());
        json.put(JSON_KEY_READ_BUFFER_SIZE, (Integer) getReadBufferSize());
        json.put(JSON_KEY_WRITE_BUFFER_SIZE, (Integer) getWriteBufferSize());
        json.put(JSON_KEY_WORKING_DIR, getWorkingDir().getAbsolutePath());

        return json.toString();
    }

    @Override
    public void deserialize(String serializedConf) throws IllegalAccessException {
        JSONObject json = (JSONObject)JSONSerializer.toJSON(serializedConf);
        //json.get(JSON_KEY_FS_BACKEND);
        setReadBufferSize((Integer)json.get(JSON_KEY_READ_BUFFER_SIZE));
        setWriteBufferSize((Integer)json.get(JSON_KEY_WRITE_BUFFER_SIZE));
        setWorkingDir(new File((String)json.get(JSON_KEY_WORKING_DIR)));
    }
}
