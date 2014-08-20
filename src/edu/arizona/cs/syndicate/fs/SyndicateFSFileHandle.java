package edu.arizona.cs.syndicate.fs;

import edu.arizona.cs.syndicate.fs.client.FileInfo;
import edu.arizona.cs.syndicate.util.BlockUtils;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Hashtable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SyndicateFSFileHandle implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(SyndicateFSFileHandle.class);

    private SyndicateFileSystem filesystem;
    private SyndicateFSFileStatus status;
    private FileInfo fileinfo;
    private boolean readonly = false;
    private boolean closed = true;
    private boolean modified = false;
    private String cachedFilePath;
    private long blockSize;
    private int blockNum;
    private Hashtable<Integer, File> cachedBlockFilesTable;
    
    SyndicateFSFileHandle(SyndicateFileSystem fs, SyndicateFSFileStatus status, FileInfo fi, boolean readonly) {
        this.filesystem = fs;
        this.status = status;
        this.fileinfo = fi;
        this.readonly = readonly;
        this.closed = false;
        this.modified = false;
        
        this.blockSize = fs.getBlockSize();
        this.blockNum = BlockUtils.getBlocks(status.getSize(), this.blockSize);
        
        try {
            this.cachedFilePath = fs.getLocalCachedFilePath(status.getPath());
            this.cachedBlockFilesTable = makeCachedBlockFilesTable(this.cachedFilePath);
        } catch (IOException ex) {
            LOG.info("exception raised while reading cache info : " + ex.toString());
            this.cachedFilePath = null;
        }
    }
    
    public synchronized SyndicateFileSystem getFileSystem() {
        return this.filesystem;
    }
    
    public synchronized SyndicateFSPath getPath() {
        return this.status.getPath();
    }
    
    public synchronized SyndicateFSFileStatus getStatus() {
        return this.status;
    }
    
    public synchronized String getCachedFilePath() {
        return this.cachedFilePath;
    }
    
    private Hashtable<Integer, File> makeCachedBlockFilesTable(String cachedFilePath) throws IOException {
        File dir = new File(cachedFilePath);
        if(dir.exists() && dir.isDirectory()) {
            Hashtable<Integer, File> table = new Hashtable<Integer, File>();
            File[] blockFilesList = dir.listFiles();
            
            for(File file : blockFilesList) {
                String filename = file.getName();
                int dotidx = filename.indexOf(".");
                if(dotidx > 0) {
                    String blockId = filename.substring(0, dotidx);
                    String blockVer = filename.substring(dotidx+1);
                    
                    int blockId_int = Integer.parseInt(blockId);
                    int blockVer_int = Integer.parseInt(blockVer);
                    
                    File existFile = table.get(blockId_int);
                    if(existFile == null) {
                        table.put(blockId_int, file);
                    } else {
                        String existFilename = existFile.getName();
                        int existBlockVer_int = 0;
                        int existDotidx = existFilename.indexOf(".");
                        if(existDotidx > 0) {
                            String existBlockVer = existFilename.substring(existDotidx+1);
                            existBlockVer_int = Integer.parseInt(existBlockVer);
                        }
                        
                        if(existBlockVer_int <= blockVer_int) {
                            // remove old
                            table.remove(blockId_int);
                            // add new
                            table.put(blockId_int, file);
                        }
                    }
                }
            }
            
            return table;
        } else {
            throw new IOException("directory not exists : " + cachedFilePath);
        }
    }
    
    public synchronized int readFileData(long fileoffset, byte[] buffer, int offset, int size) throws IOException {
        if(this.cachedFilePath != null) {
            int blockid = BlockUtils.getBlockID(fileoffset, this.blockSize);
            // check the cached file block is present
            File cachedBlockFile = this.cachedBlockFilesTable.get(blockid);
            if(cachedBlockFile != null) {
                // has cache
                if(cachedBlockFile.exists()) {
                    RandomAccessFile raf = new RandomAccessFile(cachedBlockFile, "r");
                    long inneroffset = fileoffset - (blockid * this.blockSize);
                    long left = this.blockSize - inneroffset;
                    int toRead = (int) Math.min(left, size);
                    int readBytes = 0;

                    raf.seek(inneroffset);
                    while(readBytes < toRead) {
                        int result = raf.read(buffer, offset + readBytes, toRead - readBytes);
                        if(result > 0) {
                            readBytes += result;
                        } else {
                            break;
                        }
                    }

                    raf.close();
                    return readBytes;
                }
            }
        }
        
        return this.filesystem.getIPCClient().readFileData(this.fileinfo, fileoffset, buffer, offset, size);
    }
    
    public synchronized void writeFileData(long fileoffset, byte[] buffer, int offset, int size) throws IOException {
        if(this.readonly) {
            throw new IOException("Can not write data to readonly handle");
        }
        
        this.filesystem.getIPCClient().writeFileData(this.fileinfo, fileoffset, buffer, offset, size);
        this.status.notifySizeChanged(fileoffset + size);
        this.modified = true;
    }
    
    public synchronized void truncate(long fileoffset) throws IOException {
        if(this.readonly) {
            throw new IOException("Can not truncate data to readonly handle");
        }
        
        this.filesystem.getIPCClient().truncateFile(this.fileinfo, fileoffset);
        this.status.notifySizeChanged(fileoffset);
        this.modified = true;
    }
    
    public synchronized boolean isOpen() {
        return !this.closed;
    }
    
    public synchronized void flush() throws IOException {
        if(this.readonly) {
            throw new IOException("Can not flush data to readonly handle");
        }
        
        this.filesystem.getIPCClient().flush(this.fileinfo);
    }
    
    public synchronized boolean isReadonly() {
        return this.readonly;
    }
    
    @Override
    public synchronized void close() throws IOException {
        this.filesystem.getIPCClient().closeFileHandle(this.fileinfo);
        this.closed = true;
        
        if(!this.readonly) {
            if(this.modified) {
                this.status.setDirty();
            }
        }
        
        this.cachedBlockFilesTable.clear();
    }
}
