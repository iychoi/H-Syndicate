/*
   Copyright 2016 The Trustees of University of Arizona

   Licensed under the Apache License, Version 2.0 (the "License" );
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package hsyndicate.rest.datatypes;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class Statvfs {

    
    private long bsize; // Filesystem block size
    private long frsize; // Fragment size
    private long blocks; // Size of fs in f_frsize units
    private long bfree; // Number of free blocks
    private long bavail; // Number of free blocks for unprivileged users
    private long files; // Number of inodes
    private long ffree; // Number of free inodes
    private long favail; // Number of free inodes for unprivileged users
    private long fsid; // Filesystem ID
    private long flag; // Mount flags
    private long namemax; // Maximum filename length
    private byte[] unused; // unused
    
    public Statvfs() {
        
    }
    
    @JsonProperty("f_bsize")
    public long getBsize() {
        return bsize;
    }

    @JsonProperty("f_bsize")
    public void setBsize(long bsize) {
        this.bsize = bsize;
    }

    @JsonProperty("f_frsize")
    public long getFrsize() {
        return frsize;
    }

    @JsonProperty("f_frsize")
    public void setFrsize(long frsize) {
        this.frsize = frsize;
    }

    @JsonProperty("f_blocks")
    public long getBlocks() {
        return blocks;
    }

    @JsonProperty("f_blocks")
    public void setBlocks(long blocks) {
        this.blocks = blocks;
    }

    @JsonProperty("f_bfree")
    public long getBfree() {
        return bfree;
    }

    @JsonProperty("f_bfree")
    public void setBfree(long bfree) {
        this.bfree = bfree;
    }

    @JsonProperty("f_bavail")
    public long getBavail() {
        return bavail;
    }

    @JsonProperty("f_bavail")
    public void setBavail(long bavail) {
        this.bavail = bavail;
    }

    @JsonProperty("f_files")
    public long getFiles() {
        return files;
    }

    @JsonProperty("f_files")
    public void setFiles(long files) {
        this.files = files;
    }

    @JsonProperty("f_ffree")
    public long getFfree() {
        return ffree;
    }

    @JsonProperty("f_ffree")
    public void setFfree(long ffree) {
        this.ffree = ffree;
    }

    @JsonProperty("f_favail")
    public long getFavail() {
        return favail;
    }

    @JsonProperty("f_favail")
    public void setFavail(long favail) {
        this.favail = favail;
    }

    @JsonProperty("f_fsid")
    public long getFsid() {
        return fsid;
    }

    @JsonProperty("f_fsid")
    public void setFsid(long fsid) {
        this.fsid = fsid;
    }

    @JsonProperty("f_flag")
    public long getFlag() {
        return flag;
    }

    @JsonProperty("f_flag")
    public void setFlag(long flag) {
        this.flag = flag;
    }

    @JsonProperty("f_namemax")
    public long getNamemax() {
        return namemax;
    }

    @JsonProperty("f_namemax")
    public void setNamemax(long namemax) {
        this.namemax = namemax;
    }
    
    @JsonProperty("unused")
    public byte[] getUnused() {
        return unused;
    }

    @JsonProperty("unused")
    public void setUnused(byte[] unused) {
        this.unused = unused;
    }
}
