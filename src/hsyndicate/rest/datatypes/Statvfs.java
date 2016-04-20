/*
   Copyright 2015 The Trustees of Princeton University

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

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class Statvfs {

    
    private long f_bsize; // Filesystem block size
    private long f_frsize; // Fragment size
    private long f_blocks; // Size of fs in f_frsize units
    private long f_bfree; // Number of free blocks
    private long f_bavail; // Number of free blocks for unprivileged users
    private long f_files; // Number of inodes
    private long f_ffree; // Number of free inodes
    private long f_favail; // Number of free inodes for unprivileged users
    private long f_fsid; // Filesystem ID
    private long f_flag; // Mount flags
    private long f_namemax; // Maximum filename length
    
    public Statvfs() {
        
    }
    
    @JsonProperty("f_bsize")
    public long getBsize() {
        return f_bsize;
    }

    @JsonProperty("f_bsize")
    public void setBsize(long f_bsize) {
        this.f_bsize = f_bsize;
    }

    @JsonProperty("f_frsize")
    public long getFrsize() {
        return f_frsize;
    }

    @JsonProperty("f_frsize")
    public void setFrsize(long f_frsize) {
        this.f_frsize = f_frsize;
    }

    @JsonProperty("f_blocks")
    public long getBlocks() {
        return f_blocks;
    }

    @JsonProperty("f_blocks")
    public void setBlocks(long f_blocks) {
        this.f_blocks = f_blocks;
    }

    @JsonProperty("f_bfree")
    public long getBfree() {
        return f_bfree;
    }

    @JsonProperty("f_bfree")
    public void setBfree(long f_bfree) {
        this.f_bfree = f_bfree;
    }

    @JsonProperty("f_bavail")
    public long getBavail() {
        return f_bavail;
    }

    @JsonProperty("f_bavail")
    public void setBavail(long f_bavail) {
        this.f_bavail = f_bavail;
    }

    @JsonProperty("f_files")
    public long getFiles() {
        return f_files;
    }

    @JsonProperty("f_files")
    public void setFiles(long f_files) {
        this.f_files = f_files;
    }

    @JsonProperty("f_ffree")
    public long getFfree() {
        return f_ffree;
    }

    @JsonProperty("f_ffree")
    public void setFfree(long f_ffree) {
        this.f_ffree = f_ffree;
    }

    @JsonProperty("f_favail")
    public long getFavail() {
        return f_favail;
    }

    @JsonProperty("f_favail")
    public void setFavail(long f_favail) {
        this.f_favail = f_favail;
    }

    @JsonProperty("f_fsid")
    public long getFsid() {
        return f_fsid;
    }

    @JsonProperty("f_fsid")
    public void setFsid(long f_fsid) {
        this.f_fsid = f_fsid;
    }

    @JsonProperty("f_flag")
    public long getFlag() {
        return f_flag;
    }

    @JsonProperty("f_flag")
    public void setFlag(long f_flag) {
        this.f_flag = f_flag;
    }

    @JsonProperty("f_namemax")
    public long getNamemax() {
        return f_namemax;
    }

    @JsonProperty("f_namemax")
    public void setNamemax(long f_namemax) {
        this.f_namemax = f_namemax;
    }
}
