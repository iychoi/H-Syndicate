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
public class StatRaw {
    private int type;
    private String name;
    private long file_id;
    private long ctime_sec;
    private int ctime_nsec;
    private long mtime_sec;
    private int mtime_nsec;
    private long manifest_mtime_sec;
    private int manifest_mtime_nsec;
    private long write_nonce;
    private long xattr_nonce;
    private long version;
    private int max_read_freshness;
    private int max_write_freshness;
    private long owner;
    private long coordinator;
    private long volume;
    private int mode;
    private long size;
    private int error;
    private long generation;
    private long num_children;
    private long capacity;
    private String ent_sig;
    private long ent_sig_len;
    private long parent_id;
    private String xattr_hash;

    public StatRaw() {
        
    }
    
    @JsonProperty("type")
    public int getType() {
        return type;
    }

    @JsonProperty("type")
    public void setType(int type) {
        this.type = type;
    }
    
    @JsonIgnore
    public boolean isFile() {
        return this.type == 1;
    }
    
    @JsonIgnore
    public boolean isDirectory() {
        return this.type == 2;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("file_id")
    public long getFile_id() {
        return file_id;
    }

    @JsonProperty("file_id")
    public void setFile_id(long file_id) {
        this.file_id = file_id;
    }

    @JsonProperty("ctime_sec")
    public long getCtime_sec() {
        return ctime_sec;
    }

    @JsonProperty("ctime_sec")
    public void setCtime_sec(long ctime_sec) {
        this.ctime_sec = ctime_sec;
    }

    @JsonProperty("ctime_nsec")
    public int getCtime_nsec() {
        return ctime_nsec;
    }

    @JsonProperty("ctime_nsec")
    public void setCtime_nsec(int ctime_nsec) {
        this.ctime_nsec = ctime_nsec;
    }
    
    @JsonIgnore
    public long getCtime() {
        return this.ctime_sec * 1000 + this.ctime_nsec;
    }

    @JsonProperty("mtime_sec")
    public long getMtime_sec() {
        return mtime_sec;
    }

    @JsonProperty("mtime_sec")
    public void setMtime_sec(long mtime_sec) {
        this.mtime_sec = mtime_sec;
    }

    @JsonProperty("mtime_nsec")
    public int getMtime_nsec() {
        return mtime_nsec;
    }

    @JsonProperty("mtime_nsec")
    public void setMtime_nsec(int mtime_nsec) {
        this.mtime_nsec = mtime_nsec;
    }
    
    @JsonIgnore
    public long getMtime() {
        return this.mtime_sec * 1000 + this.mtime_nsec;
    }

    @JsonProperty("manifest_mtime_sec")
    public long getManifest_mtime_sec() {
        return manifest_mtime_sec;
    }

    @JsonProperty("manifest_mtime_sec")
    public void setManifest_mtime_sec(long manifest_mtime_sec) {
        this.manifest_mtime_sec = manifest_mtime_sec;
    }

    @JsonProperty("manifest_mtime_nsec")
    public int getManifest_mtime_nsec() {
        return manifest_mtime_nsec;
    }

    @JsonProperty("manifest_mtime_nsec")
    public void setManifest_mtime_nsec(int manifest_mtime_nsec) {
        this.manifest_mtime_nsec = manifest_mtime_nsec;
    }

    @JsonProperty("write_nonce")
    public long getWrite_nonce() {
        return write_nonce;
    }

    @JsonProperty("write_nonce")
    public void setWrite_nonce(long write_nonce) {
        this.write_nonce = write_nonce;
    }

    @JsonProperty("xattr_nonce")
    public long getXattr_nonce() {
        return xattr_nonce;
    }

    @JsonProperty("xattr_nonce")
    public void setXattr_nonce(long xattr_nonce) {
        this.xattr_nonce = xattr_nonce;
    }

    @JsonProperty("version")
    public long getVersion() {
        return version;
    }

    @JsonProperty("version")
    public void setVersion(long version) {
        this.version = version;
    }

    @JsonProperty("max_read_freshness")
    public int getMax_read_freshness() {
        return max_read_freshness;
    }

    @JsonProperty("max_read_freshness")
    public void setMax_read_freshness(int max_read_freshness) {
        this.max_read_freshness = max_read_freshness;
    }

    @JsonProperty("max_write_freshness")
    public int getMax_write_freshness() {
        return max_write_freshness;
    }

    @JsonProperty("max_write_freshness")
    public void setMax_write_freshness(int max_write_freshness) {
        this.max_write_freshness = max_write_freshness;
    }

    @JsonProperty("owner")
    public long getOwner() {
        return owner;
    }

    @JsonProperty("owner")
    public void setOwner(long owner) {
        this.owner = owner;
    }

    @JsonProperty("coordinator")
    public long getCoordinator() {
        return coordinator;
    }

    @JsonProperty("coordinator")
    public void setCoordinator(long coordinator) {
        this.coordinator = coordinator;
    }

    @JsonProperty("volume")
    public long getVolume() {
        return volume;
    }

    @JsonProperty("volume")
    public void setVolume(long volume) {
        this.volume = volume;
    }

    @JsonProperty("mode")
    public int getMode() {
        return mode;
    }

    @JsonProperty("mode")
    public void setMode(int mode) {
        this.mode = mode;
    }

    @JsonProperty("size")
    public long getSize() {
        return size;
    }

    @JsonProperty("size")
    public void setSize(long size) {
        this.size = size;
    }

    @JsonProperty("error")
    public int getError() {
        return error;
    }

    @JsonProperty("error")
    public void setError(int error) {
        this.error = error;
    }

    @JsonProperty("generation")
    public long getGeneration() {
        return generation;
    }

    @JsonProperty("generation")
    public void setGeneration(long generation) {
        this.generation = generation;
    }

    @JsonProperty("num_children")
    public long getNum_children() {
        return num_children;
    }

    @JsonProperty("num_children")
    public void setNum_children(long num_children) {
        this.num_children = num_children;
    }

    @JsonProperty("capacity")
    public long getCapacity() {
        return capacity;
    }

    @JsonProperty("capacity")
    public void setCapacity(long capacity) {
        this.capacity = capacity;
    }

    @JsonProperty("ent_sig")
    public String getEnt_sig() {
        return ent_sig;
    }

    @JsonProperty("ent_sig")
    public void setEnt_sig(String ent_sig) {
        this.ent_sig = ent_sig;
    }

    @JsonProperty("ent_sig_len")
    public long getEnt_sig_len() {
        return ent_sig_len;
    }

    @JsonProperty("ent_sig_len")
    public void setEnt_sig_len(long ent_sig_len) {
        this.ent_sig_len = ent_sig_len;
    }

    @JsonProperty("parent_id")
    public long getParent_id() {
        return parent_id;
    }

    @JsonProperty("parent_id")
    public void setParent_id(long parent_id) {
        this.parent_id = parent_id;
    }

    @JsonProperty("xattr_hash")
    public String getXattr_hash() {
        return xattr_hash;
    }

    @JsonProperty("xattr_hash")
    public void setXattr_hash(String xattr_hash) {
        this.xattr_hash = xattr_hash;
    }
}
