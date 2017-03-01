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

import com.google.common.primitives.UnsignedLong;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class StatRaw {
    private int type;
    private String name;
    private UnsignedLong fileID;
    private long ctimeSec;
    private int ctimeNsec;
    private long mtimeSec;
    private int mtimeNsec;
    private long manifestMtimeSec;
    private int manifestMtimeNsec;
    private long writeNonce;
    private long xattrNonce;
    private long version;
    private int maxReadFreshness;
    private int maxWriteFreshness;
    private UnsignedLong owner;
    private UnsignedLong coordinator;
    private UnsignedLong volume;
    private int mode;
    private long size;
    private int error;
    private long generation;
    private long numChildren;
    private long capacity;
    private String entSig;
    private long entSigLen;
    private UnsignedLong parentID;
    private String xattrHash;

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
    public UnsignedLong getFileID() {
        return fileID;
    }

    @JsonProperty("file_id")
    public void setFileID(UnsignedLong fileID) {
        this.fileID = fileID;
    }

    @JsonProperty("ctime_sec")
    public long getCtimeSec() {
        return ctimeSec;
    }

    @JsonProperty("ctime_sec")
    public void setCtimeSec(long ctimeSec) {
        this.ctimeSec = ctimeSec;
    }

    @JsonProperty("ctime_nsec")
    public int getCtimeNsec() {
        return ctimeNsec;
    }

    @JsonProperty("ctime_nsec")
    public void setCtimeNsec(int ctimeNsec) {
        this.ctimeNsec = ctimeNsec;
    }
    
    @JsonIgnore
    public long getCtime() {
        return this.ctimeSec * 1000 + this.ctimeNsec;
    }

    @JsonProperty("mtime_sec")
    public long getMtimeSec() {
        return mtimeSec;
    }

    @JsonProperty("mtime_sec")
    public void setMtimeSec(long mtimeSec) {
        this.mtimeSec = mtimeSec;
    }

    @JsonProperty("mtime_nsec")
    public int getMtimeNsec() {
        return mtimeNsec;
    }

    @JsonProperty("mtime_nsec")
    public void setMtimeNsec(int mtimeNsec) {
        this.mtimeNsec = mtimeNsec;
    }
    
    @JsonIgnore
    public long getMtime() {
        return this.mtimeSec * 1000 + this.mtimeNsec;
    }

    @JsonProperty("manifest_mtime_sec")
    public long getManifestMtimeSec() {
        return manifestMtimeSec;
    }

    @JsonProperty("manifest_mtime_sec")
    public void setManifestMtimeSec(long manifestMtimeSec) {
        this.manifestMtimeSec = manifestMtimeSec;
    }

    @JsonProperty("manifest_mtime_nsec")
    public int getManifestMtimeNsec() {
        return manifestMtimeNsec;
    }

    @JsonProperty("manifest_mtime_nsec")
    public void setManifestMtimeNsec(int manifestMtimeNsec) {
        this.manifestMtimeNsec = manifestMtimeNsec;
    }

    @JsonProperty("write_nonce")
    public long getWriteNonce() {
        return writeNonce;
    }

    @JsonProperty("write_nonce")
    public void setWriteNonce(long writeNonce) {
        this.writeNonce = writeNonce;
    }

    @JsonProperty("xattr_nonce")
    public long getXattrNonce() {
        return xattrNonce;
    }

    @JsonProperty("xattr_nonce")
    public void setXattrNonce(long xattrNonce) {
        this.xattrNonce = xattrNonce;
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
    public int getMaxReadFreshness() {
        return maxReadFreshness;
    }

    @JsonProperty("max_read_freshness")
    public void setMaxReadFreshness(int maxReadFreshness) {
        this.maxReadFreshness = maxReadFreshness;
    }

    @JsonProperty("max_write_freshness")
    public int getMaxWriteFreshness() {
        return maxWriteFreshness;
    }

    @JsonProperty("max_write_freshness")
    public void setMaxWriteFreshness(int maxWriteFreshness) {
        this.maxWriteFreshness = maxWriteFreshness;
    }

    @JsonProperty("owner")
    public UnsignedLong getOwner() {
        return owner;
    }

    @JsonProperty("owner")
    public void setOwner(UnsignedLong owner) {
        this.owner = owner;
    }

    @JsonProperty("coordinator")
    public UnsignedLong getCoordinator() {
        return coordinator;
    }

    @JsonProperty("coordinator")
    public void setCoordinator(UnsignedLong coordinator) {
        this.coordinator = coordinator;
    }

    @JsonProperty("volume")
    public UnsignedLong getVolume() {
        return volume;
    }

    @JsonProperty("volume")
    public void setVolume(UnsignedLong volume) {
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
    
    @JsonIgnore
    public int getUserMode() {
        return (mode >> 6) & 0x07;
    }
    
    @JsonIgnore
    public int getGroupMode() {
        return (mode >> 3) & 0x07;
    }
    
    @JsonIgnore
    public int getOthersMode() {
        return mode & 0x07;
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
    public long getNumChildren() {
        return numChildren;
    }

    @JsonProperty("num_children")
    public void setNumChildren(long numChildren) {
        this.numChildren = numChildren;
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
    public String getEntSig() {
        return entSig;
    }

    @JsonProperty("ent_sig")
    public void setEntSig(String entSig) {
        this.entSig = entSig;
    }

    @JsonProperty("ent_sig_len")
    public long getEntSigLen() {
        return entSigLen;
    }

    @JsonProperty("ent_sig_len")
    public void setEntSigLen(long entSigLen) {
        this.entSigLen = entSigLen;
    }

    @JsonProperty("parent_id")
    public UnsignedLong getParentID() {
        return parentID;
    }

    @JsonProperty("parent_id")
    public void setParentID(UnsignedLong parentID) {
        this.parentID = parentID;
    }

    @JsonProperty("xattr_hash")
    public String getXattrHash() {
        return xattrHash;
    }

    @JsonProperty("xattr_hash")
    public void setXattrHash(String xattrHash) {
        this.xattrHash = xattrHash;
    }
}
