/*
 * Copyright 2015 iychoi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.arizona.cs.hsyndicate.fs.datatypes;

import org.codehaus.jackson.annotate.JsonProperty;

public class Stat {
    
    /*
     * Bit mask of mode
     */
    public static final int S_IFMT = 0170000; //bit mask for the file type bit fields
    public static final int S_IFSOCK = 0140000; //socket
    public static final int S_IFLNK = 0120000; //symbolic link
    public static final int S_IFREG = 0100000; //regular file
    public static final int S_IFBLK = 0060000; //block device
    public static final int S_IFDIR = 0040000; //directory
    public static final int S_IFCHR = 0020000; //character device
    public static final int S_IFIFO = 0010000; //FIFO
    public static final int S_ISUID = 0004000; //set-user-ID bit
    public static final int S_ISGID = 0002000; //set-group-ID bit (see below)
    public static final int S_ISVTX = 0001000; //sticky bit (see below)
    public static final int S_IRWXU = 00700; //mask for file owner permissions
    public static final int S_IRUSR = 00400; //owner has read permission
    public static final int S_IWUSR = 00200; //owner has write permission
    public static final int S_IXUSR = 00100; //owner has execute permission
    public static final int S_IRWXG = 00070; //mask for group permissions
    public static final int S_IRGRP = 00040; //group has read permission
    public static final int S_IWGRP = 00020; //group has write permission
    public static final int S_IXGRP = 00010; //group has execute permission
    public static final int S_IRWXO = 00007; //mask for permissions for others (not in group)
    public static final int S_IROTH = 00004; //others have read permission
    public static final int S_IWOTH = 00002; //others have write permission
    public static final int S_IXOTH = 00001; //others have execute permission
    
    private /* dev_t */ int st_dev;             /* Device ID of device containing file. */
    private /* ino_t */ int st_ino;             /* File serial number. */
    private /* mode_t */ int st_mode;		/* Mode of file */
    private /* nlink_t */ int st_nlink;         /* Number of hard links to the file. */
    private /* uid_t */ int st_uid;		/* User ID of the file's owner.	*/
    private /* gid_t */ int st_gid;		/* Group ID of the file's group.*/
    private /* dev_t */ int st_rdev;            /* Device ID (if file is character or block special). */
    private /* off_t */ long st_size;		/* Size of file, in bytes. */
    private /* time_t */ long st_atime;         /* Time of last access. */
    private /* time_t */ long st_mtime;         /* Time of last data modification. */
    private /* time_t */ long st_ctime;         /* Time of last status change. */
    private /* blksize_t */ long st_blksize;	/* Optimal block size for I/O.  */
    private /* blkcnt_t */ long st_blocks;	/* Number 512-byte blocks allocated. */
    
    public Stat() {
        this.st_dev = 0;
        this.st_ino = 0;
        this.st_mode = 0;
        this.st_nlink = 0;
        this.st_uid = 0;
        this.st_gid = 0;
        this.st_rdev = 0;
        this.st_size = 0;
        this.st_atime = 0;
        this.st_mtime = 0;
        this.st_ctime = 0;
        this.st_blksize = 0;
        this.st_blocks = 0;
    }
    
    @JsonProperty("dev")
    public int getDev() {
        return this.st_dev;
    }
    
    @JsonProperty("dev")
    public void setDev(int dev) {
        this.st_dev = dev;
    }
    
    @JsonProperty("ino")
    public int getIno() {
        return this.st_ino;
    }
    
    @JsonProperty("ino")
    public void setIno(int ino) {
        this.st_ino = ino;
    }
    
    @JsonProperty("mode")
    public int getMode() {
        return this.st_mode;
    }

    @JsonProperty("mode")
    public void setMode(int mode) {
        this.st_mode = mode;
    }
    
    @JsonProperty("nlink")
    public int getNlink() {
        return this.st_nlink;
    }

    @JsonProperty("nlink")
    public void setNlink(int nlink) {
        this.st_nlink = nlink;
    }
    
    @JsonProperty("uid")
    public int getUid() {
        return this.st_uid;
    }

    @JsonProperty("uid")
    public void setUid(int uid) {
        this.st_uid = uid;
    }

    @JsonProperty("gid")
    public int getGid() {
        return this.st_gid;
    }

    @JsonProperty("gid")
    public void setGid(int gid) {
        this.st_gid = gid;
    }
    
    @JsonProperty("rdev")
    public int getRdev() {
        return this.st_rdev;
    }
    
    @JsonProperty("rdev")
    public void setRdev(int rdev) {
        this.st_rdev = rdev;
    }

    @JsonProperty("size")
    public long getSize() {
        return this.st_size;
    }

    @JsonProperty("size")
    public void setSize(long size) {
        this.st_size = size;
    }
    
    @JsonProperty("atime")
    public long getAtime() {
        return this.st_atime;
    }

    @JsonProperty("atime")
    public void setAtime(long atime) {
        this.st_atime = atime;
    }

    @JsonProperty("mtime")
    public long getMtime() {
        return this.st_mtime;
    }

    @JsonProperty("mtime")
    public void setMtime(long mtime) {
        this.st_mtime = mtime;
    }
    
    @JsonProperty("ctime")
    public long getCtime() {
        return this.st_ctime;
    }

    @JsonProperty("ctime")
    public void setCtime(long ctime) {
        this.st_ctime = ctime;
    }

    @JsonProperty("blksize")
    public long getBlksize() {
        return this.st_blksize;
    }

    @JsonProperty("blksize")
    public void setBlksize(long blksize) {
        this.st_blksize = blksize;
    }

    @JsonProperty("blocks")
    public long getBlocks() {
        return st_blocks;
    }

    @JsonProperty("blocks")
    public void setBlocks(long blocks) {
        this.st_blocks = blocks;
    }
}
