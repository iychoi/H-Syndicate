/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package edu.arizona.cs.hsyndicate.hadoop.connector.io;

import edu.arizona.cs.hsyndicate.fs.SyndicateFSPath;
import edu.arizona.cs.hsyndicate.fs.AHSyndicateFileSystemBase;
import edu.arizona.cs.hsyndicate.hadoop.connector.input.HSyndicateFSDataInputStream;
import edu.arizona.cs.hsyndicate.hadoop.connector.input.HSyndicateFSSeekableInputStream;
import edu.arizona.cs.hsyndicate.hadoop.connector.output.HSyndicateFSDataOutputStream;
import java.io.*;
import java.util.*;
import java.rmi.server.UID;
import java.security.MessageDigest;
import org.apache.commons.logging.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.VersionMismatchException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableName;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.MergeSort;
import org.apache.hadoop.util.PriorityQueue;

/**
 * <code>HSyndicateSequenceFile</code>s are flat files consisting of binary
 * key/value pairs.
 *
 * <p><code>HSyndicateSequenceFile</code> provides {@link Writer}, {@link Reader}
 * and {@link Sorter} classes for writing, reading and sorting respectively.</p>
 *
 * There are three
 * <code>HSyndicateSequenceFile</code>
 * <code>Writer</code>s based on the {@link CompressionType} used to compress
 * key/value pairs:
 * <ol>
 * <li>
 * <code>Writer</code> : Uncompressed records.
 * </li>
 * <li>
 * <code>RecordCompressWriter</code> : Record-compressed files, only compress
 * values.
 * </li>
 * <li>
 * <code>BlockCompressWriter</code> : Block-compressed files, both keys & values
 * are collected in 'blocks' separately and compressed. The size of the 'block'
 * is configurable.
 * </ol>
 *
 * <p>The actual compression algorithm used to compress key and/or values can be
 * specified by using the appropriate {@link CompressionCodec}.</p>
 *
 * <p>The recommended way is to use the static <tt>createWriter</tt> methods
 * provided by the
 * <code>HSyndicateSequenceFile</code> to chose the preferred format.</p>
 *
 * <p>The {@link Reader} acts as the bridge and can read any of the above
 * <code>HSyndicateSequenceFile</code> formats.</p>
 *
 * <h4 id="Formats">HSyndicateSequenceFile Formats</h4>
 *
 * <p>Essentially there are 3 different formats for
 * <code>HSyndicateSequenceFile</code>s depending on the
 * <code>CompressionType</code> specified. All of them share a
 * <a href="#Header">common header</a> described below.
 *
 * <h5 id="Header">HSyndicateSequenceFile Header</h5>
 * <ul>
 * <li>
 * version - 3 bytes of magic header <b>SEQ</b>, followed by 1 byte of actual
 * version number (e.g. SEQ4 or SEQ6)
 * </li>
 * <li>
 * keyClassName -key class
 * </li>
 * <li>
 * valueClassName - value class
 * </li>
 * <li>
 * compression - A boolean which specifies if compression is turned on for
 * keys/values in this file.
 * </li>
 * <li>
 * blockCompression - A boolean which specifies if block-compression is turned
 * on for keys/values in this file.
 * </li>
 * <li>
 * compression codec -
 * <code>CompressionCodec</code> class which is used for compression of keys
 * and/or values (if compression is enabled).
 * </li>
 * <li>
 * metadata - {@link Metadata} for this file.
 * </li>
 * <li>
 * sync - A sync marker to denote end of the header.
 * </li>
 * </ul>
 *
 * <h5 id="#UncompressedFormat">Uncompressed HSyndicateSequenceFile Format</h5>
 * <ul>
 * <li>
 * <a href="#Header">Header</a>
 * </li>
 * <li>
 * Record
 * <ul>
 * <li>Record length</li>
 * <li>Key length</li>
 * <li>Key</li>
 * <li>Value</li>
 * </ul>
 * </li>
 * <li>
 * A sync-marker every few
 * <code>100</code> bytes or so.
 * </li>
 * </ul>
 *
 * <h5 id="#RecordCompressedFormat">Record-Compressed HSyndicateSequenceFile
 Format</h5>
 * <ul>
 * <li>
 * <a href="#Header">Header</a>
 * </li>
 * <li>
 * Record
 * <ul>
 * <li>Record length</li>
 * <li>Key length</li>
 * <li>Key</li>
 * <li><i>Compressed</i> Value</li>
 * </ul>
 * </li>
 * <li>
 * A sync-marker every few
 * <code>100</code> bytes or so.
 * </li>
 * </ul>
 *
 * <h5 id="#BlockCompressedFormat">Block-Compressed HSyndicateSequenceFile
 Format</h5>
 * <ul>
 * <li>
 * <a href="#Header">Header</a>
 * </li>
 * <li>
 * Record <i>Block</i>
 * <ul>
 * <li>Compressed key-lengths block-size</li>
 * <li>Compressed key-lengths block</li>
 * <li>Compressed keys block-size</li>
 * <li>Compressed keys block</li>
 * <li>Compressed value-lengths block-size</li>
 * <li>Compressed value-lengths block</li>
 * <li>Compressed values block-size</li>
 * <li>Compressed values block</li>
 * </ul>
 * </li>
 * <li>
 * A sync-marker every few
 * <code>100</code> bytes or so.
 * </li>
 * </ul>
 *
 * <p>The compressed blocks of key lengths and value lengths consist of the
 * actual lengths of individual keys/values encoded in ZeroCompressedInteger
 * format.</p>
 *
 * @see CompressionCodec
 */
public class HSyndicateSequenceFile {

    private static final Log LOG = LogFactory.getLog(HSyndicateSequenceFile.class);

    private HSyndicateSequenceFile() {
    }                         // no public ctor
    private static final byte BLOCK_COMPRESS_VERSION = (byte) 4;
    private static final byte CUSTOM_COMPRESS_VERSION = (byte) 5;
    private static final byte VERSION_WITH_METADATA = (byte) 6;
    private static byte[] VERSION = new byte[]{
        (byte) 'S', (byte) 'E', (byte) 'Q', VERSION_WITH_METADATA
    };
    private static final int SYNC_ESCAPE = -1;      // "length" of sync entries
    private static final int SYNC_HASH_SIZE = 16;   // number of bytes in hash 
    private static final int SYNC_SIZE = 4 + SYNC_HASH_SIZE; // escape + hash
    /**
     * The number of bytes between sync points.
     */
    public static final int SYNC_INTERVAL = 100 * SYNC_SIZE;

    /**
     * The compression type used to compress key/value pairs in the
     * {@link HSyndicateSequenceFile}.
     *
     * @see HSyndicateSequenceFile.Writer
     */
    public static enum CompressionType {

        /**
         * Do not compress records.
         */
        NONE,
        /**
         * Compress values only, each separately.
         */
        RECORD,
        /**
         * Compress sequences of records together in blocks.
         */
        BLOCK
    }

    /**
     * Get the compression type for the reduce outputs
     *
     * @param job the job config to look in
     * @return the kind of compression to use
     * @deprecated Use
     * {@link org.apache.hadoop.mapred.SequenceFileOutputFormat#getOutputCompressionType(org.apache.hadoop.mapred.JobConf)}
     * to get {@link CompressionType} for job-outputs.
     */
    @Deprecated
    static public CompressionType getCompressionType(Configuration job) {
        String name = job.get("io.seqfile.compression.type");
        return name == null ? CompressionType.RECORD
                : CompressionType.valueOf(name);
    }

    /**
     * Set the compression type for sequence files.
     *
     * @param job the configuration to modify
     * @param val the new compression type (none, block, record)
     * @deprecated Use the one of the many HSyndicateSequenceFile.createWriter
     * methods to specify the {@link CompressionType} while creating the
     * {@link HSyndicateSequenceFile} or
     * {@link org.apache.hadoop.mapred.SequenceFileOutputFormat#setOutputCompressionType(org.apache.hadoop.mapred.JobConf, org.apache.hadoop.io.HSyndicateSequenceFile.CompressionType)}
     * to specify the {@link CompressionType} for job-outputs. or
     */
    @Deprecated
    static public void setCompressionType(Configuration job,
            CompressionType val) {
        job.set("io.seqfile.compression.type", val.toString());
    }

    /**
     * Construct the preferred type of HSyndicateSequenceFile Writer.
     *
     * @param fs The configured filesystem.
     * @param conf The configuration.
     * @param name The name of the file.
     * @param keyClass The 'key' type.
     * @param valClass The 'value' type.
     * @return Returns the handle to the constructed HSyndicateSequenceFile Writer.
     * @throws IOException
     */
    public static Writer createWriter(AHSyndicateFileSystemBase fs, Configuration conf, SyndicateFSPath name,
            Class keyClass, Class valClass)
            throws IOException {
        return createWriter(fs, conf, name, keyClass, valClass,
                getCompressionType(conf));
    }

    /**
     * Construct the preferred type of HSyndicateSequenceFile Writer.
     *
     * @param fs The configured filesystem.
     * @param conf The configuration.
     * @param name The name of the file.
     * @param keyClass The 'key' type.
     * @param valClass The 'value' type.
     * @param compressionType The compression type.
     * @return Returns the handle to the constructed HSyndicateSequenceFile Writer.
     * @throws IOException
     */
    public static Writer createWriter(AHSyndicateFileSystemBase fs, Configuration conf, SyndicateFSPath name,
            Class keyClass, Class valClass, CompressionType compressionType)
            throws IOException {
        return createWriter(fs, conf, name, keyClass, valClass,
                compressionType, new DefaultCodec(), null, new Metadata());
    }

    /**
     * Construct the preferred type of HSyndicateSequenceFile Writer.
     *
     * @param fs The configured filesystem.
     * @param conf The configuration.
     * @param name The name of the file.
     * @param keyClass The 'key' type.
     * @param valClass The 'value' type.
     * @param compressionType The compression type.
     * @param progress The Progressable object to track progress.
     * @return Returns the handle to the constructed HSyndicateSequenceFile Writer.
     * @throws IOException
     */
    public static Writer createWriter(AHSyndicateFileSystemBase fs, Configuration conf, SyndicateFSPath name,
            Class keyClass, Class valClass, CompressionType compressionType,
            Progressable progress) throws IOException {
        return createWriter(fs, conf, name, keyClass, valClass,
                compressionType, new DefaultCodec(), progress, new Metadata());
    }

    /**
     * Construct the preferred type of HSyndicateSequenceFile Writer.
     *
     * @param fs The configured filesystem.
     * @param conf The configuration.
     * @param name The name of the file.
     * @param keyClass The 'key' type.
     * @param valClass The 'value' type.
     * @param compressionType The compression type.
     * @param codec The compression codec.
     * @return Returns the handle to the constructed HSyndicateSequenceFile Writer.
     * @throws IOException
     */
    public static Writer createWriter(AHSyndicateFileSystemBase fs, Configuration conf, SyndicateFSPath name,
            Class keyClass, Class valClass,
            CompressionType compressionType, CompressionCodec codec)
            throws IOException {
        return createWriter(fs, conf, name, keyClass, valClass,
                compressionType, codec, null, new Metadata());
    }

    /**
     * Construct the preferred type of HSyndicateSequenceFile Writer.
     *
     * @param fs The configured filesystem.
     * @param conf The configuration.
     * @param name The name of the file.
     * @param keyClass The 'key' type.
     * @param valClass The 'value' type.
     * @param compressionType The compression type.
     * @param codec The compression codec.
     * @param progress The Progressable object to track progress.
     * @param metadata The metadata of the file.
     * @return Returns the handle to the constructed HSyndicateSequenceFile Writer.
     * @throws IOException
     */
    public static Writer createWriter(AHSyndicateFileSystemBase fs, Configuration conf, SyndicateFSPath name,
            Class keyClass, Class valClass,
            CompressionType compressionType, CompressionCodec codec,
            Progressable progress, Metadata metadata) throws IOException {
        if ((codec instanceof GzipCodec)
                && !NativeCodeLoader.isNativeCodeLoaded()
                && !ZlibFactory.isNativeZlibLoaded(conf)) {
            throw new IllegalArgumentException("SequenceFile doesn't work with "
                    + "GzipCodec without native-hadoop code!");
        }

        Writer writer = null;

        if (compressionType == CompressionType.NONE) {
            writer = new Writer(fs, conf, name, keyClass, valClass, progress, metadata);
        } else if (compressionType == CompressionType.RECORD) {
            writer = new RecordCompressWriter(fs, conf, name, keyClass, valClass, codec, progress, metadata);
        } else if (compressionType == CompressionType.BLOCK) {
            writer = new BlockCompressWriter(fs, conf, name, keyClass, valClass, codec, progress, metadata);
        }

        return writer;
    }

    /**
     * Construct the preferred type of HSyndicateSequenceFile Writer.
     *
     * @param fs The configured filesystem.
     * @param conf The configuration.
     * @param name The name of the file.
     * @param keyClass The 'key' type.
     * @param valClass The 'value' type.
     * @param createParent create parent directory if non-existent
     * @param compressionType The compression type.
     * @param codec The compression codec.
     * @param metadata The metadata of the file.
     * @return Returns the handle to the constructed HSyndicateSequenceFile Writer.
     * @throws IOException
     */
    public static Writer createWriter(AHSyndicateFileSystemBase fs, Configuration conf, SyndicateFSPath name,
            Class keyClass, Class valClass, boolean createParent,
            CompressionType compressionType, CompressionCodec codec,
            Metadata metadata) throws IOException {
        if ((codec instanceof GzipCodec)
                && !NativeCodeLoader.isNativeCodeLoaded()
                && !ZlibFactory.isNativeZlibLoaded(conf)) {
            throw new IllegalArgumentException("SequenceFile doesn't work with "
                    + "GzipCodec without native-hadoop code!");
        }

        if (createParent) {
            if (!fs.exists(name.getParent())) {
                fs.mkdirs(name.getParent());
            }
        }

        HSyndicateFSDataOutputStream fsos = new HSyndicateFSDataOutputStream(fs.getFileOutputStream(name));

        switch (compressionType) {
            case NONE:
                return new Writer(conf, fsos, keyClass, valClass, metadata).ownStream();
            case RECORD:
                return new RecordCompressWriter(conf, fsos, keyClass, valClass, codec,
                        metadata).ownStream();
            case BLOCK:
                return new BlockCompressWriter(conf, fsos, keyClass, valClass, codec,
                        metadata).ownStream();
            default:
                return null;
        }
    }

    /**
     * Construct the preferred type of HSyndicateSequenceFile Writer.
     *
     * @param fs The configured filesystem.
     * @param conf The configuration.
     * @param name The name of the file.
     * @param keyClass The 'key' type.
     * @param valClass The 'value' type.
     * @param compressionType The compression type.
     * @param codec The compression codec.
     * @param progress The Progressable object to track progress.
     * @return Returns the handle to the constructed HSyndicateSequenceFile Writer.
     * @throws IOException
     */
    public static Writer createWriter(AHSyndicateFileSystemBase fs, Configuration conf, SyndicateFSPath name,
            Class keyClass, Class valClass,
            CompressionType compressionType, CompressionCodec codec,
            Progressable progress) throws IOException {
        Writer writer = createWriter(fs, conf, name, keyClass, valClass,
                compressionType, codec, progress, new Metadata());
        return writer;
    }

    /**
     * Construct the preferred type of 'raw' HSyndicateSequenceFile Writer.
     *
     * @param out The stream on top which the writer is to be constructed.
     * @param keyClass The 'key' type.
     * @param valClass The 'value' type.
     * @param compress Compress data?
     * @param blockCompress Compress blocks?
     * @param metadata The metadata of the file.
     * @return Returns the handle to the constructed HSyndicateSequenceFile Writer.
     * @throws IOException
     */
    private static Writer createWriter(Configuration conf, HSyndicateFSDataOutputStream out,
            Class keyClass, Class valClass, boolean compress, boolean blockCompress,
            CompressionCodec codec, Metadata metadata)
            throws IOException {
        if (codec != null && (codec instanceof GzipCodec)
                && !NativeCodeLoader.isNativeCodeLoaded()
                && !ZlibFactory.isNativeZlibLoaded(conf)) {
            throw new IllegalArgumentException("SequenceFile doesn't work with "
                    + "GzipCodec without native-hadoop code!");
        }

        Writer writer = null;

        if (!compress) {
            writer = new Writer(conf, out, keyClass, valClass, metadata);
        } else if (compress && !blockCompress) {
            writer = new RecordCompressWriter(conf, out, keyClass, valClass, codec, metadata);
        } else {
            writer = new BlockCompressWriter(conf, out, keyClass, valClass, codec, metadata);
        }

        return writer;
    }

    /**
     * Construct the preferred type of 'raw' HSyndicateSequenceFile Writer.
     *
     * @param fs The configured filesystem.
     * @param conf The configuration.
     * @param file The name of the file.
     * @param keyClass The 'key' type.
     * @param valClass The 'value' type.
     * @param compress Compress data?
     * @param blockCompress Compress blocks?
     * @param codec The compression codec.
     * @param progress
     * @param metadata The metadata of the file.
     * @return Returns the handle to the constructed HSyndicateSequenceFile Writer.
     * @throws IOException
     */
    private static Writer createWriter(AHSyndicateFileSystemBase fs, Configuration conf, SyndicateFSPath file,
            Class keyClass, Class valClass,
            boolean compress, boolean blockCompress,
            CompressionCodec codec, Progressable progress, Metadata metadata)
            throws IOException {
        if (codec != null && (codec instanceof GzipCodec)
                && !NativeCodeLoader.isNativeCodeLoaded()
                && !ZlibFactory.isNativeZlibLoaded(conf)) {
            throw new IllegalArgumentException("SequenceFile doesn't work with "
                    + "GzipCodec without native-hadoop code!");
        }

        Writer writer = null;

        if (!compress) {
            writer = new Writer(fs, conf, file, keyClass, valClass, progress, metadata);
        } else if (compress && !blockCompress) {
            writer = new RecordCompressWriter(fs, conf, file, keyClass, valClass,
                    codec, progress, metadata);
        } else {
            writer = new BlockCompressWriter(fs, conf, file, keyClass, valClass,
                    codec, progress, metadata);
        }

        return writer;
    }

    /**
     * Construct the preferred type of 'raw' HSyndicateSequenceFile Writer.
     *
     * @param conf The configuration.
     * @param out The stream on top which the writer is to be constructed.
     * @param keyClass The 'key' type.
     * @param valClass The 'value' type.
     * @param compressionType The compression type.
     * @param codec The compression codec.
     * @param metadata The metadata of the file.
     * @return Returns the handle to the constructed HSyndicateSequenceFile Writer.
     * @throws IOException
     */
    public static Writer createWriter(Configuration conf, HSyndicateFSDataOutputStream out,
            Class keyClass, Class valClass, CompressionType compressionType,
            CompressionCodec codec, Metadata metadata)
            throws IOException {
        if ((codec instanceof GzipCodec)
                && !NativeCodeLoader.isNativeCodeLoaded()
                && !ZlibFactory.isNativeZlibLoaded(conf)) {
            throw new IllegalArgumentException("SequenceFile doesn't work with "
                    + "GzipCodec without native-hadoop code!");
        }

        Writer writer = null;

        if (compressionType == CompressionType.NONE) {
            writer = new Writer(conf, out, keyClass, valClass, metadata);
        } else if (compressionType == CompressionType.RECORD) {
            writer = new RecordCompressWriter(conf, out, keyClass, valClass, codec, metadata);
        } else if (compressionType == CompressionType.BLOCK) {
            writer = new BlockCompressWriter(conf, out, keyClass, valClass, codec, metadata);
        }

        return writer;
    }

    /**
     * Construct the preferred type of 'raw' HSyndicateSequenceFile Writer.
     *
     * @param conf The configuration.
     * @param out The stream on top which the writer is to be constructed.
     * @param keyClass The 'key' type.
     * @param valClass The 'value' type.
     * @param compressionType The compression type.
     * @param codec The compression codec.
     * @return Returns the handle to the constructed HSyndicateSequenceFile Writer.
     * @throws IOException
     */
    public static Writer createWriter(Configuration conf, HSyndicateFSDataOutputStream out,
            Class keyClass, Class valClass, CompressionType compressionType,
            CompressionCodec codec)
            throws IOException {
        Writer writer = createWriter(conf, out, keyClass, valClass, compressionType,
                codec, new Metadata());
        return writer;
    }

    /**
     * The interface to 'raw' values of SequenceFiles.
     */
    public static interface ValueBytes {

        /**
         * Writes the uncompressed bytes to the outStream.
         *
         * @param outStream : Stream to write uncompressed bytes into.
         * @throws IOException
         */
        public void writeUncompressedBytes(DataOutputStream outStream)
                throws IOException;

        /**
         * Write compressed bytes to outStream. Note: that it will NOT compress
         * the bytes if they are not compressed.
         *
         * @param outStream : Stream to write compressed bytes into.
         */
        public void writeCompressedBytes(DataOutputStream outStream)
                throws IllegalArgumentException, IOException;

        /**
         * Size of stored data.
         */
        public int getSize();
    }

    private static class UncompressedBytes implements ValueBytes {

        private int dataSize;
        private byte[] data;

        private UncompressedBytes() {
            data = null;
            dataSize = 0;
        }

        private void reset(DataInputStream in, int length) throws IOException {
            data = new byte[length];
            dataSize = -1;

            in.readFully(data);
            dataSize = data.length;
        }

        public int getSize() {
            return dataSize;
        }

        public void writeUncompressedBytes(DataOutputStream outStream)
                throws IOException {
            outStream.write(data, 0, dataSize);
        }

        public void writeCompressedBytes(DataOutputStream outStream)
                throws IllegalArgumentException, IOException {
            throw new IllegalArgumentException("UncompressedBytes cannot be compressed!");
        }
    } // UncompressedBytes

    private static class CompressedBytes implements ValueBytes {

        private int dataSize;
        private byte[] data;
        DataInputBuffer rawData = null;
        CompressionCodec codec = null;
        CompressionInputStream decompressedStream = null;

        private CompressedBytes(CompressionCodec codec) {
            data = null;
            dataSize = 0;
            this.codec = codec;
        }

        private void reset(DataInputStream in, int length) throws IOException {
            data = new byte[length];
            dataSize = -1;

            in.readFully(data);
            dataSize = data.length;
        }

        public int getSize() {
            return dataSize;
        }

        public void writeUncompressedBytes(DataOutputStream outStream)
                throws IOException {
            if (decompressedStream == null) {
                rawData = new DataInputBuffer();
                decompressedStream = codec.createInputStream(rawData);
            } else {
                decompressedStream.resetState();
            }
            rawData.reset(data, 0, dataSize);

            byte[] buffer = new byte[8192];
            int bytesRead = 0;
            while ((bytesRead = decompressedStream.read(buffer, 0, 8192)) != -1) {
                outStream.write(buffer, 0, bytesRead);
            }
        }

        public void writeCompressedBytes(DataOutputStream outStream)
                throws IllegalArgumentException, IOException {
            outStream.write(data, 0, dataSize);
        }
    } // CompressedBytes

    /**
     * The class encapsulating with the metadata of a file. The metadata of a
     * file is a list of attribute name/value pairs of Text type.
     *
     */
    public static class Metadata implements Writable {

        private TreeMap<Text, Text> theMetadata;

        public Metadata() {
            this(new TreeMap<Text, Text>());
        }

        public Metadata(TreeMap<Text, Text> arg) {
            if (arg == null) {
                this.theMetadata = new TreeMap<Text, Text>();
            } else {
                this.theMetadata = arg;
            }
        }

        public Text get(Text name) {
            return this.theMetadata.get(name);
        }

        public void set(Text name, Text value) {
            this.theMetadata.put(name, value);
        }

        public TreeMap<Text, Text> getMetadata() {
            return new TreeMap<Text, Text>(this.theMetadata);
        }

        public void write(DataOutput out) throws IOException {
            out.writeInt(this.theMetadata.size());
            Iterator<Map.Entry<Text, Text>> iter =
                    this.theMetadata.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Text, Text> en = iter.next();
                en.getKey().write(out);
                en.getValue().write(out);
            }
        }

        public void readFields(DataInput in) throws IOException {
            int sz = in.readInt();
            if (sz < 0) {
                throw new IOException("Invalid size: " + sz + " for file metadata object");
            }
            this.theMetadata = new TreeMap<Text, Text>();
            for (int i = 0; i < sz; i++) {
                Text key = new Text();
                Text val = new Text();
                key.readFields(in);
                val.readFields(in);
                this.theMetadata.put(key, val);
            }
        }

        public boolean equals(Metadata other) {
            if (other == null) {
                return false;
            }
            if (this.theMetadata.size() != other.theMetadata.size()) {
                return false;
            }
            Iterator<Map.Entry<Text, Text>> iter1 =
                    this.theMetadata.entrySet().iterator();
            Iterator<Map.Entry<Text, Text>> iter2 =
                    other.theMetadata.entrySet().iterator();
            while (iter1.hasNext() && iter2.hasNext()) {
                Map.Entry<Text, Text> en1 = iter1.next();
                Map.Entry<Text, Text> en2 = iter2.next();
                if (!en1.getKey().equals(en2.getKey())) {
                    return false;
                }
                if (!en1.getValue().equals(en2.getValue())) {
                    return false;
                }
            }
            if (iter1.hasNext() || iter2.hasNext()) {
                return false;
            }
            return true;
        }

        public int hashCode() {
            assert false : "hashCode not designed";
            return 42; // any arbitrary constant will do 
        }

        public String toString() {
            StringBuffer sb = new StringBuffer();
            sb.append("size: ").append(this.theMetadata.size()).append("\n");
            Iterator<Map.Entry<Text, Text>> iter =
                    this.theMetadata.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Text, Text> en = iter.next();
                sb.append("\t").append(en.getKey().toString()).append("\t").append(en.getValue().toString());
                sb.append("\n");
            }
            return sb.toString();
        }
    }

    /**
     * Write key/value pairs to a sequence-format file.
     */
    public static class Writer implements java.io.Closeable {

        Configuration conf;
        HSyndicateFSDataOutputStream out;
        boolean ownOutputStream = true;
        DataOutputBuffer buffer = new DataOutputBuffer();
        Class keyClass;
        Class valClass;
        private boolean compress;
        CompressionCodec codec = null;
        CompressionOutputStream deflateFilter = null;
        DataOutputStream deflateOut = null;
        Metadata metadata = null;
        Compressor compressor = null;
        protected Serializer keySerializer;
        protected Serializer uncompressedValSerializer;
        protected Serializer compressedValSerializer;
        // Insert a globally unique 16-byte value every few entries, so that one
        // can seek into the middle of a file and then synchronize with record
        // starts and ends by scanning for this value.
        long lastSyncPos;                     // position of last sync
        byte[] sync;                          // 16 random bytes

        {
            try {
                MessageDigest digester = MessageDigest.getInstance("MD5");
                long time = System.currentTimeMillis();
                digester.update((new UID() + "@" + time).getBytes());
                sync = digester.digest();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Implicit constructor: needed for the period of transition!
         */
        Writer() {
        }

        /**
         * Create the named file.
         */
        public Writer(AHSyndicateFileSystemBase fs, Configuration conf, SyndicateFSPath name,
                Class keyClass, Class valClass)
                throws IOException {
            this(fs, conf, name, keyClass, valClass, null, new Metadata());
        }

        /**
         * Create the named file with write-progress reporter.
         */
        public Writer(AHSyndicateFileSystemBase fs, Configuration conf, SyndicateFSPath name,
                Class keyClass, Class valClass,
                Progressable progress, Metadata metadata)
                throws IOException {
            init(name, conf,
                    new HSyndicateFSDataOutputStream(fs.getFileOutputStream(name)),
                    keyClass, valClass, false, null, metadata);
            initializeFileHeader();
            writeFileHeader();
            finalizeFileHeader();
        }

        /**
         * Write to an arbitrary stream using a specified buffer size.
         */
        Writer(Configuration conf, HSyndicateFSDataOutputStream out,
                Class keyClass, Class valClass, Metadata metadata)
                throws IOException {
            this.ownOutputStream = false;
            init(null, conf, out, keyClass, valClass, false, null, metadata);

            initializeFileHeader();
            writeFileHeader();
            finalizeFileHeader();
        }

        /**
         * Write the initial part of file header.
         */
        void initializeFileHeader()
                throws IOException {
            out.write(VERSION);
        }

        /**
         * Write the final part of file header.
         */
        void finalizeFileHeader()
                throws IOException {
            out.write(sync);                       // write the sync bytes
            out.flush();                           // flush header
        }

        boolean isCompressed() {
            return compress;
        }

        boolean isBlockCompressed() {
            return false;
        }

        Writer ownStream() {
            this.ownOutputStream = true;
            return this;
        }

        /**
         * Write and flush the file header.
         */
        void writeFileHeader()
                throws IOException {
            Text.writeString(out, keyClass.getName());
            Text.writeString(out, valClass.getName());

            out.writeBoolean(this.isCompressed());
            out.writeBoolean(this.isBlockCompressed());

            if (this.isCompressed()) {
                Text.writeString(out, (codec.getClass()).getName());
            }
            this.metadata.write(out);
        }

        /**
         * Initialize.
         */
        @SuppressWarnings("unchecked")
        void init(SyndicateFSPath name, Configuration conf, HSyndicateFSDataOutputStream out,
                Class keyClass, Class valClass,
                boolean compress, CompressionCodec codec, Metadata metadata)
                throws IOException {
            this.conf = conf;
            this.out = out;
            this.keyClass = keyClass;
            this.valClass = valClass;
            this.compress = compress;
            this.codec = codec;
            this.metadata = metadata;
            SerializationFactory serializationFactory = new SerializationFactory(conf);
            this.keySerializer = serializationFactory.getSerializer(keyClass);
            this.keySerializer.open(buffer);
            this.uncompressedValSerializer = serializationFactory.getSerializer(valClass);
            this.uncompressedValSerializer.open(buffer);
            if (this.codec != null) {
                ReflectionUtils.setConf(this.codec, this.conf);
                this.compressor = CodecPool.getCompressor(this.codec);
                this.deflateFilter = this.codec.createOutputStream(buffer, compressor);
                this.deflateOut =
                        new DataOutputStream(new BufferedOutputStream(deflateFilter));
                this.compressedValSerializer = serializationFactory.getSerializer(valClass);
                this.compressedValSerializer.open(deflateOut);
            }
        }

        /**
         * Returns the class of keys in this file.
         */
        public Class getKeyClass() {
            return keyClass;
        }

        /**
         * Returns the class of values in this file.
         */
        public Class getValueClass() {
            return valClass;
        }

        /**
         * Returns the compression codec of data in this file.
         */
        public CompressionCodec getCompressionCodec() {
            return codec;
        }

        /**
         * create a sync point
         */
        public void sync() throws IOException {
            if (sync != null && lastSyncPos != out.getPos()) {
                out.writeInt(SYNC_ESCAPE);                // mark the start of the sync
                out.write(sync);                          // write sync
                lastSyncPos = out.getPos();               // update lastSyncPos
            }
        }

        /**
         * flush all currently written data to the file system
         */
        public void syncFs() throws IOException {
            if (out != null) {
                out.sync();                               // flush contents to file system
            }
        }

        /**
         * Returns the configuration of this file.
         */
        Configuration getConf() {
            return conf;
        }

        /**
         * Close the file.
         */
        public synchronized void close() throws IOException {
            keySerializer.close();
            uncompressedValSerializer.close();
            if (compressedValSerializer != null) {
                compressedValSerializer.close();
            }

            CodecPool.returnCompressor(compressor);
            compressor = null;

            if (out != null) {

                // Close the underlying stream iff we own it...
                if (ownOutputStream) {
                    out.close();
                } else {
                    out.flush();
                }
                out = null;
            }
        }

        synchronized void checkAndWriteSync() throws IOException {
            if (sync != null
                    && out.getPos() >= lastSyncPos + SYNC_INTERVAL) { // time to emit sync
                sync();
            }
        }

        /**
         * Append a key/value pair.
         */
        public synchronized void append(Writable key, Writable val)
                throws IOException {
            append((Object) key, (Object) val);
        }

        /**
         * Append a key/value pair.
         */
        @SuppressWarnings("unchecked")
        public synchronized void append(Object key, Object val)
                throws IOException {
            if (key.getClass() != keyClass) {
                throw new IOException("wrong key class: " + key.getClass().getName()
                        + " is not " + keyClass);
            }
            if (val.getClass() != valClass) {
                throw new IOException("wrong value class: " + val.getClass().getName()
                        + " is not " + valClass);
            }

            buffer.reset();

            // Append the 'key'
            keySerializer.serialize(key);
            int keyLength = buffer.getLength();
            if (keyLength < 0) {
                throw new IOException("negative length keys not allowed: " + key);
            }

            // Append the 'value'
            if (compress) {
                deflateFilter.resetState();
                compressedValSerializer.serialize(val);
                deflateOut.flush();
                deflateFilter.finish();
            } else {
                uncompressedValSerializer.serialize(val);
            }

            // Write the record out
            checkAndWriteSync();                                // sync
            out.writeInt(buffer.getLength());                   // total record length
            out.writeInt(keyLength);                            // key portion length
            out.write(buffer.getData(), 0, buffer.getLength()); // data
        }

        public synchronized void appendRaw(byte[] keyData, int keyOffset,
                int keyLength, ValueBytes val) throws IOException {
            if (keyLength < 0) {
                throw new IOException("negative length keys not allowed: " + keyLength);
            }

            int valLength = val.getSize();

            checkAndWriteSync();

            out.writeInt(keyLength + valLength);          // total record length
            out.writeInt(keyLength);                    // key portion length
            out.write(keyData, keyOffset, keyLength);   // key
            val.writeUncompressedBytes(out);            // value
        }

        /**
         * Returns the current length of the output file.
         *
         * <p>This always returns a synchronized position. In other words,
         * immediately after calling
         * {@link HSyndicateSequenceFile.Reader#seek(long)} with a position returned
         * by this method, {@link HSyndicateSequenceFile.Reader#next(Writable)} may
         * be called. However the key may be earlier in the file than key last
         * written when this method was called (e.g., with block-compression, it
         * may be the first key in the block that was being written when this
         * method was called).
         */
        public synchronized long getLength() throws IOException {
            return out.getPos();
        }
    } // class Writer

    /**
     * Write key/compressed-value pairs to a sequence-format file.
     */
    static class RecordCompressWriter extends Writer {

        /**
         * Create the named file.
         */
        public RecordCompressWriter(AHSyndicateFileSystemBase fs, Configuration conf, SyndicateFSPath name,
                Class keyClass, Class valClass, CompressionCodec codec)
                throws IOException {
            this(conf, new HSyndicateFSDataOutputStream(fs.getFileOutputStream(name)), keyClass, valClass, codec, new Metadata());
        }

        /**
         * Create the named file with write-progress reporter.
         */
        public RecordCompressWriter(AHSyndicateFileSystemBase fs, Configuration conf, SyndicateFSPath name,
                Class keyClass, Class valClass,
                CompressionCodec codec, Progressable progress, Metadata metadata)
                throws IOException {
            super.init(name, conf,
                    new HSyndicateFSDataOutputStream(fs.getFileOutputStream(name)),
                    keyClass, valClass, true, codec, metadata);

            initializeFileHeader();
            writeFileHeader();
            finalizeFileHeader();
        }

        /**
         * Create the named file with write-progress reporter.
         */
        public RecordCompressWriter(AHSyndicateFileSystemBase fs, Configuration conf, SyndicateFSPath name,
                Class keyClass, Class valClass, CompressionCodec codec,
                Progressable progress)
                throws IOException {
            this(fs, conf, name, keyClass, valClass, codec, progress, new Metadata());
        }

        /**
         * Write to an arbitrary stream using a specified buffer size.
         */
        RecordCompressWriter(Configuration conf, HSyndicateFSDataOutputStream out,
                Class keyClass, Class valClass, CompressionCodec codec, Metadata metadata)
                throws IOException {
            this.ownOutputStream = false;
            super.init(null, conf, out, keyClass, valClass, true, codec, metadata);

            initializeFileHeader();
            writeFileHeader();
            finalizeFileHeader();

        }

        boolean isCompressed() {
            return true;
        }

        boolean isBlockCompressed() {
            return false;
        }

        /**
         * Append a key/value pair.
         */
        @SuppressWarnings("unchecked")
        public synchronized void append(Object key, Object val)
                throws IOException {
            if (key.getClass() != keyClass) {
                throw new IOException("wrong key class: " + key.getClass().getName()
                        + " is not " + keyClass);
            }
            if (val.getClass() != valClass) {
                throw new IOException("wrong value class: " + val.getClass().getName()
                        + " is not " + valClass);
            }

            buffer.reset();

            // Append the 'key'
            keySerializer.serialize(key);
            int keyLength = buffer.getLength();
            if (keyLength < 0) {
                throw new IOException("negative length keys not allowed: " + key);
            }

            // Compress 'value' and append it
            deflateFilter.resetState();
            compressedValSerializer.serialize(val);
            deflateOut.flush();
            deflateFilter.finish();

            // Write the record out
            checkAndWriteSync();                                // sync
            out.writeInt(buffer.getLength());                   // total record length
            out.writeInt(keyLength);                            // key portion length
            out.write(buffer.getData(), 0, buffer.getLength()); // data
        }

        /**
         * Append a key/value pair.
         */
        public synchronized void appendRaw(byte[] keyData, int keyOffset,
                int keyLength, ValueBytes val) throws IOException {

            if (keyLength < 0) {
                throw new IOException("negative length keys not allowed: " + keyLength);
            }

            int valLength = val.getSize();

            checkAndWriteSync();                        // sync
            out.writeInt(keyLength + valLength);          // total record length
            out.writeInt(keyLength);                    // key portion length
            out.write(keyData, keyOffset, keyLength);   // 'key' data
            val.writeCompressedBytes(out);              // 'value' data
        }
    } // RecordCompressionWriter

    /**
     * Write compressed key/value blocks to a sequence-format file.
     */
    static class BlockCompressWriter extends Writer {

        private int noBufferedRecords = 0;
        private DataOutputBuffer keyLenBuffer = new DataOutputBuffer();
        private DataOutputBuffer keyBuffer = new DataOutputBuffer();
        private DataOutputBuffer valLenBuffer = new DataOutputBuffer();
        private DataOutputBuffer valBuffer = new DataOutputBuffer();
        private int compressionBlockSize;

        /**
         * Create the named file.
         */
        public BlockCompressWriter(AHSyndicateFileSystemBase fs, Configuration conf, SyndicateFSPath name,
                Class keyClass, Class valClass, CompressionCodec codec)
                throws IOException {
            this(fs, conf, name, keyClass, valClass,
                    codec, null, new Metadata());
        }

        /**
         * Create the named file with write-progress reporter.
         */
        public BlockCompressWriter(AHSyndicateFileSystemBase fs, Configuration conf, SyndicateFSPath name,
                Class keyClass, Class valClass,
                CompressionCodec codec,
                Progressable progress, Metadata metadata)
                throws IOException {
            super.init(name, conf,
                    new HSyndicateFSDataOutputStream(fs.getFileOutputStream(name)),
                    keyClass, valClass, true, codec, metadata);
            init(1000000);

            initializeFileHeader();
            writeFileHeader();
            finalizeFileHeader();
        }

        /**
         * Create the named file with write-progress reporter.
         */
        public BlockCompressWriter(AHSyndicateFileSystemBase fs, Configuration conf, SyndicateFSPath name,
                Class keyClass, Class valClass, CompressionCodec codec,
                Progressable progress)
                throws IOException {
            this(fs, conf, name, keyClass, valClass, codec, progress, new Metadata());
        }

        /**
         * Write to an arbitrary stream using a specified buffer size.
         */
        BlockCompressWriter(Configuration conf, HSyndicateFSDataOutputStream out,
                Class keyClass, Class valClass, CompressionCodec codec, Metadata metadata)
                throws IOException {
            this.ownOutputStream = false;
            super.init(null, conf, out, keyClass, valClass, true, codec, metadata);
            init(conf.getInt("io.seqfile.compress.blocksize", 1000000));

            initializeFileHeader();
            writeFileHeader();
            finalizeFileHeader();
        }

        boolean isCompressed() {
            return true;
        }

        boolean isBlockCompressed() {
            return true;
        }

        /**
         * Initialize
         */
        void init(int compressionBlockSize) throws IOException {
            this.compressionBlockSize = compressionBlockSize;
            keySerializer.close();
            keySerializer.open(keyBuffer);
            uncompressedValSerializer.close();
            uncompressedValSerializer.open(valBuffer);
        }

        /**
         * Workhorse to check and write out compressed data/lengths
         */
        private synchronized void writeBuffer(DataOutputBuffer uncompressedDataBuffer)
                throws IOException {
            deflateFilter.resetState();
            buffer.reset();
            deflateOut.write(uncompressedDataBuffer.getData(), 0,
                    uncompressedDataBuffer.getLength());
            deflateOut.flush();
            deflateFilter.finish();

            WritableUtils.writeVInt(out, buffer.getLength());
            out.write(buffer.getData(), 0, buffer.getLength());
        }

        /**
         * Compress and flush contents to dfs
         */
        public synchronized void sync() throws IOException {
            if (noBufferedRecords > 0) {
                super.sync();

                // No. of records
                WritableUtils.writeVInt(out, noBufferedRecords);

                // Write 'keys' and lengths
                writeBuffer(keyLenBuffer);
                writeBuffer(keyBuffer);

                // Write 'values' and lengths
                writeBuffer(valLenBuffer);
                writeBuffer(valBuffer);

                // Flush the file-stream
                out.flush();

                // Reset internal states
                keyLenBuffer.reset();
                keyBuffer.reset();
                valLenBuffer.reset();
                valBuffer.reset();
                noBufferedRecords = 0;
            }

        }

        /**
         * Close the file.
         */
        public synchronized void close() throws IOException {
            if (out != null) {
                sync();
            }
            super.close();
        }

        /**
         * Append a key/value pair.
         */
        @SuppressWarnings("unchecked")
        public synchronized void append(Object key, Object val)
                throws IOException {
            if (key.getClass() != keyClass) {
                throw new IOException("wrong key class: " + key + " is not " + keyClass);
            }
            if (val.getClass() != valClass) {
                throw new IOException("wrong value class: " + val + " is not " + valClass);
            }

            // Save key/value into respective buffers 
            int oldKeyLength = keyBuffer.getLength();
            keySerializer.serialize(key);
            int keyLength = keyBuffer.getLength() - oldKeyLength;
            if (keyLength < 0) {
                throw new IOException("negative length keys not allowed: " + key);
            }
            WritableUtils.writeVInt(keyLenBuffer, keyLength);

            int oldValLength = valBuffer.getLength();
            uncompressedValSerializer.serialize(val);
            int valLength = valBuffer.getLength() - oldValLength;
            WritableUtils.writeVInt(valLenBuffer, valLength);

            // Added another key/value pair
            ++noBufferedRecords;

            // Compress and flush?
            int currentBlockSize = keyBuffer.getLength() + valBuffer.getLength();
            if (currentBlockSize >= compressionBlockSize) {
                sync();
            }
        }

        /**
         * Append a key/value pair.
         */
        public synchronized void appendRaw(byte[] keyData, int keyOffset,
                int keyLength, ValueBytes val) throws IOException {

            if (keyLength < 0) {
                throw new IOException("negative length keys not allowed");
            }

            int valLength = val.getSize();

            // Save key/value data in relevant buffers
            WritableUtils.writeVInt(keyLenBuffer, keyLength);
            keyBuffer.write(keyData, keyOffset, keyLength);
            WritableUtils.writeVInt(valLenBuffer, valLength);
            val.writeUncompressedBytes(valBuffer);

            // Added another key/value pair
            ++noBufferedRecords;

            // Compress and flush?
            int currentBlockSize = keyBuffer.getLength() + valBuffer.getLength();
            if (currentBlockSize >= compressionBlockSize) {
                sync();
            }
        }
    } // BlockCompressionWriter

    /**
     * Reads key/value pairs from a sequence-format file.
     */
    public static class Reader implements java.io.Closeable {

        private SyndicateFSPath file;
        private HSyndicateFSDataInputStream in;
        private DataOutputBuffer outBuf = new DataOutputBuffer();
        private byte version;
        private String keyClassName;
        private String valClassName;
        private Class keyClass;
        private Class valClass;
        private CompressionCodec codec = null;
        private Metadata metadata = null;
        private byte[] sync = new byte[SYNC_HASH_SIZE];
        private byte[] syncCheck = new byte[SYNC_HASH_SIZE];
        private boolean syncSeen;
        private long end;
        private int keyLength;
        private int recordLength;
        private boolean decompress;
        private boolean blockCompressed;
        private Configuration conf;
        private int noBufferedRecords = 0;
        private boolean lazyDecompress = true;
        private boolean valuesDecompressed = true;
        private int noBufferedKeys = 0;
        private int noBufferedValues = 0;
        private DataInputBuffer keyLenBuffer = null;
        private CompressionInputStream keyLenInFilter = null;
        private DataInputStream keyLenIn = null;
        private Decompressor keyLenDecompressor = null;
        private DataInputBuffer keyBuffer = null;
        private CompressionInputStream keyInFilter = null;
        private DataInputStream keyIn = null;
        private Decompressor keyDecompressor = null;
        private DataInputBuffer valLenBuffer = null;
        private CompressionInputStream valLenInFilter = null;
        private DataInputStream valLenIn = null;
        private Decompressor valLenDecompressor = null;
        private DataInputBuffer valBuffer = null;
        private CompressionInputStream valInFilter = null;
        private DataInputStream valIn = null;
        private Decompressor valDecompressor = null;
        private Deserializer keyDeserializer;
        private Deserializer valDeserializer;

        /**
         * Open the named file.
         */
        public Reader(AHSyndicateFileSystemBase fs, SyndicateFSPath file, Configuration conf)
                throws IOException {
            this(fs, file, conf, false);
        }

        private Reader(AHSyndicateFileSystemBase fs, SyndicateFSPath file,
                Configuration conf, boolean tempReader) throws IOException {
            this(fs, file, 0, fs.getSize(file), conf, tempReader);
        }

        private Reader(AHSyndicateFileSystemBase fs, SyndicateFSPath file, long start,
                long length, Configuration conf, boolean tempReader)
                throws IOException {
            this.file = file;
            this.in = openFile(fs, file, length);
            this.conf = conf;
            boolean succeeded = false;
            try {
                seek(start);
                this.end = in.getPos() + length;
                init(tempReader);
                succeeded = true;
            } finally {
                if (!succeeded) {
                    IOUtils.cleanup(LOG, in);
                }
            }
        }

        /**
         * Override this method to specialize the type of
         * {@link FSDataInputStream} returned.
         */
        protected HSyndicateFSDataInputStream openFile(AHSyndicateFileSystemBase fs, SyndicateFSPath file,
                long length) throws IOException {
            return new HSyndicateFSDataInputStream(new HSyndicateFSSeekableInputStream(fs.getFileInputStream(file)));
        }

        /**
         * Initialize the {@link Reader}
         *
         * @param tmpReader <code>true</code> if we are constructing a temporary
         * reader {@link HSyndicateSequenceFile.Sorter.cloneFileAttributes}, and
         * hence do not initialize every component; <code>false</code>
         * otherwise.
         * @throws IOException
         */
        private void init(boolean tempReader) throws IOException {
            byte[] versionBlock = new byte[VERSION.length];
            in.readFully(versionBlock);

            if ((versionBlock[0] != VERSION[0])
                    || (versionBlock[1] != VERSION[1])
                    || (versionBlock[2] != VERSION[2])) {
                throw new IOException(file + " not a SequenceFile");
            }

            // Set 'version'
            version = versionBlock[3];
            if (version > VERSION[3]) {
                throw new VersionMismatchException(VERSION[3], version);
            }

            if (version < BLOCK_COMPRESS_VERSION) {
                UTF8 className = new UTF8();

                className.readFields(in);
                keyClassName = className.toString(); // key class name

                className.readFields(in);
                valClassName = className.toString(); // val class name
            } else {
                keyClassName = Text.readString(in);
                valClassName = Text.readString(in);
            }

            if (version > 2) {                          // if version > 2
                this.decompress = in.readBoolean();       // is compressed?
            } else {
                decompress = false;
            }

            if (version >= BLOCK_COMPRESS_VERSION) {    // if version >= 4
                this.blockCompressed = in.readBoolean();  // is block-compressed?
            } else {
                blockCompressed = false;
            }

            // if version >= 5
            // setup the compression codec
            if (decompress) {
                if (version >= CUSTOM_COMPRESS_VERSION) {
                    String codecClassname = Text.readString(in);
                    try {
                        Class<? extends CompressionCodec> codecClass = conf.getClassByName(codecClassname).asSubclass(CompressionCodec.class);
                        this.codec = ReflectionUtils.newInstance(codecClass, conf);
                    } catch (ClassNotFoundException cnfe) {
                        throw new IllegalArgumentException("Unknown codec: "
                                + codecClassname, cnfe);
                    }
                } else {
                    codec = new DefaultCodec();
                    ((Configurable) codec).setConf(conf);
                }
            }

            this.metadata = new Metadata();
            if (version >= VERSION_WITH_METADATA) {    // if version >= 6
                this.metadata.readFields(in);
            }

            if (version > 1) {                          // if version > 1
                in.readFully(sync);                       // read sync bytes
            }

            // Initialize... *not* if this we are constructing a temporary Reader
            if (!tempReader) {
                valBuffer = new DataInputBuffer();
                if (decompress) {
                    valDecompressor = CodecPool.getDecompressor(codec);
                    valInFilter = codec.createInputStream(valBuffer, valDecompressor);
                    valIn = new DataInputStream(valInFilter);
                } else {
                    valIn = valBuffer;
                }

                if (blockCompressed) {
                    keyLenBuffer = new DataInputBuffer();
                    keyBuffer = new DataInputBuffer();
                    valLenBuffer = new DataInputBuffer();

                    keyLenDecompressor = CodecPool.getDecompressor(codec);
                    keyLenInFilter = codec.createInputStream(keyLenBuffer,
                            keyLenDecompressor);
                    keyLenIn = new DataInputStream(keyLenInFilter);

                    keyDecompressor = CodecPool.getDecompressor(codec);
                    keyInFilter = codec.createInputStream(keyBuffer, keyDecompressor);
                    keyIn = new DataInputStream(keyInFilter);

                    valLenDecompressor = CodecPool.getDecompressor(codec);
                    valLenInFilter = codec.createInputStream(valLenBuffer,
                            valLenDecompressor);
                    valLenIn = new DataInputStream(valLenInFilter);
                }

                SerializationFactory serializationFactory =
                        new SerializationFactory(conf);
                this.keyDeserializer =
                        getDeserializer(serializationFactory, getKeyClass());
                if (!blockCompressed) {
                    this.keyDeserializer.open(valBuffer);
                } else {
                    this.keyDeserializer.open(keyIn);
                }
                this.valDeserializer =
                        getDeserializer(serializationFactory, getValueClass());
                this.valDeserializer.open(valIn);
            }
        }

        @SuppressWarnings("unchecked")
        private Deserializer getDeserializer(SerializationFactory sf, Class c) {
            return sf.getDeserializer(c);
        }

        /**
         * Close the file.
         */
        public synchronized void close() throws IOException {
            // Return the decompressors to the pool
            CodecPool.returnDecompressor(keyLenDecompressor);
            CodecPool.returnDecompressor(keyDecompressor);
            CodecPool.returnDecompressor(valLenDecompressor);
            CodecPool.returnDecompressor(valDecompressor);
            keyLenDecompressor = keyDecompressor = null;
            valLenDecompressor = valDecompressor = null;

            if (keyDeserializer != null) {
                keyDeserializer.close();
            }
            if (valDeserializer != null) {
                valDeserializer.close();
            }

            // Close the input-stream
            in.close();
        }

        /**
         * Returns the name of the key class.
         */
        public String getKeyClassName() {
            return keyClassName;
        }

        /**
         * Returns the class of keys in this file.
         */
        public synchronized Class<?> getKeyClass() {
            if (null == keyClass) {
                try {
                    keyClass = WritableName.getClass(getKeyClassName(), conf);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return keyClass;
        }

        /**
         * Returns the name of the value class.
         */
        public String getValueClassName() {
            return valClassName;
        }

        /**
         * Returns the class of values in this file.
         */
        public synchronized Class<?> getValueClass() {
            if (null == valClass) {
                try {
                    valClass = WritableName.getClass(getValueClassName(), conf);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return valClass;
        }

        /**
         * Returns true if values are compressed.
         */
        public boolean isCompressed() {
            return decompress;
        }

        /**
         * Returns true if records are block-compressed.
         */
        public boolean isBlockCompressed() {
            return blockCompressed;
        }

        /**
         * Returns the compression codec of data in this file.
         */
        public CompressionCodec getCompressionCodec() {
            return codec;
        }

        /**
         * Returns the metadata object of the file
         */
        public Metadata getMetadata() {
            return this.metadata;
        }

        /**
         * Returns the configuration used for this file.
         */
        Configuration getConf() {
            return conf;
        }

        /**
         * Read a compressed buffer
         */
        private synchronized void readBuffer(DataInputBuffer buffer,
                CompressionInputStream filter) throws IOException {
            // Read data into a temporary buffer
            DataOutputBuffer dataBuffer = new DataOutputBuffer();

            try {
                int dataBufferLength = WritableUtils.readVInt(in);
                dataBuffer.write(in, dataBufferLength);

                // Set up 'buffer' connected to the input-stream
                buffer.reset(dataBuffer.getData(), 0, dataBuffer.getLength());
            } finally {
                dataBuffer.close();
            }

            // Reset the codec
            filter.resetState();
        }

        /**
         * Read the next 'compressed' block
         */
        private synchronized void readBlock() throws IOException {
            // Check if we need to throw away a whole block of 
            // 'values' due to 'lazy decompression' 
            if (lazyDecompress && !valuesDecompressed) {
                in.seek(WritableUtils.readVInt(in) + in.getPos());
                in.seek(WritableUtils.readVInt(in) + in.getPos());
            }

            // Reset internal states
            noBufferedKeys = 0;
            noBufferedValues = 0;
            noBufferedRecords = 0;
            valuesDecompressed = false;

            //Process sync
            if (sync != null) {
                in.readInt();
                in.readFully(syncCheck);                // read syncCheck
                if (!Arrays.equals(sync, syncCheck)) // check it
                {
                    throw new IOException("File is corrupt!");
                }
            }
            syncSeen = true;

            // Read number of records in this block
            noBufferedRecords = WritableUtils.readVInt(in);

            // Read key lengths and keys
            readBuffer(keyLenBuffer, keyLenInFilter);
            readBuffer(keyBuffer, keyInFilter);
            noBufferedKeys = noBufferedRecords;

            // Read value lengths and values
            if (!lazyDecompress) {
                readBuffer(valLenBuffer, valLenInFilter);
                readBuffer(valBuffer, valInFilter);
                noBufferedValues = noBufferedRecords;
                valuesDecompressed = true;
            }
        }

        /**
         * Position valLenIn/valIn to the 'value' corresponding to the 'current'
         * key
         */
        private synchronized void seekToCurrentValue() throws IOException {
            if (!blockCompressed) {
                if (decompress) {
                    valInFilter.resetState();
                }
                valBuffer.reset();
            } else {
                // Check if this is the first value in the 'block' to be read
                if (lazyDecompress && !valuesDecompressed) {
                    // Read the value lengths and values
                    readBuffer(valLenBuffer, valLenInFilter);
                    readBuffer(valBuffer, valInFilter);
                    noBufferedValues = noBufferedRecords;
                    valuesDecompressed = true;
                }

                // Calculate the no. of bytes to skip
                // Note: 'current' key has already been read!
                int skipValBytes = 0;
                int currentKey = noBufferedKeys + 1;
                for (int i = noBufferedValues; i > currentKey; --i) {
                    skipValBytes += WritableUtils.readVInt(valLenIn);
                    --noBufferedValues;
                }

                // Skip to the 'val' corresponding to 'current' key
                if (skipValBytes > 0) {
                    if (valIn.skipBytes(skipValBytes) != skipValBytes) {
                        throw new IOException("Failed to seek to " + currentKey
                                + "(th) value!");
                    }
                }
            }
        }

        /**
         * Get the 'value' corresponding to the last read 'key'.
         *
         * @param val : The 'value' to be read.
         * @throws IOException
         */
        public synchronized void getCurrentValue(Writable val)
                throws IOException {
            if (val instanceof Configurable) {
                ((Configurable) val).setConf(this.conf);
            }

            // Position stream to 'current' value
            seekToCurrentValue();

            if (!blockCompressed) {
                val.readFields(valIn);

                if (valIn.read() > 0) {
                    LOG.info("available bytes: " + valIn.available());
                    throw new IOException(val + " read " + (valBuffer.getPosition() - keyLength)
                            + " bytes, should read "
                            + (valBuffer.getLength() - keyLength));
                }
            } else {
                // Get the value
                int valLength = WritableUtils.readVInt(valLenIn);
                val.readFields(valIn);

                // Read another compressed 'value'
                --noBufferedValues;

                // Sanity check
                if (valLength < 0) {
                    LOG.debug(val + " is a zero-length value");
                }
            }

        }

        /**
         * Get the 'value' corresponding to the last read 'key'.
         *
         * @param val : The 'value' to be read.
         * @throws IOException
         */
        public synchronized Object getCurrentValue(Object val)
                throws IOException {
            if (val instanceof Configurable) {
                ((Configurable) val).setConf(this.conf);
            }

            // Position stream to 'current' value
            seekToCurrentValue();

            if (!blockCompressed) {
                val = deserializeValue(val);

                if (valIn.read() > 0) {
                    LOG.info("available bytes: " + valIn.available());
                    throw new IOException(val + " read " + (valBuffer.getPosition() - keyLength)
                            + " bytes, should read "
                            + (valBuffer.getLength() - keyLength));
                }
            } else {
                // Get the value
                int valLength = WritableUtils.readVInt(valLenIn);
                val = deserializeValue(val);

                // Read another compressed 'value'
                --noBufferedValues;

                // Sanity check
                if (valLength < 0) {
                    LOG.debug(val + " is a zero-length value");
                }
            }
            return val;

        }

        @SuppressWarnings("unchecked")
        private Object deserializeValue(Object val) throws IOException {
            return valDeserializer.deserialize(val);
        }

        /**
         * Read the next key in the file into
         * <code>key</code>, skipping its value. True if another entry exists,
         * and false at end of file.
         */
        public synchronized boolean next(Writable key) throws IOException {
            if (key.getClass() != getKeyClass()) {
                throw new IOException("wrong key class: " + key.getClass().getName()
                        + " is not " + keyClass);
            }

            if (!blockCompressed) {
                outBuf.reset();

                keyLength = next(outBuf);
                if (keyLength < 0) {
                    return false;
                }

                valBuffer.reset(outBuf.getData(), outBuf.getLength());

                key.readFields(valBuffer);
                valBuffer.mark(0);
                if (valBuffer.getPosition() != keyLength) {
                    throw new IOException(key + " read " + valBuffer.getPosition()
                            + " bytes, should read " + keyLength);
                }
            } else {
                //Reset syncSeen
                syncSeen = false;

                if (noBufferedKeys == 0) {
                    try {
                        readBlock();
                    } catch (EOFException eof) {
                        return false;
                    }
                }

                int keyLength = WritableUtils.readVInt(keyLenIn);

                // Sanity check
                if (keyLength < 0) {
                    return false;
                }

                //Read another compressed 'key'
                key.readFields(keyIn);
                --noBufferedKeys;
            }

            return true;
        }

        /**
         * Read the next key/value pair in the file into
         * <code>key</code> and
         * <code>val</code>. Returns true if such a pair exists and false when
         * at end of file
         */
        public synchronized boolean next(Writable key, Writable val)
                throws IOException {
            if (val.getClass() != getValueClass()) {
                throw new IOException("wrong value class: " + val + " is not " + valClass);
            }

            boolean more = next(key);

            if (more) {
                getCurrentValue(val);
            }

            return more;
        }

        /**
         * Read and return the next record length, potentially skipping over a
         * sync block.
         *
         * @return the length of the next record or -1 if there is no next
         * record
         * @throws IOException
         */
        private synchronized int readRecordLength() throws IOException {
            if (in.getPos() >= end) {
                return -1;
            }
            int length = in.readInt();
            if (version > 1 && sync != null
                    && length == SYNC_ESCAPE) {              // process a sync entry
                in.readFully(syncCheck);                // read syncCheck
                if (!Arrays.equals(sync, syncCheck)) // check it
                {
                    throw new IOException("File is corrupt!");
                }
                syncSeen = true;
                if (in.getPos() >= end) {
                    return -1;
                }
                length = in.readInt();                  // re-read length
            } else {
                syncSeen = false;
            }

            return length;
        }

        /**
         * Read the next key/value pair in the file into
         * <code>buffer</code>. Returns the length of the key read, or -1 if at
         * end of file. The length of the value may be computed by calling
         * buffer.getLength() before and after calls to this method.
         */
        /**
         * @deprecated Call
         * {@link #nextRaw(DataOutputBuffer,HSyndicateSequenceFile.ValueBytes)}.
         */
        public synchronized int next(DataOutputBuffer buffer) throws IOException {
            // Unsupported for block-compressed sequence files
            if (blockCompressed) {
                throw new IOException("Unsupported call for block-compressed"
                        + " SequenceFiles - use SequenceFile.Reader.next(DataOutputStream, ValueBytes)");
            }
            try {
                int length = readRecordLength();
                if (length == -1) {
                    return -1;
                }
                int keyLength = in.readInt();
                buffer.write(in, length);
                return keyLength;
            } catch (ChecksumException e) {             // checksum failure
                handleChecksumException(e);
                return next(buffer);
            }
        }

        public ValueBytes createValueBytes() {
            ValueBytes val = null;
            if (!decompress || blockCompressed) {
                val = new UncompressedBytes();
            } else {
                val = new CompressedBytes(codec);
            }
            return val;
        }

        /**
         * Read 'raw' records.
         *
         * @param key - The buffer into which the key is read
         * @param val - The 'raw' value
         * @return Returns the total record length or -1 for end of file
         * @throws IOException
         */
        public synchronized int nextRaw(DataOutputBuffer key, ValueBytes val)
                throws IOException {
            if (!blockCompressed) {
                int length = readRecordLength();
                if (length == -1) {
                    return -1;
                }
                int keyLength = in.readInt();
                int valLength = length - keyLength;
                key.write(in, keyLength);
                if (decompress) {
                    CompressedBytes value = (CompressedBytes) val;
                    value.reset(in, valLength);
                } else {
                    UncompressedBytes value = (UncompressedBytes) val;
                    value.reset(in, valLength);
                }

                return length;
            } else {
                //Reset syncSeen
                syncSeen = false;

                // Read 'key'
                if (noBufferedKeys == 0) {
                    if (in.getPos() >= end) {
                        return -1;
                    }

                    try {
                        readBlock();
                    } catch (EOFException eof) {
                        return -1;
                    }
                }
                int keyLength = WritableUtils.readVInt(keyLenIn);
                if (keyLength < 0) {
                    throw new IOException("zero length key found!");
                }
                key.write(keyIn, keyLength);
                --noBufferedKeys;

                // Read raw 'value'
                seekToCurrentValue();
                int valLength = WritableUtils.readVInt(valLenIn);
                UncompressedBytes rawValue = (UncompressedBytes) val;
                rawValue.reset(valIn, valLength);
                --noBufferedValues;

                return (keyLength + valLength);
            }

        }

        /**
         * Read 'raw' keys.
         *
         * @param key - The buffer into which the key is read
         * @return Returns the key length or -1 for end of file
         * @throws IOException
         */
        public int nextRawKey(DataOutputBuffer key)
                throws IOException {
            if (!blockCompressed) {
                recordLength = readRecordLength();
                if (recordLength == -1) {
                    return -1;
                }
                keyLength = in.readInt();
                key.write(in, keyLength);
                return keyLength;
            } else {
                //Reset syncSeen
                syncSeen = false;

                // Read 'key'
                if (noBufferedKeys == 0) {
                    if (in.getPos() >= end) {
                        return -1;
                    }

                    try {
                        readBlock();
                    } catch (EOFException eof) {
                        return -1;
                    }
                }
                int keyLength = WritableUtils.readVInt(keyLenIn);
                if (keyLength < 0) {
                    throw new IOException("zero length key found!");
                }
                key.write(keyIn, keyLength);
                --noBufferedKeys;

                return keyLength;
            }

        }

        /**
         * Read the next key in the file, skipping its value. Return null at end
         * of file.
         */
        public synchronized Object next(Object key) throws IOException {
            if (key != null && key.getClass() != getKeyClass()) {
                throw new IOException("wrong key class: " + key.getClass().getName()
                        + " is not " + keyClass);
            }

            if (!blockCompressed) {
                outBuf.reset();

                keyLength = next(outBuf);
                if (keyLength < 0) {
                    return null;
                }

                valBuffer.reset(outBuf.getData(), outBuf.getLength());

                key = deserializeKey(key);
                valBuffer.mark(0);
                if (valBuffer.getPosition() != keyLength) {
                    throw new IOException(key + " read " + valBuffer.getPosition()
                            + " bytes, should read " + keyLength);
                }
            } else {
                //Reset syncSeen
                syncSeen = false;

                if (noBufferedKeys == 0) {
                    try {
                        readBlock();
                    } catch (EOFException eof) {
                        return null;
                    }
                }

                int keyLength = WritableUtils.readVInt(keyLenIn);

                // Sanity check
                if (keyLength < 0) {
                    return null;
                }

                //Read another compressed 'key'
                key = deserializeKey(key);
                --noBufferedKeys;
            }

            return key;
        }

        @SuppressWarnings("unchecked")
        private Object deserializeKey(Object key) throws IOException {
            return keyDeserializer.deserialize(key);
        }

        /**
         * Read 'raw' values.
         *
         * @param val - The 'raw' value
         * @return Returns the value length
         * @throws IOException
         */
        public synchronized int nextRawValue(ValueBytes val)
                throws IOException {

            // Position stream to current value
            seekToCurrentValue();

            if (!blockCompressed) {
                int valLength = recordLength - keyLength;
                if (decompress) {
                    CompressedBytes value = (CompressedBytes) val;
                    value.reset(in, valLength);
                } else {
                    UncompressedBytes value = (UncompressedBytes) val;
                    value.reset(in, valLength);
                }

                return valLength;
            } else {
                int valLength = WritableUtils.readVInt(valLenIn);
                UncompressedBytes rawValue = (UncompressedBytes) val;
                rawValue.reset(valIn, valLength);
                --noBufferedValues;
                return valLength;
            }

        }

        private void handleChecksumException(ChecksumException e)
                throws IOException {
            if (this.conf.getBoolean("io.skip.checksum.errors", false)) {
                LOG.warn("Bad checksum at " + getPosition() + ". Skipping entries.");
                sync(getPosition() + this.conf.getInt("io.bytes.per.checksum", 512));
            } else {
                throw e;
            }
        }

        /**
         * Set the current byte position in the input file.
         *
         * <p>The position passed must be a position returned by {@link
         * HSyndicateSequenceFile.Writer#getLength()} when writing this file. To
         * seek to an arbitrary position, use
         * {@link HSyndicateSequenceFile.Reader#sync(long)}.
         */
        public synchronized void seek(long position) throws IOException {
            in.seek(position);
            if (blockCompressed) {                      // trigger block read
                noBufferedKeys = 0;
                valuesDecompressed = true;
            }
        }

        /**
         * Seek to the next sync mark past a given position.
         */
        public synchronized void sync(long position) throws IOException {
            if (position + SYNC_SIZE >= end) {
                seek(end);
                return;
            }

            try {
                seek(position + 4);                         // skip escape
                in.readFully(syncCheck);
                int syncLen = sync.length;
                for (int i = 0; in.getPos() < end; i++) {
                    int j = 0;
                    for (; j < syncLen; j++) {
                        if (sync[j] != syncCheck[(i + j) % syncLen]) {
                            break;
                        }
                    }
                    if (j == syncLen) {
                        in.seek(in.getPos() - SYNC_SIZE);     // position before sync
                        return;
                    }
                    syncCheck[i % syncLen] = in.readByte();
                }
            } catch (ChecksumException e) {             // checksum failure
                handleChecksumException(e);
            }
        }

        /**
         * Returns true iff the previous call to next passed a sync mark.
         */
        public boolean syncSeen() {
            return syncSeen;
        }

        /**
         * Return the current byte position in the input file.
         */
        public synchronized long getPosition() throws IOException {
            return in.getPos();
        }

        /**
         * Returns the name of the file.
         */
        public String toString() {
            return file.toString();
        }
    }
} // SequenceFile
