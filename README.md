HSynth
======

A system for connecting Hadoop MapReduce to 3rd party distributed file systems

Supported File Systems
- Syndicate, a CDN-backed Distributed Filesystems (via Syndicate-ipc)
- Any mountable file systems (including Fuse)

Provides Interfaces for Hadoop
- Connector Interface : provides new I/O modules (RecordReader, RecordWriter) and FileFormats (Text, SequenceFile, MapFile, BloomMapFile) for HSynth 
- FileSystem Interface : provides an implementation of Hadoop FileSystem interface for HSynth

