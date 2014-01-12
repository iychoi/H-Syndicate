HSynth
======

HSynth is a system for scientific data processing. HSynth works on Hadoop system and provides modules for processing big scientific data processing.

Storage
-------

HSynth provides a Hadoop Distributed File System (HDFS) driver for Syndicate. Syndicate, a CDN-backed Distributed Filesystem, provides decentralized, scalable, fast, flexible and secure storage for scientific research data.

See the [Syndicate](https://github.com/jcnelson/syndicate) project for details.

Data Processing
---------------

Currently this project focuses on bioinformatics and provides features for genomic data processing.

HSynth supports following input formats.
* Fasta / Compressed Fasta
* HDF5 (incomplete)

Reading HDF5 file data on Hadoop MapReduce is working on the separated project [HDF5HadoopReader](https://github.com/iychoi/HDF5HadoopReader).

HSynth supports fasta processing tools.
* K-mer index builder
* ID index builder
* K-mer searcher

Build
-----

Building from the source code is very simple. All source code is in Java and written with "NetBeans". If you are using NetBeans IDE, load the project and build through the IDE. Or, simple type "ant".

```
$ ant
```

All dependencies for this project are already in /libs/ directory.


