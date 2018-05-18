#### 2.3.1
* Fix bug where file creation didnt properly close mmap immediately,
  causing subsequent rename failures on windows

#### 2.3.0
* New method of creating hash indexes added: Sorting
  By presorting the hash entries, the hash table construction can be done using less memory than the size of the hash table.
  The cost is extra CPU time and temporary disk usage.
  As a result, some new API methods have been added:
  - SparkeyWriter.setHashSeed() to manually set which seed to use, for deterministic hash indexes. (Default: random)
  - Add SparkeyWriter.setMaxMemory() to set how much memory to use for index construction (Default: free memory / 2)
  - Add SparkeyWriter.setConstructionMethod to allow for explicit configuration of creation method to use (Default: AUTO)
* Various minor optimizations
* Add dependency on com.fasterxml.util:java-merge-sort

* Performance differences:
  - Writing hash index is 2-3x slower when using sorting (but this can be avoided by setting a large max memory or explicitly
    setting construction method to IN_MEMORY.
  - Random lookups are 6% faster than in 2.2.1 for compressed data and 5-17% faster for uncompressed (more improvement for larger data sets)



#### 2.2.1
* Minor bug fix to avoid stack overflow for large read and write operations.

#### 2.2.0
* Make Sparkey.open return a thread local (i.e. thread safe) reader by default.

#### 2.1.3
* Update snappy dependency

#### 2.1.2
* Fix bug with replacing files on Windows systems.

#### 2.1.1
* Fix minor bug related to generating filenames.

#### 2.1.0
* Always close files in case of exceptions upon creation.
* Removed IOExceptions for close() for reader types
* Make SparkeyReader and SparkeyWriter implement Closeable to support try-with-resources

#### 2.0.3
* Fix bug which triggers a BufferUnderflowException in some rare cases.
* Add support for using fsync in the writer.

#### 2.0.2
* Improve detection of corrupt files.
* Fix file-descriptor leaks when closing readers and writers.
* Use JMH for benchmarks.
* Make hash sparsity configurable.
* Fix minor bug in isAt()

#### 2.0.1
* Fix race condition in ThreadLocalSparkeyReader.close() and improve GC of thread-local readers.

#### 2.0.0
* Initial public release
