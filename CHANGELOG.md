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
