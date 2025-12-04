#### (next version)
* **Performance optimization**: New `readFullyCompare()` method combines reading and comparing bytes
  in a single operation, avoiding temporary buffer allocation and data copying. Provides 11-17%
  improvement in high-concurrency uncompressed workloads and 2-8% improvement in most other scenarios.
* **SIMD optimization**: Byte array comparisons now use vectorized instructions (AVX2/AVX-512) on
  Java 9+ via Multi-Release JAR. Provides 4-13% improvement in single-threaded and low-concurrency
  scenarios (up to 8 threads). Java 8 continues to use the standard byte-by-byte comparison.
* **PooledSparkeyReader improvements**: Optimized with lock-free atomic operations for better
  performance under high contention.
* Fix thread-safety bug in PooledSparkeyReader.getAsEntry() where the entry object could be shared
  across threads.
* Internal simplification: ThreadLocalSparkeyReader now extends PooledSparkeyReader to reduce code
  duplication.
* Build improvements: Added comprehensive JMH benchmarking infrastructure and performance testing tools.
* Dependency updates:
  - Bump commons-io from 2.7 to 2.14.0
  - Bump guava from 29.0-jre to 32.0.0-jre
  - Bump snappy-java from 1.1.7.2 to 1.1.10.4
  - Bump logback-classic from 1.2.3 to 1.2.13
  - Upgrade zstd-jni from 1.5.2-2 to 1.5.2-5
  - Upgrade slf4j-api from 1.7.2 to 1.7.36

#### 3.3.0
* **New PooledSparkeyReader**: Sparkey.open() now returns PooledSparkeyReader instead
  of ThreadLocalSparkeyReader by default. This provides better memory safety for Java 21+
  applications using virtual threads, where ThreadLocalSparkeyReader can cause unbounded
  memory growth. Performance is on par with ThreadLocalSparkeyReader.
* PooledSparkeyReader uses thread-ID-based striping with a bounded pool of readers,
  providing O(pool size) memory usage instead of O(threads).
* ThreadLocalSparkeyReader is now deprecated but still available via
  Sparkey.openThreadLocalReader() for backward compatibility.
* New factory methods: Sparkey.openPooledReader(file) and
  Sparkey.openPooledReader(file, poolSize) for explicit pool configuration.
* Fix ByteBufferCleaner to avoid deprecated sun.misc.Unsafe.invokeCleaner on Java 19+.
  Uses version-specific cleaners: Java 8 uses sun.reflect, Java 9-18 uses sun.misc.Unsafe.invokeCleaner,
  and Java 19+ uses a no-op implementation (automatic cleanup via JVM's internal Cleaner API).
* Performance improvement on Java 19+: Skip both cleanup loop and 100ms sleep when manual cleanup
  is not needed, providing faster close operations.

#### 3.2.4
* Fix for ThreadLocalSparkeyReader due to changed behavior of
  ThreadLocal for tasks in ForkJoinPool.commonPool() in Java 16+
* Fixed bug where calling isLoaded on a duplicated MappedByteBuffer
  throws exception.

#### 3.2.2
* Updated version of zstd-jni.
  See https://github.com/spotify/sparkey-java/issues/52

#### 3.2.1
* Fixed bug where creating hash files would erroneously lose some keys.
  The bug only applies to cases where the construction mode is SORTING
  and hash collisions are present (so typically only when hash mode is
  32 bits and the number of keys is more than 100000).
  The bug was introduced along with SORTING in 2.3.0

#### 3.2.0
* Added methods to reader: `getLoadedBytes()` and `getTotalBytes()`

#### 3.1.0
* Added support for zstd compression

#### 3.0.1
* Fix bug where file-descriptors are not closed after using a Sparkey iterator.

#### 3.0.0
* Compiles as Java 8, up from Java 6.
* Removed Guava as a dependency.
* Upgraded Snappy dependency.
* Optimized sorting-based hash file creation slightly.
* Changed API from ListenableFuture to CompletionStage in ReloadableSparkeyReader.

#### 2.3.2
* Add automatic-module-name for better module support.
* Running close() on a SparkeyReader will now synchronously unmap the files.
  This may block for 100 ms if the reader has been duplicated (typically when used from multiple threads).
* Fixed some bugs with unclosed files in certain cases. As an effect of this, it is more stable on Windows. 

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
