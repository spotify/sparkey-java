# Sparkey Performance Benchmark

This directory contains performance benchmarking tools for comparing different Sparkey reader implementations.

## Quick Start

### Running the Benchmark

For scientific, statistically rigorous benchmarking using JMH:

```bash
# Quick smoke test (~5 minutes)
./run-performance-report.sh --quick

# Full benchmark (~15 minutes)
./run-performance-report.sh --full
```

This produces publication-quality results with proper warmup, statistical analysis, and confidence intervals.

## What It Measures

The benchmark tests all available reader implementations across multiple dimensions:

### Reader Types

The benchmark automatically discovers and tests all available reader implementations:

**Java 8+ (always available):**
- **SINGLE_THREADED_MMAP_JDK8** - Generic single-threaded reader using MappedByteBuffer
- **POOLED_MMAP_JDK8** - Thread-safe pooled reader using MappedByteBuffer

**Java 22+ (when running on Java 22+):**
- **UNCOMPRESSED_MEMORYSEGMENT_J22** - Uncompressed-only reader using MemorySegment API (zero-copy)
- **SINGLE_THREADED_MEMORYSEGMENT_J22** - Single-threaded reader using MemorySegment API
- **POOLED_MEMORYSEGMENT_J22** - Thread-safe pooled reader using MemorySegment API

### Test Parameters

- **Compression**: NONE (uncompressed), SNAPPY
- **Value sizes**: Small (~6 bytes), Large (~56 bytes)
- **Concurrency**: Single-threaded, Multi-threaded (8, 16, 32 threads)
- **Entries**: 100,000 key-value pairs

## Benchmark Configuration

The benchmark is implemented as a JMH test: `src/test/java/com/spotify/sparkey/system/ReaderComparisonBenchmark.java`

### Default Settings

**Quick mode (`--quick`):**
- Warmup: 1 iteration × 1 second = 1s per benchmark
- Measurement: 5 iterations × 1 second = 5s per benchmark
- Total per benchmark: ~6 seconds
- Total time: ~1 minute for all configurations
- Expected error: 5-10% (good for smoke testing)

**Full mode (`--full`, default):**
- Warmup: 3 iterations × 2 seconds = 6s per benchmark
- Measurement: 10 iterations × 2 seconds = 20s per benchmark
- Total per benchmark: ~26 seconds
- Total time: ~4 minutes for all configurations
- Expected error: <5% for most benchmarks, <10% for high-contention multithreaded

Each iteration runs millions of operations on modern hardware, providing excellent statistical significance with predictable runtime.

### Output

Results are saved to timestamped files in `benchmark-results/`:
```
benchmark-results/performance-report-20250115-143022.txt
```

## Sample Output

```
Benchmark                                                          Mode  Cnt    Score    Error  Units
ReaderComparisonBenchmark.lookupRandomSingleThreaded              avgt    5   89.234 ±  2.156  ns/op
  (compressionType=NONE, readerType=UNCOMPRESSED_MEMORYSEGMENT_J22, valuePadding=0)
ReaderComparisonBenchmark.lookupRandomSingleThreaded              avgt    5   91.456 ±  1.823  ns/op
  (compressionType=NONE, readerType=SINGLE_THREADED_MMAP_JDK8, valuePadding=0)
ReaderComparisonBenchmark.lookupRandomSingleThreaded              avgt    5  156.340 ± 12.456  ns/op
  (compressionType=SNAPPY, readerType=SINGLE_THREADED_MMAP_JDK8, valuePadding=0)
ReaderComparisonBenchmark.lookupRandomMultithreaded               avgt    5  112.567 ±  8.234  ns/op
  (compressionType=NONE, readerType=POOLED_MEMORYSEGMENT_J22, valuePadding=0, threads=8)
```

## Interpretation

- **Score**: Average time per lookup in nanoseconds (lower is better)
- **Error**: 99.9% confidence interval
- **Mode**: `avgt` = average time

### Expected Results

**Uncompressed (NONE):**
- Java 22+ MemorySegment readers: ~85-95 ns/lookup (fastest, zero-copy)
- Java 8 MappedByteBuffer readers: ~90-100 ns/lookup (baseline)
- Performance gap widens with larger values due to zero-copy streaming

**Compressed (SNAPPY):**
- All readers similar performance (~150-170 ns/lookup)
- Decompression overhead dominates, memory access optimization less visible

**Multi-threaded:**
- Pooled readers scale well across threads
- Single-threaded readers not available in multi-threaded benchmarks

## Advanced Usage

### Customizing Parameters

Edit `src/test/java/com/spotify/sparkey/system/ReaderComparisonBenchmark.java`:

```java
@Param({"100000"})  // Number of entries
public int numElements;

@Param({"NONE", "SNAPPY"})  // Compression types
public String compressionType;

@Param({"SINGLE_THREADED_MMAP_JDK8", "POOLED_MMAP_JDK8", ...})
public String readerType;

@Param({"0", "50"})  // Value padding (0=small, 50=large)
public int valuePadding;
```

### Running Specific Benchmarks

To run only specific parameter combinations, modify `run-performance-report.sh` to add JMH filters:

```bash
# Only test uncompressed
-p compressionType=NONE

# Only test specific reader
-p readerType=UNCOMPRESSED_MEMORYSEGMENT_J22

# Multiple filters
-p compressionType=NONE -p valuePadding=0
```

### Manual JMH Invocation

If you need full control, run JMH manually after building:

```bash
mvn clean package -DskipTests
java -cp "target/test-classes:target/sparkey-*.jar:..." \
  org.openjdk.jmh.Main \
  ReaderComparisonBenchmark \
  -wi 3 -w 2 \
  -i 10 -r 2
```

Options:
- `-wi <count>` - Number of warmup iterations
- `-w <seconds>` - Time per warmup iteration
- `-i <count>` - Number of measurement iterations
- `-r <seconds>` - Time per measurement iteration
- Add `-p <param>=<value>` to filter specific configurations

See `run-performance-report.sh` for the exact classpath construction.
