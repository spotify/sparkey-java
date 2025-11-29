# Sparkey Performance Benchmark

This directory contains performance benchmarking tools for comparing different Sparkey reader implementations.

## Quick Start

### JMH Benchmark (Recommended)

For scientific, statistically rigorous benchmarking using JMH:

```bash
./run-performance-report.sh
```

This produces publication-quality results with proper warmup, statistical analysis, and confidence intervals.

### Simple Benchmark

For quick performance checks without JMH overhead:

```bash
./benchmark-performance.sh
```

This will:
1. Compile the project
2. Create test files (100K entries each):
   - Uncompressed
   - Snappy-compressed
3. Benchmark all reader implementations
4. Print a summary report

## What It Measures

The benchmark tests all available reader implementations for the current branch:

### Common Readers (all branches)

- **SingleThreadedSparkeyReader** - Generic single-threaded reader
- **PooledSparkeyReader** - Thread-safe pooled reader
- **ThreadLocalSparkeyReader** - Thread-safe reader with thread-local instances

### Branch-specific Readers

- **ImmutableSparkeyReader** - Zero-overhead thread-safe reader (java21-4.0.0 branch only, uncompressed only)

Note: The exact set of readers tested depends on which branch you're running the benchmark on.

## Benchmark Configuration

Default settings (in `PerformanceBenchmark.java`):
- Entries: 100,000
- Warmup iterations: 100,000
- Benchmark iterations: 1,000,000

Modify these constants to adjust the benchmark parameters.

## Sample Output

```
Sparkey Performance Benchmark
=============================

Configuration:
  Entries: 100000
  Warmup iterations: 100000
  Benchmark iterations: 1000000

Creating test file: NONE (uncompressed)
  Log size: 8.77 MB
  Index size: 1.53 MB
  Total size: 10.30 MB

Creating test file: SNAPPY (snappy)
  Log size: 4.23 MB
  Index size: 1.53 MB
  Total size: 5.76 MB

=============================
UNCOMPRESSED FILES
=============================

Benchmarking ImmutableSparkeyReader...
  Warming up... done
  Measuring performance... done
  Result: 89.45 ns/lookup

Benchmarking SingleThreadedSparkeyReader...
  Warming up... done
  Measuring performance... done
  Result: 91.23 ns/lookup

Benchmarking PooledSparkeyReader...
  Warming up... done
  Measuring performance... done
  Result: 95.67 ns/lookup

=============================
COMPRESSED FILES (Snappy)
=============================

Benchmarking SingleThreadedSparkeyReader...
  Warming up... done
  Measuring performance... done
  Result: 156.34 ns/lookup

Benchmarking PooledSparkeyReader...
  Warming up... done
  Measuring performance... done
  Result: 162.45 ns/lookup

=============================
SUMMARY
=============================

Uncompressed Performance:
-----------------------
Implementation                      ns/lookup   vs Fastest
---------------------------------------------------------
ImmutableSparkeyReader                  89.45   (fastest)
SingleThreadedSparkeyReader             91.23       1.02x
PooledSparkeyReader                     95.67       1.07x

Compressed Performance:
---------------------
Implementation                      ns/lookup   vs Fastest
---------------------------------------------------------
SingleThreadedSparkeyReader            156.34   (fastest)
PooledSparkeyReader                    162.45       1.04x

=============================
Benchmark Complete!
=============================
```

## Interpretation

- **ns/lookup**: Average time per lookup in nanoseconds (lower is better)
- **vs Fastest**: Slowdown factor compared to fastest implementation

### Expected Results

**Uncompressed:**
- ImmutableSparkeyReader should be fastest (~90 ns/lookup)
- SingleThreadedSparkeyReader slightly slower (~2% overhead)
- PooledSparkeyReader adds pooling overhead (~5-10%)

**Compressed:**
- SingleThreadedSparkeyReader and PooledSparkeyReader similar
- Both ~60-70% slower than uncompressed due to decompression

## JMH Benchmark Details

The JMH benchmark (`run-performance-report.sh`) provides:

- **Statistical rigor**: Proper warmup, multiple iterations, confidence intervals
- **JVM warmup**: Ensures JIT compilation is complete
- **Fork isolation**: Each benchmark runs in separate JVM
- **Outlier detection**: Automatically identifies and reports anomalies

### JMH Output

The benchmark produces a timestamped report file with detailed results:

```
Benchmark                                                         Mode  Cnt     Score     Error  Units
ReaderComparisonBenchmark.lookupRandom:·gc.alloc.rate.norm       avgt    5    ≈ 10⁻³            B/op
ReaderComparisonBenchmark.lookupRandom                            avgt    5   89.234 ±   2.156  ns/op
  (compressionType=NONE, readerType=ImmutableSparkeyReader)
ReaderComparisonBenchmark.lookupRandom                            avgt    5   91.456 ±   1.823  ns/op
  (compressionType=NONE, readerType=SingleThreadedSparkeyReader)
...
```

### Customizing JMH Parameters

Edit `ReaderComparisonBenchmark.java` to change:
- `@Param("100000")` - Number of entries
- `@Param({"NONE", "SNAPPY"})` - Compression types to test
- `@Warmup(iterations = 3, time = 2)` - Warmup configuration
- `@Measurement(iterations = 5, time = 2)` - Measurement configuration

## Advanced Usage

Run the simple benchmark directly with custom parameters:

```bash
mvn exec:java \
  -Dexec.mainClass="com.spotify.sparkey21.benchmark.PerformanceBenchmark" \
  -Dexec.classpathScope=test
```

Or integrate into your own code:

```java
import com.spotify.sparkey21.benchmark.PerformanceBenchmark;

public class MyBenchmark {
  public static void main(String[] args) throws Exception {
    new PerformanceBenchmark().run();
  }
}
```
