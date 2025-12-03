#!/bin/bash
set -e

# Performance Report Runner - Uses JMH for scientific benchmarking
# Compares different SparkeyReader implementations

cd "$(dirname "$0")"

echo "==================================================================="
echo "Sparkey Reader Performance Report (JMH)"
echo "==================================================================="
echo ""

# Extract JAVA_HOME from Maven
JAVA_HOME_LINE=$(mvn --version 2>&1 | grep "Java home:" || echo "")
if [ -n "$JAVA_HOME_LINE" ]; then
  MAVEN_JAVA_HOME=$(echo "$JAVA_HOME_LINE" | sed 's/Java home: //')
  export PATH="$MAVEN_JAVA_HOME/bin:$PATH"
fi

# Show Java/Maven version
echo "Maven/Java version:"
mvn --version 2>&1 | grep -E "^(Apache Maven|Java version|Java home)" || mvn --version 2>&1 | head -3
echo ""

echo "==================================================================="
echo "Building and running JMH benchmarks..."
echo "==================================================================="
echo ""

# Clean and build JAR (with MRJAR support for Java 22)
echo "Building multi-release JAR..."
mvn clean package -DskipTests -q
echo "Build complete"
echo ""

# Build classpath with MRJAR
echo "Building classpath..."
mvn dependency:build-classpath -Dmdep.outputFile=cp.txt -q
CP="target/test-classes:target/sparkey-3.3.1-SNAPSHOT.jar:$(cat cp.txt)"
echo "Classpath ready"
echo ""

# Run JMH annotation processor
echo "Running JMH annotation processor..."
mkdir -p target/generated-test-sources/jmh
JMH_PROC_CP="$HOME/.m2/repository/org/openjdk/jmh/jmh-generator-annprocess/1.21/jmh-generator-annprocess-1.21.jar:$HOME/.m2/repository/org/openjdk/jmh/jmh-core/1.21/jmh-core-1.21.jar"
javac -cp "$CP" \
  -processorpath "$JMH_PROC_CP" \
  -processor org.openjdk.jmh.generators.BenchmarkProcessor \
  -proc:only \
  -s target/generated-test-sources/jmh \
  -d target/test-classes \
  src/test/java/com/spotify/sparkey/system/ReaderComparisonBenchmark.java 2>&1 | grep -v "^Note:" || true
echo "JMH sources generated"
echo ""

# Compile generated sources
echo "Compiling generated JMH sources..."
find target/generated-test-sources/jmh -name "*.java" -print0 | xargs -0 javac -cp "$CP" -d target/test-classes 2>&1 | grep -v "^Note:" || true
echo "JMH sources compiled"
echo ""

# Run JMH benchmark
echo "Running JMH ReaderComparisonBenchmark..."
echo ""
echo "Configuration:"
echo "  - Reader types: SingleThreaded_MMap_JDK8, Pooled_MMap_JDK8"
echo "  - Compression: NONE (uncompressed), SNAPPY"
echo "  - Value sizes: 0 (small ~6 bytes), 50 (large ~56 bytes)"
echo "  - Entries: 100,000"
echo "  - Benchmarks: Single-threaded and Multi-threaded (8, 16, 32 threads)"
echo "  - Warmup: 3 iterations x 3 seconds (9s total warmup)"
echo "  - Measurement: 5 iterations x 3 seconds (15s total measurement)"
echo "  - Estimated time: ~18 minutes"
echo ""

# Create benchmark-results directory if it doesn't exist
mkdir -p benchmark-results

OUTPUT_FILE="benchmark-results/performance-report-$(date +%Y%m%d-%H%M%S).txt"

# Only test MappedByteBuffer-based readers (JDK 8+)
# Java 22+ MemorySegment readers are in a separate branch
JMH_PARAMS="-p readerType=SINGLE_THREADED_MMAP_JDK8,POOLED_MMAP_JDK8"
echo ""

java -cp "$CP" org.openjdk.jmh.Main \
  ReaderComparisonBenchmark \
  $JMH_PARAMS \
  -e 'lookupRandomMultithreaded.*compressionType=SNAPPY' \
  -rf text \
  -rff "$OUTPUT_FILE" \
  2>&1 | tee /dev/tty

echo ""
echo "==================================================================="
echo "Benchmark Complete!"
echo "==================================================================="
echo ""
echo "Results saved to: $OUTPUT_FILE"
echo ""

# Pretty print summary if possible
if [ -f "$OUTPUT_FILE" ]; then
  echo "Summary:"
  echo "--------"
  grep "^Benchmark\|^ReaderComparison" "$OUTPUT_FILE" | head -20
fi

echo ""
echo "For full results, see: $OUTPUT_FILE"
echo ""
