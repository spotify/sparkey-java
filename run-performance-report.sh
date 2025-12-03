#!/bin/bash
set -e

# Performance Report Runner - Uses JMH for scientific benchmarking
# Compares different SparkeyReader implementations
#
# Usage:
#   ./run-performance-report.sh [--quick|--full]
#
# Options:
#   --quick    Fast smoke test (1 warmup x 1s, 2 measurement x 2s) - ~5 min
#   --full     Full benchmark (3 warmup x 3s, 5 measurement x 3s) - ~15 min (default)

# Parse command line arguments
MODE="full"
if [ "$1" = "--quick" ]; then
  MODE="quick"
  WARMUP_ITERATIONS=1
  WARMUP_TIME=1
  MEASUREMENT_ITERATIONS=2
  MEASUREMENT_TIME=2
elif [ "$1" = "--full" ] || [ -z "$1" ]; then
  MODE="full"
  WARMUP_ITERATIONS=3
  WARMUP_TIME=3
  MEASUREMENT_ITERATIONS=5
  MEASUREMENT_TIME=3
else
  echo "Unknown option: $1"
  echo "Usage: $0 [--quick|--full]"
  exit 1
fi

cd "$(dirname "$0")"

echo "==================================================================="
echo "Sparkey Reader Performance Report (JMH) - ${MODE} mode"
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

# Suppress Maven warnings from JAnsi and Guava
export MAVEN_OPTS="--enable-native-access=ALL-UNNAMED --add-opens=jdk.unsupported/sun.misc=ALL-UNNAMED"

# Clean and build JAR (with MRJAR support for Java 22)
echo "Building multi-release JAR..."
mvn clean package -DskipTests -q 2>&1 | grep -v "WARNING.*sun.misc.Unsafe" | grep -v "WARNING.*terminally deprecated" | grep -v "WARNING.*will be removed" | grep -v "WARNING.*Please consider reporting" || true
echo "Build complete"
echo ""

# Build classpath with MRJAR
echo "Building classpath..."
mvn dependency:build-classpath -Dmdep.outputFile=/tmp/sparkey-cp.txt -q 2>&1 | grep -v "WARNING.*sun.misc.Unsafe" | grep -v "WARNING.*terminally deprecated" | grep -v "WARNING.*will be removed" | grep -v "WARNING.*Please consider reporting" || true
CP="target/test-classes:target/sparkey-3.3.1-SNAPSHOT.jar:$(cat /tmp/sparkey-cp.txt)"
echo "Classpath ready"
echo ""

# Run JMH annotation processor
echo "Running JMH annotation processor..."
mkdir -p target/generated-test-sources/jmh
JMH_PROC_CP="$HOME/.m2/repository/org/openjdk/jmh/jmh-generator-annprocess/1.37/jmh-generator-annprocess-1.37.jar:$HOME/.m2/repository/org/openjdk/jmh/jmh-core/1.37/jmh-core-1.37.jar"
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
echo "  - Reader types: SingleThreaded_MMap_JDK8 (1 thread only), Pooled_MMap_JDK8 (all thread counts)"
echo "  - Compression: NONE (uncompressed), SNAPPY"
echo "  - Value sizes: 0 (small ~6 bytes), 50 (large ~56 bytes)"
echo "  - Entries: 100,000"
echo "  - Benchmarks: Single-threaded and Multi-threaded (8, 16, 32 threads)"
echo "  - Warmup: ${WARMUP_ITERATIONS} iterations x ${WARMUP_TIME} seconds"
echo "  - Measurement: ${MEASUREMENT_ITERATIONS} iterations x ${MEASUREMENT_TIME} seconds"
echo ""

# Create benchmark-results directory if it doesn't exist
mkdir -p benchmark-results

OUTPUT_FILE="benchmark-results/performance-report-$(date +%Y%m%d-%H%M%S).txt"

# Test both single-threaded and pooled readers
# Single-threaded reader will automatically skip multithreaded benchmarks (validation in setup)
# Java 22+ MemorySegment readers are in a separate branch
JMH_PARAMS="-p readerType=SINGLE_THREADED_MMAP_JDK8,POOLED_MMAP_JDK8"
echo ""

java -cp "$CP" \
  --enable-native-access=ALL-UNNAMED \
  --add-opens=jdk.unsupported/sun.misc=ALL-UNNAMED \
  org.openjdk.jmh.Main \
  ReaderComparisonBenchmark \
  $JMH_PARAMS \
  -wi $WARMUP_ITERATIONS -w $WARMUP_TIME \
  -i $MEASUREMENT_ITERATIONS -r $MEASUREMENT_TIME \
  -rf text \
  -rff "$OUTPUT_FILE" \
  2>&1 | grep --line-buffered -v "WARNING.*sun.misc.Unsafe" | grep --line-buffered -v "WARNING.*terminally deprecated" | grep --line-buffered -v "WARNING.*will be removed" | grep --line-buffered -v "WARNING.*Please consider reporting"

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
