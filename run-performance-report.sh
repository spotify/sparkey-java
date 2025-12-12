#!/bin/bash
set -e

# Performance Report Runner - Uses JMH for scientific benchmarking
# Compares different SparkeyReader implementations
#
# Usage:
#   ./run-performance-report.sh [--quick|--full]
#
# Options:
#   --quick    Fast smoke test (1 warmup × 1s, 5 measurements × 1s) - ~1 min
#   --full     Full benchmark (3 warmup × 2s, 10 measurements × 2s) - ~4 min (default)

# Parse command line arguments
MODE="full"
if [ "$1" = "--quick" ]; then
  MODE="quick"
  WARMUP_ITERATIONS=1
  WARMUP_TIME=1
  MEASUREMENT_ITERATIONS=5
  MEASUREMENT_TIME=1
elif [ "$1" = "--full" ] || [ -z "$1" ]; then
  MODE="full"
  WARMUP_ITERATIONS=3
  WARMUP_TIME=2
  MEASUREMENT_ITERATIONS=10
  MEASUREMENT_TIME=2
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
if [ ${PIPESTATUS[0]} -ne 0 ]; then
  echo "ERROR: Maven build failed"
  exit 1
fi
echo "Build complete"
echo ""

# Build classpath with MRJAR
echo "Building classpath..."
mvn dependency:build-classpath -Dmdep.outputFile=/tmp/sparkey-cp.txt -q 2>&1 | grep -v "WARNING.*sun.misc.Unsafe" | grep -v "WARNING.*terminally deprecated" | grep -v "WARNING.*will be removed" | grep -v "WARNING.*Please consider reporting" || true
if [ ${PIPESTATUS[0]} -ne 0 ]; then
  echo "ERROR: Failed to build classpath"
  exit 1
fi
# Build classpaths
# For JMH annotation processor: need target/classes first to resolve symbols during compilation
COMPILE_CP="target/classes:target/test-classes:$(cat /tmp/sparkey-cp.txt)"
# For benchmark runtime: need MRJAR first to expose Java 22 classes
JAR_FILE=$(ls target/sparkey-*.jar | grep -v "javadoc\|sources" | head -1)
RUNTIME_CP="$JAR_FILE:target/classes:target/test-classes:$(cat /tmp/sparkey-cp.txt)"
echo "Classpath ready"
echo ""

# Run JMH annotation processor
echo "Running JMH annotation processor..."
mkdir -p target/generated-test-sources/jmh
JMH_PROC_CP="$HOME/.m2/repository/org/openjdk/jmh/jmh-generator-annprocess/1.37/jmh-generator-annprocess-1.37.jar:$HOME/.m2/repository/org/openjdk/jmh/jmh-core/1.37/jmh-core-1.37.jar"
javac -cp "$COMPILE_CP" \
  -processorpath "$JMH_PROC_CP" \
  -processor org.openjdk.jmh.generators.BenchmarkProcessor \
  -proc:only \
  -s target/generated-test-sources/jmh \
  -d target/test-classes \
  src/test/java/com/spotify/sparkey/system/FocusedReaderBenchmark.java 2>&1 | grep -v "^Note:" || true
if [ ${PIPESTATUS[0]} -ne 0 ]; then
  echo "ERROR: JMH annotation processor failed"
  exit 1
fi
echo "JMH sources generated"
echo ""

# Compile generated sources
echo "Compiling generated JMH sources..."
find target/generated-test-sources/jmh -name "*.java" -print0 | xargs -0 javac -cp "$COMPILE_CP" -d target/test-classes 2>&1 | grep -v "^Note:" || true
if [ ${PIPESTATUS[0]} -ne 0 ]; then
  echo "ERROR: Failed to compile generated JMH sources"
  exit 1
fi
echo "JMH sources compiled"
echo ""

# Run JMH benchmark
echo "Running JMH ReaderComparisonBenchmark..."
echo ""
echo "Configuration:"
echo "  - Benchmark: FocusedReaderBenchmark (scenario-based testing)"
echo "  - Scenarios:"
echo "    1. Uncompressed Single-Threaded (J8 vs J22)"
echo "    2. Uncompressed Multi-Threaded 8, 16 (Immutable vs Pooled)"
echo "    3. Compressed Multi-Threaded 8, 16 (SNAPPY, ZSTD)"
echo "    4. Stress Test 32 threads (Uncompressed)"
echo "    5. Value Size Comparison (0, 50, 1000 bytes)"
echo "  - Entries: 100,000"
echo "  - Warmup: ${WARMUP_ITERATIONS} iterations × ${WARMUP_TIME}s = $((WARMUP_ITERATIONS * WARMUP_TIME))s per benchmark"
echo "  - Measurement: ${MEASUREMENT_ITERATIONS} iterations × ${MEASUREMENT_TIME}s = $((MEASUREMENT_ITERATIONS * MEASUREMENT_TIME))s per benchmark"
echo ""

# Create benchmark-results directory if it doesn't exist
mkdir -p benchmark-results

OUTPUT_FILE="benchmark-results/performance-report-$(date +%Y%m%d-%H%M%S).txt"

# Test all available reader types (defined in @Param annotation)
# Readers that don't support multithreading will skip multithreaded benchmarks (validation in setup)
# Readers not available on current JVM will be skipped automatically
JMH_PARAMS=""
echo ""

java -cp "$RUNTIME_CP" \
  --enable-native-access=ALL-UNNAMED \
  --add-opens=jdk.unsupported/sun.misc=ALL-UNNAMED \
  org.openjdk.jmh.Main \
  FocusedReaderBenchmark \
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
