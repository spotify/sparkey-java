#!/bin/bash
set -e

# Performance Report Runner - Uses JMH for scientific benchmarking
# Compares different SparkeyReader implementations
#
# Usage:
#   ./run-performance-report.sh [--quick|--full] [JMH options...]
#
# Options:
#   --quick    Fast smoke test (1 warmup, 3 measurements) - ~1 min
#   --full     Full benchmark (2 warmup, 5 measurements) - ~4 min (default)
#
# Examples:
#   ./run-performance-report.sh
#   ./run-performance-report.sh --quick
#   ./run-performance-report.sh -- -p readerType=POOLED_MMAP_JDK8,POOLED_HEAP

# Parse command line arguments
MODE="full"
EXTRA_ARGS=""
if [ "$1" = "--quick" ]; then
  MODE="quick"
  WARMUP_ITERATIONS=1
  MEASUREMENT_ITERATIONS=3
  WARMUP_TIME="1"
  MEASUREMENT_TIME="1"
  shift
elif [ "$1" = "--full" ] || [ -z "$1" ]; then
  MODE="full"
  WARMUP_ITERATIONS=3
  MEASUREMENT_ITERATIONS=10
  WARMUP_TIME="1"
  MEASUREMENT_TIME="2"
  if [ "$1" = "--full" ]; then shift; fi
elif [ "$1" = "--" ]; then
  MODE="full"
  WARMUP_ITERATIONS=3
  MEASUREMENT_ITERATIONS=10
  WARMUP_TIME="1"
  MEASUREMENT_TIME="2"
  shift
else
  echo "Unknown option: $1"
  echo "Usage: $0 [--quick|--full] [-- JMH options...]"
  exit 1
fi

# Remaining args after -- are passed to JMH
if [ "$1" = "--" ]; then shift; fi
EXTRA_ARGS="$@"

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
echo "Running FocusedReaderBenchmark..."
echo ""
echo "Configuration:"
echo "  - Mode: AverageTime (ns/op per lookup)"
echo "  - Warmup: ${WARMUP_ITERATIONS} iterations"
echo "  - Measurement: ${MEASUREMENT_ITERATIONS} iterations"
echo "  - Entries: 100,000"
echo ""

# Create benchmark-results directory if it doesn't exist
mkdir -p benchmark-results

OUTPUT_FILE="benchmark-results/performance-report-$(date +%Y%m%d-%H%M%S).txt"

java -cp "$RUNTIME_CP" \
  --enable-native-access=ALL-UNNAMED \
  --add-opens=jdk.unsupported/sun.misc=ALL-UNNAMED \
  org.openjdk.jmh.Main \
  FocusedReaderBenchmark \
  -wi $WARMUP_ITERATIONS \
  -w $WARMUP_TIME \
  -i $MEASUREMENT_ITERATIONS \
  -r $MEASUREMENT_TIME \
  -rf text \
  -rff "$OUTPUT_FILE" \
  $EXTRA_ARGS \
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
  cat "$OUTPUT_FILE"
fi

echo ""
echo "For full results, see: $OUTPUT_FILE"
echo ""
