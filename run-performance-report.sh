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

# Clean and compile
echo "Compiling project..."
mvn clean test-compile -q
echo "Build complete"
echo ""

# Build classpath
echo "Building classpath..."
mvn dependency:build-classpath -Dmdep.outputFile=cp.txt -q
CP="target/test-classes:target/classes:$(cat cp.txt)"
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
echo "  - Uncompressed: ImmutableSparkeyReader, SingleThreadedSparkeyReader, PooledSparkeyReader"
echo "  - Compressed (Snappy): SingleThreadedSparkeyReader, PooledSparkeyReader"
echo "  - Entries: 100,000"
echo "  - Warmup: 3 iterations x 2 seconds"
echo "  - Measurement: 5 iterations x 2 seconds"
echo ""

OUTPUT_FILE="performance-report-$(date +%Y%m%d-%H%M%S).txt"

java -cp "$CP" org.openjdk.jmh.Main \
  ReaderComparisonBenchmark \
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
