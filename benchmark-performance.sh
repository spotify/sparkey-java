#!/bin/bash
# Sparkey Performance Benchmark Runner
# Compares performance of different reader implementations

set -e

cd "$(dirname "$0")"

echo "======================================"
echo "Sparkey Performance Benchmark"
echo "======================================"
echo

# Compile project
echo "Compiling project..."
mvn compile -q

# Run benchmark
echo "Running benchmarks..."
echo
mvn exec:java -q \
  -Dexec.mainClass="com.spotify.sparkey21.benchmark.PerformanceBenchmark" \
  -Dexec.classpathScope=test

echo
echo "======================================"
echo "Benchmark Complete!"
echo "======================================"