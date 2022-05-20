This is the java version of sparkey. It's not a binding, but a full reimplementation.
See [Sparkey](http://github.com/spotify/sparkey) for more documentation on how it works.

### Travis
Continuous integration with [travis](https://travis-ci.org/spotify/sparkey-java).

[![Build Status](https://travis-ci.org/spotify/sparkey-java.svg?branch=master)](https://travis-ci.org/spotify/sparkey-java)

### Dependencies

* Java 6 or higher
* Maven

### Building

    mvn package

### Changelog
See [changelog](CHANGELOG.md).

### Usage

Sparkey is meant to be used as a library embedded in other software.

To import it with maven, use this for Java 8 or higher:

    <dependency>
      <groupId>com.spotify.sparkey</groupId>
      <artifactId>sparkey</artifactId>
      <version>3.2.4</version>
    </dependency>

Use this for Java 6 or 7:

    <dependency>
      <groupId>com.spotify.sparkey</groupId>
      <artifactId>sparkey</artifactId>
      <version>2.3.2</version>
    </dependency>

To help get started, take a look at the
[API documentation](http://spotify.github.io/sparkey-java/apidocs/2.0.0-SNAPSHOT/)
or an example usage: [SparkeyExample](src/test/java/com/spotify/sparkey/system/SparkeyExample.java)

### License

Apache License, Version 2.0

### Performance

This data is the direct output from running the benchmarks via IntelliJ, using the JMH plugin
on a machine with `Intel(R) Core(TM) i7-7500U CPU @ 2.70GHz`

#### Random lookups

    Benchmark             (numElements)  (type)   Mode  Cnt        Score         Error  Units
    LookupBenchmark.test           1000    NONE  thrpt    4  4768851.493 ±  934818.190  ops/s
    LookupBenchmark.test          10000    NONE  thrpt    4  4404571.185 ±  952648.631  ops/s
    LookupBenchmark.test         100000    NONE  thrpt    4  3892021.755 ±  560657.509  ops/s
    LookupBenchmark.test        1000000    NONE  thrpt    4  2748648.598 ± 1345168.410  ops/s
    LookupBenchmark.test       10000000    NONE  thrpt    4  1921539.397 ±   64678.755  ops/s
    LookupBenchmark.test      100000000    NONE  thrpt    4     8576.763 ±   10666.193  ops/s
    LookupBenchmark.test           1000  SNAPPY  thrpt    4   776222.884 ±   46536.257  ops/s
    LookupBenchmark.test          10000  SNAPPY  thrpt    4   707242.387 ±  460934.026  ops/s
    LookupBenchmark.test         100000  SNAPPY  thrpt    4   651857.795 ±  975531.050  ops/s
    LookupBenchmark.test        1000000  SNAPPY  thrpt    4   791848.718 ±   19363.131  ops/s
    LookupBenchmark.test       10000000  SNAPPY  thrpt    4   700438.201 ±   27910.579  ops/s
    LookupBenchmark.test      100000000  SNAPPY  thrpt    4   681790.103 ±   45388.918  ops/s

Here we see that performance goes down slightly as more elements are added. This is caused by:
* More frequent CPU cache misses.
* More page faults.

If you can mlock the full dataset, the performance should be more predictable.

#### Appending data

    Benchmark                   (type)   Mode  Cnt         Score         Error  Units
    AppendBenchmark.testMedium    NONE  thrpt    4   1595668.598 ±  850581.241  ops/s
    AppendBenchmark.testMedium  SNAPPY  thrpt    4    919539.081 ±  205638.008  ops/s
    AppendBenchmark.testSmall     NONE  thrpt    4   9800098.075 ±  707288.665  ops/s
    AppendBenchmark.testSmall   SNAPPY  thrpt    4  20227222.636 ± 7567353.506  ops/s

This is mostly disk bound, CPU is not fully utilized. So even though Snappy uses more CPU, it's still faster
because there's less data to write to disk, especially when the keys and values are small.
(The score is number of appends, not amount of data, so this may be slightly misleading)
    
#### Writing hash file

    Benchmark                (constructionMethod)  (numElements)  Mode  Cnt   Score   Error  Units
    WriteHashBenchmark.test             IN_MEMORY           1000    ss    4   0.003 ± 0.001   s/op
    WriteHashBenchmark.test             IN_MEMORY          10000    ss    4   0.009 ± 0.016   s/op
    WriteHashBenchmark.test             IN_MEMORY         100000    ss    4   0.035 ± 0.088   s/op
    WriteHashBenchmark.test             IN_MEMORY        1000000    ss    4   0.273 ± 0.120   s/op
    WriteHashBenchmark.test             IN_MEMORY       10000000    ss    4   4.862 ± 0.970   s/op
    WriteHashBenchmark.test               SORTING           1000    ss    4   0.005 ± 0.033   s/op
    WriteHashBenchmark.test               SORTING          10000    ss    4   0.014 ± 0.008   s/op
    WriteHashBenchmark.test               SORTING         100000    ss    4   0.070 ± 0.111   s/op
    WriteHashBenchmark.test               SORTING        1000000    ss    4   0.835 ± 0.310   s/op
    WriteHashBenchmark.test               SORTING       10000000    ss    4  13.919 ± 1.908   s/op

Writing using the in-memory method is faster, but only works if you can keep the full hash file in memory while
building it. Sorting is about 3x slower, but is more reliably for very large data sets.
