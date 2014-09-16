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

To import it with maven, use this:

    <dependency>
      <groupId>com.spotify.sparkey</groupId>
      <artifactId>sparkey</artifactId>
      <version>2.0.3</version>
    </dependency>

To help get started, take a look at the
[API documentation](http://spotify.github.io/sparkey-java/apidocs/2.0.0-SNAPSHOT/)
or an example usage: [SparkeyExample](src/test/java/com/spotify/sparkey/system/SparkeyExample.java)

### License

Apache License, Version 2.0

### Performance

This data is the direct output from running

    mvn clean package install && (cd benchmark; mvn clean package)
    scp benchmark/target/microbenchmarks.jar $TESTMACHINE:
    
and then running this on the test machine:

    java -jar microbenchmarks.jar com.spotify.sparkey.system.*.*

on the same machine ((Intel(R) Xeon(R) CPU L5630 @ 2.13GHz))
as the performance benchmark for the sparkey c implementation, so the numbers should
be somewhat comparable.

    Benchmark                            (numElements) (type)   Mode   Samples         Mean   Mean error    Units
    c.s.s.s.AppendBenchmark.testMedium             N/A   NONE  thrpt       100   560514.462    11164.792    ops/s
    c.s.s.s.AppendBenchmark.testMedium             N/A SNAPPY  thrpt       100   287809.906     5216.655    ops/s
    c.s.s.s.AppendBenchmark.testSmall              N/A   NONE  thrpt       100  2530425.989   106629.449    ops/s
    c.s.s.s.AppendBenchmark.testSmall              N/A SNAPPY  thrpt       100  2909965.740   114540.700    ops/s
    
    c.s.s.s.LookupBenchmark.test                  1000   NONE  thrpt       100  1583592.318    44701.721    ops/s
    c.s.s.s.LookupBenchmark.test                  1000 SNAPPY  thrpt       100   401894.168     6929.453    ops/s
    c.s.s.s.LookupBenchmark.test                 10000   NONE  thrpt       100  1505772.744    44702.055    ops/s
    c.s.s.s.LookupBenchmark.test                 10000 SNAPPY  thrpt       100   417876.461     7232.855    ops/s
    c.s.s.s.LookupBenchmark.test                100000   NONE  thrpt       100  1328646.838    35313.306    ops/s
    c.s.s.s.LookupBenchmark.test                100000 SNAPPY  thrpt       100   422015.707     5738.393    ops/s
    c.s.s.s.LookupBenchmark.test               1000000   NONE  thrpt       100  1132310.981    34490.731    ops/s
    c.s.s.s.LookupBenchmark.test               1000000 SNAPPY  thrpt       100   387936.344     6120.736    ops/s
    c.s.s.s.LookupBenchmark.test              10000000   NONE  thrpt       100   963257.371    15601.812    ops/s
    c.s.s.s.LookupBenchmark.test              10000000 SNAPPY  thrpt       100   388512.642     1823.866    ops/s
    c.s.s.s.LookupBenchmark.test             100000000   NONE  thrpt        80   764810.198    23815.241    ops/s
    c.s.s.s.LookupBenchmark.test             100000000 SNAPPY  thrpt       100   367202.525     4695.112    ops/s
    
    c.s.s.s.WriteHashBenchmark.test          100000000    N/A     ss       100       86.003        2.437        s
    c.s.s.s.WriteHashBenchmark.test           10000000    N/A     ss       100        6.772        0.116        s
    c.s.s.s.WriteHashBenchmark.test            1000000    N/A     ss       100        0.424        0.012        s
    c.s.s.s.WriteHashBenchmark.test             100000    N/A     ss       100        0.046        0.000        s
    c.s.s.s.WriteHashBenchmark.test              10000    N/A     ss       100        0.006        0.001        s
    c.s.s.s.WriteHashBenchmark.test               1000    N/A     ss       100        0.008        0.001        s

Some notes on the results:
* The AppendBenchmark is bottlenecking on disk write rather than CPU.
* The lookup performance degrades somewhat as more elements are added. It is unclear exactly what causes this,
  but it is likely a combination of page cache misses, cpu cache misses and algorithmic complexity of the hash algorithm.
* The writeHash performance appears to be mostly linear, the actual superlinear behaviour is possibly due to
  page cache misses and algorithmic complexity of the hash algorithm.
