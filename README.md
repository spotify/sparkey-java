This is the java version of sparkey. It's not a binding, but a full reimplementation.
See [Sparkey](http://github.com/spotify/sparkey) for more documentation on how it works.

Dependencies
------------

* Java 6 or higher
* Maven

Building
--------
    mvn package

Running the benchmark test
--------------------------
    mvn exec:java -Dexec.mainClass="com.spotify.sparkey.system.Benchmark" -Dexec.classpathScope="test"

Usage
-----
Sparkey is meant to be used as a library embedded in other software.
Take a look at the [API documentation](http://spotify.github.io/sparkey-java/apidocs/2.0.0-SNAPSHOT/) which gives examples on how to use it.

License
-------
Apache License, Version 2.0

Performance
-----------
This data is the direct output from running

    mvn exec:java -Dexec.mainClass="com.spotify.sparkey.system.Benchmark" -Dexec.classpathScope="test"

on the same machine ((Intel(R) Xeon(R) CPU L5630 @ 2.13GHz))
as the performance benchmark for the sparkey c implementation, so the numbers should
be somewhat comparable.

    Testing bulk insert of 1000 elements and 1000.000 random lookups
      Candidate: Sparkey NONE
        creation time (wall):         0.06
        throughput (puts/wallsec):    17241.38
        file size:                    28384
        lookup time (wall):           0.70
        throughput (lookups/wallsec): 1430615.16

    Testing bulk insert of 1000.000 elements and 1000.000 random lookups
      Candidate: Sparkey NONE
        creation time (wall):         1.11
        throughput (puts/wallsec):    899280.58
        file size:                    34177984
        lookup time (wall):           0.85
        throughput (lookups/wallsec): 1179245.28

    Testing bulk insert of 10.000.000 elements and 1000.000 random lookups
      Candidate: Sparkey NONE
        creation time (wall):         10.40
        throughput (puts/wallsec):    961815.91
        file size:                    413778012
        lookup time (wall):           1.09
        throughput (lookups/wallsec): 916590.28

    Testing bulk insert of 100.000.000 elements and 1000.000 random lookups
      Candidate: Sparkey NONE
        creation time (wall):         124.08
        throughput (puts/wallsec):    805905.68
        file size:                    4857777992
        lookup time (wall):           2.20
        throughput (lookups/wallsec): 455580.87

    Testing bulk insert of 1000 elements and 1000.000 random lookups
      Candidate: Sparkey SNAPPY
        creation time (wall):         0.11
        throughput (puts/wallsec):    9523.81
        file size:                    19085
        lookup time (wall):           2.56
        throughput (lookups/wallsec): 390930.41

    Testing bulk insert of 1000.000 elements and 1000.000 random lookups
      Candidate: Sparkey SNAPPY
        creation time (wall):         1.19
        throughput (puts/wallsec):    843170.32
        file size:                    24368687
        lookup time (wall):           2.48
        throughput (lookups/wallsec): 403225.81

    Testing bulk insert of 10.000.000 elements and 1000.000 random lookups
      Candidate: Sparkey SNAPPY
        creation time (wall):         13.49
        throughput (puts/wallsec):    741344.80
        file size:                    311872219
        lookup time (wall):           2.64
        throughput (lookups/wallsec): 379075.06

    Testing bulk insert of 100.000.000 elements and 1000.000 random lookups
      Candidate: Sparkey SNAPPY
        creation time (wall):         139.14
        throughput (puts/wallsec):    718705.75
        file size:                    3162865465
        lookup time (wall):           3.34
        throughput (lookups/wallsec): 299850.07

