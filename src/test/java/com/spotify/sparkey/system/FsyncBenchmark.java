/*
 * Copyright (c) 2011-2014 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.spotify.sparkey.system;

import com.spotify.sparkey.CompressionType;
import com.spotify.sparkey.Sparkey;
import com.spotify.sparkey.SparkeyWriter;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Warmup(iterations = 2)
@Measurement(iterations = 4)
@Fork(value = 1, warmups = 0)
public class FsyncBenchmark {

  private File indexFile;
  private File logFile;
  private SparkeyWriter writer;

  @Param({"NONE", "SNAPPY", "ZSTD"})
  public String type;

  @Param({"true", "false"})
  public boolean fsync;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    indexFile = new File("test.spi");
    logFile = Sparkey.getLogFile(indexFile);

    CompressionType compressionType = CompressionType.valueOf(type);

    indexFile.deleteOnExit();
    logFile.deleteOnExit();
    indexFile.delete();
    logFile.delete();

    writer = Sparkey.createNew(indexFile, compressionType, 1024);
    writer.setFsync(fsync);
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    writer.close();
    indexFile.delete();
    logFile.delete();
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(1000)
  public void testFsync() throws IOException {
    for (int i = 0; i < 1000; i++) {
      writer.put("key" , "value");
    }
    writer.flush();
  }
}
