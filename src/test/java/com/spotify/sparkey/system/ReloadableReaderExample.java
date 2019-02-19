/*
 * Copyright (c) 2011-2013 Spotify AB
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

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.spotify.sparkey.CompressionType;
import com.spotify.sparkey.extra.ReloadableSparkeyReader;
import com.spotify.sparkey.Sparkey;
import com.spotify.sparkey.SparkeyWriter;
import org.junit.Ignore;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Ignore
public class ReloadableReaderExample {

  private static final int ENTRIES = 1000;
  private static final CompressionType TYPE = CompressionType.NONE;

  public static void main(String[] args)
      throws IOException, InterruptedException, ExecutionException {
     run();
  }

  private static void run() throws IOException, InterruptedException, ExecutionException {
    ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());

    // create dummy log/index files, and load the reader from them
    final File logFile = new File("reloadabletest.spl");
    create(Sparkey.getIndexFile(logFile));
    final ReloadableSparkeyReader reader = ReloadableSparkeyReader.fromLogFile(logFile, executorService).toCompletableFuture().get();

    // should be ignored (same file)
    reader.load(logFile);

    // should load from second file now
    final File logFile2 = new File("reloadabletest2.spl");
    create(Sparkey.getIndexFile(logFile2));
    reader.load(logFile2);

    reader.close();
    executorService.shutdown();
    executorService.awaitTermination(10, TimeUnit.SECONDS);

    Sparkey.getIndexFile(logFile).delete();
    logFile.delete();
    Sparkey.getIndexFile(logFile2).delete();
    logFile2.delete();

    System.out.println("Done!");
  }

  private static void create(File indexFile) throws IOException {
    final SparkeyWriter writer = Sparkey.createNew(indexFile, TYPE, 512);
    for (int i = 0; i < ENTRIES; i++) {
      writer.put("Key" + i, "Value" + i);
    }
    writer.flush();
    writer.writeHash();
    writer.close();
  }

}
