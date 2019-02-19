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
package com.spotify.sparkey.extra;

import com.spotify.sparkey.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

/**
 * A sparkey reader that can switch between log files at runtime.
 *
 * This reader is thread-safe.
 */
public class ReloadableSparkeyReader extends AbstractDelegatingSparkeyReader {
  private static final Logger log = LoggerFactory.getLogger(ReloadableSparkeyReader.class);

  private final ExecutorService executorService;

  private volatile SparkeyReader reader;
  private volatile File currentLogFile;

  /**
   * Creates a new {@link ReloadableSparkeyReader} from a log file.
   *
   * @param logFile The log file to start with.
   * @param executorService An executor service that is used to run reload tasks on.
   * @return A future that resolves to the sparkey reader once it has loaded the log file.
   */
  public static CompletionStage<ReloadableSparkeyReader> fromLogFile(File logFile, ExecutorService executorService) {
    ReloadableSparkeyReader reader = new ReloadableSparkeyReader(executorService);
    return reader.load(logFile);
  }

  private ReloadableSparkeyReader(ExecutorService executorService) {
    if (executorService == null) {
      throw new IllegalArgumentException("executor service must not be null");
    }
    this.executorService = executorService;
  }

  /**
   * Load a new log file into this reader.
   * @param logFile the log file to load.
   * @return A future that resolves to the sparkey reader once it has loaded the new log file.
   */
  public CompletionStage<ReloadableSparkeyReader> load(final File logFile) {
    checkArgument(isValidLogFile(logFile));
    CompletableFuture<ReloadableSparkeyReader> result = new CompletableFuture<>();
    this.executorService.submit(() -> {
      switchReader(logFile);
      result.complete(this);
    });
    return result;
  }

  private void checkArgument(boolean b) {
    if (!b) {
      throw new IllegalArgumentException();
    }
  }

  @Override
  protected SparkeyReader getDelegateReader() {
    return this.reader;
  }

  private boolean isValidLogFile(File logFile) {
    return logFile != null && logFile.exists() && logFile.getName().endsWith(".spl");
  }

  private SparkeyReader createFromLogFile(File logFile) {
    Objects.requireNonNull(logFile);
    checkArgument(logFile.exists());
    checkArgument(logFile.getName().endsWith(".spl"));

    File indexFile = Sparkey.getIndexFile(logFile);
    if (!indexFile.exists()) {
      log.info("create sparkey index for log file {}", logFile.getAbsolutePath());
      try {
        SparkeyWriter w = Sparkey.append(indexFile);
        w.writeHash();
        w.close();
      } catch (IOException ex) {
        throw new ReloadableSparkeyReaderException("couldn't create index file", ex);
      }
    }

    try {
      return new ThreadLocalSparkeyReader(indexFile);
    } catch (IOException ex) {
      throw new ReloadableSparkeyReaderException("couldn't create sparkey reader", ex);
    }
  }

  private synchronized void switchReader(File logFile) {
    if (this.currentLogFile != null && this.currentLogFile.equals(logFile)) {
      log.debug("ignore reload (same log file)");
      return;
    }

    SparkeyReader newReader = createFromLogFile(logFile);
    SparkeyReader toClose = this.reader;

    this.currentLogFile = logFile;
    this.reader = newReader;

    long keys = reader.getLogHeader().getNumPuts() - reader.getLogHeader().getNumDeletes();
    log.info("loaded sparkey index {}, {} keys", logFile.getAbsolutePath(), keys);

    if (toClose != null) {
      toClose.close();
    }
  }

  public static class ReloadableSparkeyReaderException extends RuntimeException {
    public ReloadableSparkeyReaderException(String msg, Throwable t) {
      super(msg, t);
    }
  }
}
