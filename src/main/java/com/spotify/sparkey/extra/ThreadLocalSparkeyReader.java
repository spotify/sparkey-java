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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A thread-safe Sparkey Reader.
 */
public class ThreadLocalSparkeyReader extends AbstractDelegatingSparkeyReader {
  private final Collection<SparkeyReader> readers = new ArrayList<>();
  private volatile ThreadLocal<SparkeyReader> threadLocalReader;
  private final AtomicBoolean firstRead = new AtomicBoolean(false);

  public ThreadLocalSparkeyReader(File indexFile) throws IOException {
    this(Sparkey.openSingleThreadedReader(indexFile), true);
  }

  public ThreadLocalSparkeyReader(final SparkeyReader reader) {
    this(reader, false);
  }

  private ThreadLocalSparkeyReader(final SparkeyReader reader, final boolean owner) {
    Objects.requireNonNull(reader, "reader may not be null");
    this.readers.add(reader);
    this.threadLocalReader = ThreadLocal.withInitial(() -> {
      if (owner && firstRead.compareAndSet(false, true)) {
        return reader; // No need to duplicate the reader for the first usage if we are the owner of the reader
      }

      SparkeyReader r = reader.duplicate();
      synchronized (readers) {
        readers.add(r);
      }
      return r;
    });
  }

  @Override
  public void close() {
    this.threadLocalReader = null;
    synchronized (readers) {
      for (SparkeyReader reader : readers) {
        reader.close();
      }
      readers.clear();
    }
  }

  @Override
  public SparkeyReader duplicate() {
    if (threadLocalReader == null) {
      throw new IllegalStateException("reader is closed");
    }
    return this;
  }

  @Override
  protected SparkeyReader getDelegateReader() {
    if (threadLocalReader == null) {
      throw new IllegalStateException("reader is closed");
    }
    return threadLocalReader.get();
  }

}
