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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A thread-safe Sparkey Reader.
 */
public class ThreadLocalSparkeyReader extends AbstractDelegatingSparkeyReader {
  private final SparkeyReader reader;
  private final Set<SparkeyReader> readers = ConcurrentHashMap.newKeySet();
  private volatile ThreadLocal<SparkeyReader> threadLocalReader;

  public ThreadLocalSparkeyReader(File indexFile) throws IOException {
    this(Sparkey.openSingleThreadedReader(indexFile));
  }

  private ThreadLocalSparkeyReader(final SparkeyReader reader) {
    this.reader = reader;
    this.threadLocalReader = PersistentThreadLocal.withInitial(() -> {
      SparkeyReader r = reader.duplicate();
      readers.add(r);
      return r;
    });
  }

  @Override
  public void close() {
    this.threadLocalReader = null;
    synchronized (readers) {
      reader.close();
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
    final ThreadLocal<SparkeyReader> threadLocal = this.threadLocalReader;
    if (threadLocal == null) {
      throw new IllegalStateException("reader is closed");
    }
    return threadLocal.get();
  }

}
