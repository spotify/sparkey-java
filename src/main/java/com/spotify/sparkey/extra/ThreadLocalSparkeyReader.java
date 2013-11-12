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

import com.google.common.collect.Lists;
import com.spotify.sparkey.*;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

/**
 * A thread-safe Sparkey Reader.
 */
public class ThreadLocalSparkeyReader implements SparkeyReader {

  private final Collection<SparkeyReader> readers = Lists.newArrayList();
  private volatile ThreadLocal<SparkeyReader> threadLocalReader;

  public ThreadLocalSparkeyReader(File indexFile) throws IOException {
    this(Sparkey.open(indexFile));
  }

  public ThreadLocalSparkeyReader(final SparkeyReader reader) {
    if (reader == null) {
      throw new IllegalArgumentException("reader may not be null");
    }
    this.readers.add(reader);
    this.threadLocalReader = new ThreadLocal<SparkeyReader>() {
      @Override
      protected SparkeyReader initialValue() {
        getReader();
        SparkeyReader r = reader.duplicate();
        synchronized (readers) {
          readers.add(r);
        }
        return r;
      }
    };
  }

  @Override
  public String getAsString(String key) throws IOException {
    return getReader().get().getAsString(key);
  }

  private ThreadLocal<SparkeyReader> getReader() {
    ThreadLocal<SparkeyReader> reader = threadLocalReader;
    if (reader == null) {
      throw new IllegalStateException("Reader is closed");
    }
    return reader;
  }

  @Override
  public byte[] getAsByteArray(byte[] key) throws IOException {
    return getReader().get().getAsByteArray(key);
  }

  @Override
  public Entry getAsEntry(byte[] key) throws IOException {
    return getReader().get().getAsEntry(key);
  }

  @Override
  public void close() throws IOException {
    this.threadLocalReader = null;
    synchronized (readers) {
      for (SparkeyReader reader : readers) {
        reader.close();
      }
      readers.clear();
    }
  }

  @Override
  public IndexHeader getIndexHeader() {
    return getReader().get().getIndexHeader();
  }

  @Override
  public LogHeader getLogHeader() {
    return getReader().get().getLogHeader();
  }

  @Override
  public SparkeyReader duplicate() {
    // Just to make sure we're not closed
    getReader();

    return this;
  }

  @Override
  public Iterator<Entry> iterator() {
    return getReader().get().iterator();
  }
}
