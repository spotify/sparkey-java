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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

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
    checkNotNull(reader, "reader may not be null");

    this.readers.add(reader);
    this.threadLocalReader = new ThreadLocal<SparkeyReader>() {
      @Override
      protected SparkeyReader initialValue() {
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
    return getLocalReader().getAsString(key);
  }

  @Override
  public byte[] getAsByteArray(byte[] key) throws IOException {
    return getLocalReader().getAsByteArray(key);
  }

  @Override
  public Entry getAsEntry(byte[] key) throws IOException {
    return getLocalReader().getAsEntry(key);
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
    return getLocalReader().getIndexHeader();
  }

  @Override
  public LogHeader getLogHeader() {
    return getLocalReader().getLogHeader();
  }

  @Override
  public SparkeyReader duplicate() {
    checkState(threadLocalReader != null, "reader is closed");
    return this;
  }

  @Override
  public Iterator<Entry> iterator() {
    return getLocalReader().iterator();
  }

  protected SparkeyReader getLocalReader() {
    checkState(threadLocalReader != null, "reader is closed");
    return threadLocalReader.get();
  }

}
