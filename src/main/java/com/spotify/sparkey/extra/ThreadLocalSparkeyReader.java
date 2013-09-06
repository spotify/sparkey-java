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
  private final ThreadLocal<SparkeyReader> threadLocalReader;

  public ThreadLocalSparkeyReader(File indexFile) throws IOException {
    this(Sparkey.open(indexFile));
  }

  public ThreadLocalSparkeyReader(final SparkeyReader reader) {
    this.readers.add(reader);
    this.threadLocalReader = new ThreadLocal<SparkeyReader>() {
      @Override
      protected SparkeyReader initialValue() {
        SparkeyReader r = reader.duplicate();
        readers.add(r);
        return r;
      }
    };
  }

  @Override
  public String getAsString(String key) throws IOException {
    return threadLocalReader.get().getAsString(key);
  }

  @Override
  public byte[] getAsByteArray(byte[] key) throws IOException {
    return threadLocalReader.get().getAsByteArray(key);
  }

  @Override
  public Entry getAsEntry(byte[] key) throws IOException {
    return threadLocalReader.get().getAsEntry(key);
  }

  @Override
  public void close() throws IOException {
    synchronized (readers) {
      for (SparkeyReader reader : readers) {
        reader.close();
      }
      readers.clear();
    }
    threadLocalReader.remove();
  }

  @Override
  public IndexHeader getIndexHeader() {
    return threadLocalReader.get().getIndexHeader();
  }

  @Override
  public LogHeader getLogHeader() {
    return threadLocalReader.get().getLogHeader();
  }

  @Override
  public SparkeyReader duplicate() {
    return this;
  }

  @Override
  public Iterator<Entry> iterator() {
    return threadLocalReader.get().iterator();
  }
}
