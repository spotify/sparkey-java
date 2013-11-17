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

import com.spotify.sparkey.IndexHeader;
import com.spotify.sparkey.LogHeader;
import com.spotify.sparkey.SparkeyReader;

import java.io.IOException;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * A superclass for Sparkey readers that delgate to another {@link SparkeyReader}.
 *
 * Subclasses must either pass a non-null {@link SparkeyReader} delegate to the
 * {@link DelegatingSparkeyReader#DelegatingSparkeyReader(com.spotify.sparkey.SparkeyReader)}
 * constructor, or override the {@link DelegatingSparkeyReader#getDelegateReader()}
 * method.
 */
public class DelegatingSparkeyReader implements SparkeyReader {
  private final SparkeyReader delegate;

  public DelegatingSparkeyReader() {
    this.delegate = null;
  }

  public DelegatingSparkeyReader(final SparkeyReader delegate) {
    checkArgument(delegate != null, "delegate must not be null");
    this.delegate = delegate;
  }

  protected SparkeyReader getDelegateReader() {
    checkState(delegate != null, "delegate must not be null");
    return this.delegate;
  }

  @Override
  public String getAsString(String key) throws IOException {
    return getDelegateReader().getAsString(key);
  }

  @Override
  public byte[] getAsByteArray(byte[] key) throws IOException {
    return getDelegateReader().getAsByteArray(key);
  }

  @Override
  public Entry getAsEntry(byte[] key) throws IOException {
    return getDelegateReader().getAsEntry(key);
  }

  @Override
  public void close() throws IOException {
    getDelegateReader().close();
  }

  @Override
  public IndexHeader getIndexHeader() {
    return getDelegateReader().getIndexHeader();
  }

  @Override
  public LogHeader getLogHeader() {
    return getDelegateReader().getLogHeader();
  }

  @Override
  public SparkeyReader duplicate() {
    return getDelegateReader().duplicate();
  }

  @Override
  public Iterator<Entry> iterator() {
    return getDelegateReader().iterator();
  }

}
