/*
 * Copyright (c) 2013 Spotify AB
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

/**
 * A superclass for Sparkey readers that delegate to another {@link SparkeyReader}.
 *
 * Subclasses must override the {@link AbstractDelegatingSparkeyReader#getDelegateReader()}
 * method.
 */
public abstract class AbstractDelegatingSparkeyReader implements SparkeyReader {

  protected abstract SparkeyReader getDelegateReader();

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
