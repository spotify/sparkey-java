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
package com.spotify.sparkey;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

public interface SparkeyReader extends Iterable<SparkeyReader.Entry> {
  /**
   * @param key the key to search for, interpreted as an UTF-8 string.
   * @return null if the key/value pair was not found, otherwise the value interpreted as an UTF-8 string.
   */
  String getAsString(String key) throws IOException;

  /**
   * @param key the key to search for
   * @return null if the key/value pair was not found, otherwise the raw byte array value
   */
  byte[] getAsByteArray(byte[] key) throws IOException;

  /**
   * This is mostly useful for retrieving large values that don't fit in a byte array.
   *
   * @param key the key to search for
   * @return null if the key/value pair was not found, otherwise the entry.
   *
   */
  Entry getAsEntry(byte[] key) throws IOException;

  void close() throws IOException;

  IndexHeader getIndexHeader();
  LogHeader getLogHeader();

  /**
   * Create a duplicate of the reader. Useful for using the reader from another thread.
   * @return a duplicate of the reader.
   */
  SparkeyReader duplicate();

  /**
   * Get an iterator over all the live entries.
   *
   * The iterator object is not thread safe,
   * and the entry objects are highly volatile
   * and will be invalidated by the next
   * iteration step. Don't leak this entry,
   * copy whatever data you want from it instead.
   *
   * @return an iterator
   */
  @Override
  Iterator<Entry> iterator();

  interface Entry {
    int getKeyLength();
    byte[] getKey();
    String getKeyAsString();

    long getValueLength();
    byte[] getValue() throws IOException;
    String getValueAsString() throws IOException;
    InputStream getValueAsStream();

    Type getType();
  }

  enum Type {
    PUT, DELETE
  }
}
