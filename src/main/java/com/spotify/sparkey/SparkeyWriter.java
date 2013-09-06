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

public interface SparkeyWriter {
  /**
   * Append the key/value pair to the writer, as UTF-8.
   */
  void put(String key, String value) throws IOException;

  /**
   * Append the key/value pair to the writer.
   */
  void put(byte[] key, byte[] value) throws IOException;

  /**
   * Append the key/value pair to the writer.
   *
   * Only uses the first valueLen bytes from valueStream.
   */
  void put(byte[] key, InputStream valueStream, long valueLen) throws IOException;

  /**
   * Deletes the key from the writer, as UTF-8
   */
  void delete(String key) throws IOException;

  /**
   * Deletes the key from the writer.
   */
  void delete(byte[] key) throws IOException;

  /**
   * Flush all pending writes to file.
   */
  void flush() throws IOException;

  /**
   * Flush and close the writer.
   */
  void close() throws IOException;

  /**
   * Create or rewrite the index,
   * which is required for random lookups to be visible.
   */
  void writeHash() throws IOException;

  /**
   * Create or rewrite the index,
   * which is required for random lookups to be visible.
   *
   * @param hashType choice of hash type, can be 32 or 64 bits.
   */
  void writeHash(HashType hashType) throws IOException;
}
