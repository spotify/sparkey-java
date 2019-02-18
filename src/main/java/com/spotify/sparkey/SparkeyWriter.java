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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

public interface SparkeyWriter extends Closeable {
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
  @Override
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
   * @deprecated Use writer.setHashType(hashType); writer.writeHash(); instead
   */
  @Deprecated
  void writeHash(HashType hashType) throws IOException;

  /**
   * Set whether or not flushes and hash writes should be synced to disk.
   *
   * @param fsync whether or not flushes and hash writes should be synced to disk
   */
  void setFsync(boolean fsync);

  /**
   * Set the hash type for all subsequent writeHash operations.
   * @param hashType choice of hash type, can be 32 or 64 bits.
   *                 if null, will use the default.
   */
  void setHashType(HashType hashType);

  /**
   * Set the sparsity for all subsequent writeHash operations.
   * A sparsity of 1.0 would mean that every slot in the hash table is occupied.
   * The actual minimum sparsity level is 1.3, values lower than this are ignored.
   * @param sparsity
   */
  void setHashSparsity(double sparsity);

  /**
   * Set the hash seed to use. Default: a random seed
   * If set to 0, a random seed will be used.
   * @param hashSeed
   */
  void setHashSeed(int hashSeed);

  /**
   * Set the maximum amount of memory to use for index construction.
   * Default: Runtime.freeMemory() / 2
   * @param maxMemory
   */
  void setMaxMemory(long maxMemory);

  /**
   * Set which construction method to use to create the hash index.
   * Default: AUTO
   * @param method
   */
  void setConstructionMethod(ConstructionMethod method);

  enum ConstructionMethod {
    /**
     * Chooses construction method dynamically based on size of data and available memory.
     */
    AUTO,

    /**
     * Write hash index in memory
     */
    IN_MEMORY,

    /**
     * Sort hash entries before writing to the hash index.
     */
    SORTING
  }
}
