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

interface BlockRandomInput {

  void close();

  void seek(long pos) throws IOException;

  int readUnsignedByte() throws IOException;

  void readFully(byte[] buffer, int offset, int length) throws IOException;

  void skipBytes(long amount) throws IOException;

  BlockRandomInput duplicate();

  void closeDuplicate();

  long getLoadedBytes();

  /**
   * Compare bytes at current position with the provided byte array, advancing position by length bytes.
   *
   * This method always advances the current position by {@code length} bytes, regardless of whether
   * the comparison succeeds or fails. This matches the semantics of {@link #readFully(byte[], int, int)}.
   *
   * This is more efficient than calling {@code readFully()} followed by {@code Arrays.equals()}, as it:
   * - Avoids allocating a temporary buffer
   * - Avoids copying data from memory-mapped storage
   * - Uses vectorized comparison (SIMD) on supporting implementations
   *
   * @param length number of bytes to read and compare
   * @param key byte array to compare against (only first {@code length} bytes are compared)
   * @return true if the bytes at current position match the first {@code length} bytes of {@code key}
   */
  boolean readFullyCompare(int length, byte[] key) throws IOException;
}
