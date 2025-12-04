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
import java.lang.foreign.MemorySegment;

/**
 * Java 22+ uncompressed block random input.
 *
 * Wraps immutable ReadOnlyMemMapJ22 with stateful position tracking.
 */
class UncompressedBlockRandomInputJ22 implements BlockRandomInput {
  private final ReadOnlyMemMapJ22 data;
  private long position;

  UncompressedBlockRandomInputJ22(ReadOnlyMemMapJ22 data) {
    this.data = data;
    this.position = 0;
  }

  @Override
  public void close() {
    data.close();
  }

  @Override
  public void seek(long pos) throws IOException {
    this.position = pos;
  }

  @Override
  public int readUnsignedByte() throws IOException {
    int result = data.readUnsignedByte(position);
    position++;
    return result;
  }

  @Override
  public void readFully(byte[] buffer, int offset, int length) throws IOException {
    data.readFully(position, buffer, offset, length);
    position += length;
  }

  @Override
  public void skipBytes(long amount) throws IOException {
    position += amount;
  }

  @Override
  public UncompressedBlockRandomInputJ22 duplicate() {
    return new UncompressedBlockRandomInputJ22(data.duplicate());
  }

  @Override
  public void closeDuplicate() {
    data.closeDuplicate();
  }

  @Override
  public long getLoadedBytes() {
    // ReadOnlyMemMapJ22 doesn't track loaded bytes (MemorySegment is all-or-nothing)
    // Return 0 as conservative estimate
    return 0;
  }

  @Override
  public boolean readFullyCompare(int length, byte[] key) throws IOException {
    boolean result = data.readFullyCompare(position, length, key);
    position += length;
    return result;
  }

  /**
   * Get current position in the data stream.
   * Used for tracking value positions for lazy access.
   */
  long getPosition() {
    return position;
  }

  /**
   * Get a zero-copy slice of the underlying data.
   * Used for lazy value access - no allocation, no copying.
   */
  MemorySegment asSlice(long pos, long length) throws IOException {
    return data.asSlice(pos, length);
  }
}
