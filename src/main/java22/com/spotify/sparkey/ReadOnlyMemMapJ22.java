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
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

final class ReadOnlyMemMapJ22 implements RandomAccessDataStateless {
  // ValueLayouts for little-endian access
  private static final ValueLayout.OfByte JAVA_BYTE = ValueLayout.JAVA_BYTE;
  private static final ValueLayout.OfInt JAVA_INT_LE = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
  private static final ValueLayout.OfLong JAVA_LONG_LE = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

  private final String filename;
  private final long size;

  private final Arena arena;
  private final MemorySegment segment;

  ReadOnlyMemMapJ22(File file) throws IOException {
    this.filename = file.toString();
    this.size = file.length();
    if (size <= 0) {
      throw new IllegalArgumentException("Non-positive size: " + size);
    }

    // Map the file - RandomAccessFile closed immediately after mapping via try-with-resources
    Sparkey.incrOpenFiles();
    Arena arena = null;
    try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
      // Map the entire file as a single segment - no 2GB limit with MemorySegment!
      arena = Arena.ofShared();
      try (FileChannel channel = raf.getChannel()) {
        this.segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);
      }
      // RandomAccessFile closed automatically here - we only needed it for mapping
      // The MemorySegment is now backed by OS page mappings, independent of the file descriptor
      this.arena = arena;  // Only assign to final field after successful initialization
      Sparkey.incrOpenMaps();
    } catch (Throwable e) {
      if (arena != null) {
        arena.close();  // Close arena to prevent resource leak
      }
      Sparkey.decrOpenFiles();
      throw e;
    }
  }

  public void close() {
    synchronized (this) {
      // Fast-exit if already closed (prevents double-close)
      if (!arena.scope().isAlive()) {
        return;
      }

      // Decrement counters - file was closed immediately after mapping
      Sparkey.decrOpenFiles();
      Sparkey.decrOpenMaps();

      // Arena-based cleanup: deterministic and safe!
      // No need to null out segment - Arena.close() makes it inaccessible
      arena.close();
    }
  }

  void closeDuplicate() {
    // No-op: duplicates don't own resources, so closing them does nothing
    // Only the original opener should call close()
  }

  @Override
  public int readUnsignedByte(long position) throws IOException {
    try {
      byte b = segment.get(JAVA_BYTE, position);
      return ((int) b) & 0xFF;
    } catch (IllegalStateException e) {
      throw closedException();
    }
  }

  @Override
  public int readLittleEndianInt(long position) throws IOException {
    try {
      return segment.get(JAVA_INT_LE, position);
    } catch (IllegalStateException e) {
      throw closedException();
    }
  }

  @Override
  public long readLittleEndianLong(long position) throws IOException {
    try {
      return segment.get(JAVA_LONG_LE, position);
    } catch (IllegalStateException e) {
      throw closedException();
    }
  }

  void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    try {
      MemorySegment.copy(segment, JAVA_BYTE, position, buffer, offset, length);
    } catch (IllegalStateException e) {
      throw closedException();
    }
  }

  /**
   * Threshold for switching between allocate-then-copy vs toArray().
   * For small values: allocate-then-copy is faster (avoids slice allocation)
   * For large values: toArray() may have optimized bulk copy paths
   */
  private static final int TOARRAY_THRESHOLD = 256;

  /**
   * Read bytes from the memory segment as a new byte array.
   * For small values (≤256 bytes): uses allocate-then-copy to avoid slice allocation
   * For large values (>256 bytes): uses MemorySegment.toArray() for potential bulk optimizations
   */
  byte[] readBytes(long position, int length) throws IOException {
    try {
      if (length <= TOARRAY_THRESHOLD) {
        // Small values: allocate-then-copy (avoids slice allocation overhead)
        byte[] result = new byte[length];
        MemorySegment.copy(segment, JAVA_BYTE, position, result, 0, length);
        return result;
      } else {
        // Large values: use toArray (potential bulk copy optimizations)
        return segment.asSlice(position, length).toArray(JAVA_BYTE);
      }
    } catch (IllegalStateException e) {
      throw closedException();
    }
  }

  /**
   * Compare bytes in the memory segment with a byte array without copying.
   * Uses byte-by-byte comparison.
   */
  boolean equalsBytes(long position, int length, byte[] key) throws IOException {
    try {
      for (int i = 0; i < length; i++) {
        if (segment.get(JAVA_BYTE, position + i) != key[i]) {
          return false;
        }
      }
      return true;
    } catch (IllegalStateException e) {
      throw closedException();
    }
  }

  /**
   * Compare bytes in the memory segment with a byte array using vectorized mismatch.
   * Based on microbenchmark results showing mismatch() is 1.3-8x faster at ALL key sizes.
   *
   * @param keySegment Pre-sliced MemorySegment of the key array
   */
  boolean equalsBytes(long position, int length, byte[] key, MemorySegment keySegment) throws IOException {
    try {
      // Vectorized comparison using mismatch() - faster at all sizes (1.3x at 1 byte, 8x at 1KB)
      long mismatch = segment.asSlice(position, length).mismatch(keySegment);
      return mismatch == -1;
    } catch (IllegalStateException e) {
      throw closedException();
    }
  }

  /**
   * Compare bytes at position with provided byte array, matching readFullyCompare semantics.
   */
  boolean readFullyCompare(long position, int length, byte[] key) throws IOException {
    return equalsBytes(position, length, key);
  }

  /**
   * Get a zero-copy slice of the memory segment.
   * Used for lazy value access - no allocation, no copying.
   *
   * @param position Starting position in the file
   * @param length Number of bytes in the slice
   * @return MemorySegment view of the data
   */
  MemorySegment asSlice(long position, long length) throws IOException {
    try {
      return segment.asSlice(position, length);
    } catch (IllegalStateException e) {
      throw closedException();
    }
  }

  private SparkeyReaderClosedException closedException() {
    return new SparkeyReaderClosedException("Reader has been closed");
  }

  ReadOnlyMemMapJ22 duplicate() {
    // ReadOnlyMemMapJ22 is immutable, so just return self - no actual duplication needed
    return this;
  }

  @Override
  public String toString() {
    return "ReadOnlyMemMapJ22{" +
            "filename=" + filename +
            ", size=" + size +
            '}';
  }

}
