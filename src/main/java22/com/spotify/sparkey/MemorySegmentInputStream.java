/*
 * Copyright (c) 2025 Spotify AB
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

import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Zero-copy InputStream backed by a MemorySegment.
 * Supports values larger than 2GB (limited only by MemorySegment size).
 */
final class MemorySegmentInputStream extends InputStream {
  private static final ValueLayout.OfByte JAVA_BYTE = ValueLayout.JAVA_BYTE;

  private final MemorySegment segment;
  private final long size;
  private long position;
  private long mark;

  MemorySegmentInputStream(MemorySegment segment) {
    this.segment = segment;
    this.size = segment.byteSize();
    this.position = 0;
    this.mark = 0;
  }

  @Override
  public int read() {
    if (position >= size) {
      return -1;
    }
    byte b = segment.get(JAVA_BYTE, position);
    position++;
    return ((int) b) & 0xFF;
  }

  @Override
  public int read(byte[] b, int off, int len) {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    }
    if (len == 0) {
      return 0;
    }
    if (position >= size) {
      return -1;
    }

    // Calculate how much we can actually read
    long remaining = size - position;
    // Cast is safe: result never exceeds len (which is already an int)
    int toRead = (int) Math.min(len, remaining);

    // Zero-copy read from MemorySegment to byte array
    MemorySegment.copy(segment, JAVA_BYTE, position, b, off, toRead);
    position += toRead;

    return toRead;
  }

  @Override
  public long skip(long n) {
    if (n <= 0) {
      return 0;
    }
    long remaining = size - position;
    long skipped = Math.min(n, remaining);
    position += skipped;
    return skipped;
  }

  @Override
  public int available() {
    long remaining = size - position;
    // Clamp to Integer.MAX_VALUE for API compatibility
    return (int) Math.min(remaining, Integer.MAX_VALUE);
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  @Override
  public synchronized void mark(int readlimit) {
    mark = position;
  }

  @Override
  public synchronized void reset() {
    position = mark;
  }

  @Override
  public void close() {
    // No-op: MemorySegment lifecycle is managed externally
  }
}
