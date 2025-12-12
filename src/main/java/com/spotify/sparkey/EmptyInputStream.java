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

/**
 * Immutable singleton empty InputStream for DELETE entries and zero-length values.
 * More efficient than wrapping an empty MemorySegment.
 *
 * Similar to InputStream.nullInputStream() from Java 11+, but works on Java 8+.
 */
final class EmptyInputStream extends InputStream {

  /**
   * Singleton instance - completely thread-safe since there's no mutable state.
   */
  static final InputStream INSTANCE = new EmptyInputStream();

  private EmptyInputStream() {
    // Private constructor - use INSTANCE
  }

  @Override
  public int read() {
    return -1;  // Always EOF
  }

  @Override
  public int read(byte[] b) {
    if (b == null) {
      throw new NullPointerException();
    }
    return -1;  // Always EOF
  }

  @Override
  public int read(byte[] b, int off, int len) {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    }
    return -1;  // Always EOF
  }

  @Override
  public long skip(long n) {
    return 0;  // Nothing to skip
  }

  @Override
  public int available() {
    return 0;  // No bytes available
  }

  @Override
  public void close() {
    // No-op
  }

  @Override
  public boolean markSupported() {
    return true;  // Mark/reset are trivial for empty stream
  }

  @Override
  public void mark(int readlimit) {
    // No-op - no state to save
  }

  @Override
  public void reset() {
    // No-op - nothing to reset to
  }
}
