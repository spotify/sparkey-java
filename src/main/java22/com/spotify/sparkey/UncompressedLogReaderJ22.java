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

import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

/**
 * Fully immutable log reader for UNCOMPRESSED files.
 * Optimized version that skips entry block handling.
 *
 * For uncompressed files:
 * - No entry blocks - each position points directly to an entry
 * - No need to skip entries within blocks
 */
final class UncompressedLogReaderJ22 {
  final ReadOnlyMemMapJ22 data;  // Package-private for inlined access from UncompressedIndexHashJ22
  private final LogHeader logHeader;

  UncompressedLogReaderJ22(ReadOnlyMemMapJ22 data, LogHeader logHeader) {
    this.data = data;
    this.logHeader = logHeader;
  }

  /**
   * Simple immutable entry that reads value data on demand.
   * Supports values larger than 2GB via getValueAsStream().
   * getValue() throws IllegalStateException for values > Integer.MAX_VALUE.
   */
  static final class ImmutableEntry implements SparkeyReader.Entry {
    private final int keyLen;
    private final byte[] key;
    private final long valueLen;
    private final long valuePosition;
    private final ReadOnlyMemMapJ22 data;

    ImmutableEntry(int keyLen, byte[] key,
                   long valueLen, long valuePosition, ReadOnlyMemMapJ22 data) {
      this.keyLen = keyLen;
      // Defensive copy: ensure immutability even if caller reuses the key array
      this.key = java.util.Arrays.copyOf(key, keyLen);
      this.valueLen = valueLen;
      this.valuePosition = valuePosition;
      this.data = data;
    }

    @Override
    public int getKeyLength() {
      return keyLen;
    }

    @Override
    public byte[] getKey() {
      return key;
    }

    @Override
    public String getKeyAsString() {
      return new String(key, StandardCharsets.UTF_8);
    }

    @Override
    public long getValueLength() {
      return valueLen;
    }

    @Override
    public byte[] getValue() throws IOException {
      if (valueLen > Integer.MAX_VALUE) {
        throw new IllegalStateException("Value size is " + valueLen +
            " bytes, exceeds byte[] limit. Use getValueAsStream() instead.");
      }
      return data.readBytes(valuePosition, (int) valueLen);
    }

    @Override
    public String getValueAsString() throws IOException {
      return new String(getValue(), StandardCharsets.UTF_8);
    }

    @Override
    public InputStream getValueAsStream() {
      // Zero-copy stream backed by MemorySegment - no allocation, supports values > 2GB
      if (valueLen == 0) {
        // Use singleton for empty streams (edge case, but possible)
        return EmptyInputStream.INSTANCE;
      }
      try {
        MemorySegment valueSegment = data.asSlice(valuePosition, valueLen);
        return new MemorySegmentInputStream(valueSegment);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public SparkeyReader.Type getType() {
      return SparkeyReader.Type.PUT;
    }
  }
}
