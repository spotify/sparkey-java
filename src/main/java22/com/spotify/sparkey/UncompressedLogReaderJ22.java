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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
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
   * Note: valueLen is int internally (values > Integer.MAX_VALUE not supported in practice),
   * but getValueLength() returns long per API contract.
   */
  static final class ImmutableEntry implements SparkeyReader.Entry {
    private final int keyLen;
    private final byte[] key;
    private final int valueLen;
    private final long valuePosition;
    private final ReadOnlyMemMapJ22 data;

    ImmutableEntry(int keyLen, byte[] key,
                   int valueLen, long valuePosition, ReadOnlyMemMapJ22 data) {
      this.keyLen = keyLen;
      this.key = key;
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
      return valueLen;  // Widen int to long for API
    }

    @Override
    public byte[] getValue() throws IOException {
      return data.readBytes(valuePosition, valueLen);
    }

    @Override
    public String getValueAsString() throws IOException {
      return new String(getValue(), StandardCharsets.UTF_8);
    }

    @Override
    public InputStream getValueAsStream() {
      try {
        return new ByteArrayInputStream(getValue());
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
