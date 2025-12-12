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
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Raw log iterator for uncompressed Sparkey log files.
 * Returns ALL entries in the log (including DELETEs and superseded values).
 * Reads from immutable ReadOnlyMemMapJ22 but maintains mutable position state.
 *
 * This is a low-level iterator - use UncompressedSparkeyReaderJ22.iterator()
 * for hash-validated iteration that only returns current entries.
 */
final class UncompressedLogIteratorJ22 implements Iterator<SparkeyReader.Entry> {
  private final ReadOnlyMemMapJ22 logData;
  private final LogHeader logHeader;
  private final long endPosition;

  private long currentPosition;
  private LogEntry nextEntry;
  private boolean hasNextCached;

  UncompressedLogIteratorJ22(ReadOnlyMemMapJ22 logData, LogHeader logHeader) {
    this.logData = logData;
    this.logHeader = logHeader;
    this.currentPosition = logHeader.size();  // Start after header
    this.endPosition = logHeader.getDataEnd();
    this.hasNextCached = false;
  }

  @Override
  public boolean hasNext() {
    if (hasNextCached) {
      return true;
    }

    if (currentPosition >= endPosition) {
      return false;
    }

    try {
      // Read next entry at currentPosition
      long entryPosition = currentPosition;
      long p = currentPosition;

      // Read key length (VLQ)
      int keyLen = UncompressedUtilJ22.readVLQInt(logData, p);
      p += Util.unsignedVLQSize(keyLen);

      // Determine entry type and actual key length
      SparkeyReader.Type type;
      int actualKeyLen;
      long valueLen;

      if (keyLen == 0) {
        // DELETE entry: keyLen=0, next VLQ is actual key length
        type = SparkeyReader.Type.DELETE;
        actualKeyLen = UncompressedUtilJ22.readVLQInt(logData, p);
        p += Util.unsignedVLQSize(actualKeyLen);
        valueLen = 0;
      } else {
        // PUT entry: keyLen is encoded as actualKeyLen+1
        type = SparkeyReader.Type.PUT;
        actualKeyLen = keyLen - 1;

        // Read value length
        valueLen = UncompressedUtilJ22.readVLQLong(logData, p);
        p += Util.unsignedVLQSize(valueLen);
      }

      // Read key bytes
      byte[] key = new byte[actualKeyLen];
      logData.readFully(p, key, 0, actualKeyLen);
      p += actualKeyLen;

      // Create entry (p now points to value start)
      long valuePosition = p;

      // Advance currentPosition past this entry
      currentPosition = p + valueLen;

      // For uncompressed, entryIndex is always 0 (no block compression)
      nextEntry = new LogEntry(
        type, actualKeyLen, key, valueLen, valuePosition, entryPosition, 0, logData);
      hasNextCached = true;
      return true;

    } catch (IOException e) {
      throw new RuntimeException("Error reading log entry", e);
    }
  }

  @Override
  public SparkeyReader.Entry next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    hasNextCached = false;
    return nextEntry;
  }

  /**
   * Entry for iteration.
   * Stores all data needed to access key and value from immutable log data.
   * Also stores position and entryIndex for hash validation.
   * Note: valueLen is int internally (values > Integer.MAX_VALUE not supported in practice),
   * but getValueLength() returns long per API contract.
   */
  static final class LogEntry implements SparkeyReader.Entry {
    private final SparkeyReader.Type type;
    private final int keyLen;
    private final byte[] key;
    private final long valueLen;
    private final long valuePosition;
    private final long position;      // Position in log for validation
    private final int entryIndex;      // Entry index within block (always 0 for uncompressed)
    private final ReadOnlyMemMapJ22 logData;

    LogEntry(SparkeyReader.Type type, int keyLen, byte[] key,
             long valueLen, long valuePosition, long position, int entryIndex,
             ReadOnlyMemMapJ22 logData) {
      this.type = type;
      this.keyLen = keyLen;
      this.key = key;
      this.valueLen = valueLen;
      this.valuePosition = valuePosition;
      this.position = position;
      this.entryIndex = entryIndex;
      this.logData = logData;
    }

    // Package-private accessors for validation
    long getPosition() {
      return position;
    }

    int getEntryIndex() {
      return entryIndex;
    }

    byte[] getKeyBuf() {
      return key;
    }

    @Override
    public SparkeyReader.Type getType() {
      return type;
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
      return new String(key, 0, keyLen, StandardCharsets.UTF_8);
    }

    @Override
    public long getValueLength() {
      return valueLen;
    }

    @Override
    public byte[] getValue() throws IOException {
      if (type == SparkeyReader.Type.DELETE) {
        return new byte[0];
      }

      if (valueLen > Integer.MAX_VALUE) {
        throw new IllegalStateException("Value size is " + valueLen +
            " bytes, exceeds byte[] limit. Use getValueAsStream() instead.");
      }

      byte[] value = new byte[(int) valueLen];
      logData.readFully(valuePosition, value, 0, (int) valueLen);
      return value;
    }

    @Override
    public String getValueAsString() throws IOException {
      return new String(getValue(), StandardCharsets.UTF_8);
    }

    @Override
    public InputStream getValueAsStream() {
      // Zero-copy stream backed by MemorySegment - no allocation, supports values > 2GB
      if (type == SparkeyReader.Type.DELETE || valueLen == 0) {
        // Use singleton for empty streams - DELETE entries only appear during iteration
        return EmptyInputStream.INSTANCE;
      }
      try {
        MemorySegment valueSegment = logData.asSlice(valuePosition, valueLen);
        return new MemorySegmentInputStream(valueSegment);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
