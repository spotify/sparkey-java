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

/**
 * Fully immutable index hash reader for UNCOMPRESSED files.
 * Optimized version that skips all entry block handling.
 *
 * For uncompressed files:
 * - maxEntriesPerBlock = 1
 * - entryBlockBits = 0
 * - entryIndex = always 0
 * - logPosition = address (no shift needed)
 */
final class UncompressedIndexHashJ22 {
  private final ReadOnlyMemMapJ22 indexData;
  private final UncompressedLogReaderJ22 logReader;
  private final IndexHeader header;
  private final LogHeader logHeader;

  // Cached values from header
  private final long numSlots;
  private final int slotSize;
  private final long headerSize;
  private final HashType hashType;
  private final int hashSeed;
  private final AddressSize addressSize;
  private final long maxDisplacement;

  // Cached size constants for hot loop optimization
  private final int hashSize;
  private final int addressSizeBytes;

  UncompressedIndexHashJ22(ReadOnlyMemMapJ22 indexData,
                                    UncompressedLogReaderJ22 logReader,
                                    IndexHeader header, LogHeader logHeader) {
    this.indexData = indexData;
    this.logReader = logReader;
    this.header = header;
    this.logHeader = logHeader;

    // Cache frequently accessed header values
    this.numSlots = header.getHashCapacity();
    this.slotSize = header.getSlotSize();
    this.headerSize = header.size();
    this.hashType = header.getHashType();
    this.hashSeed = header.getHashSeed();
    this.addressSize = header.getAddressData();
    this.maxDisplacement = header.getMaxDisplacement();

    // Cache size constants to avoid method calls in hot loop
    this.hashSize = hashType.size();
    this.addressSizeBytes = addressSize.size();
  }

  /**
   * Lookup value as byte array, bypassing Entry allocation.
   * Optimized hot path for getAsByteArray() and getAsString().
   *
   * Fully inlined to avoid reading VLQs twice (hash table walk + log entry parsing in one pass).
   */
  byte[] getValueBytes(int keyLen, byte[] key) throws IOException {
    // Hash the key
    long hash = hashType.hash(keyLen, key, hashSeed);
    long wantedSlot = Long.remainderUnsigned(hash, numSlots);

    // Create MemorySegment for vectorized comparison (1.3-8x faster at all sizes)
    java.lang.foreign.MemorySegment keySegment =
        java.lang.foreign.MemorySegment.ofArray(key).asSlice(0, keyLen);

    // Start at hash bucket
    long slot = wantedSlot;
    long pos = headerSize + slot * slotSize;
    long displacement = 0;

    // Walk the hash table using linear probing
    while (true) {
      // Read hash from slot (fixed size)
      long hash2 = hashType.readHash(indexData, pos);

      // Read address from slot (fixed size)
      long logPosition = addressSize.readAddress(indexData, pos + hashSize);

      if (logPosition == 0) {
        // Empty slot - key not found
        return null;
      }

      // For uncompressed: address IS the log position (no entry index encoding)

      if (hash == hash2) {
        // Hash matches - check key in log and read value if match (inline to read VLQs only once!)
        long p = logPosition;

        // Read keyLen VLQ
        int storedKeyLen = UncompressedUtilJ22.readVLQInt(logReader.data, p);
        p += Util.unsignedVLQSize(storedKeyLen);

        if (storedKeyLen == 0) {
          // DELETE entry - continue probing (hash collision with deleted key)
        } else {
          // Decode actual key length (stored as keyLen + 1)
          int actualKeyLen = storedKeyLen - 1;

          if (actualKeyLen == keyLen) {
            // Key length matches - read valueLen VLQ
            long valueLen = UncompressedUtilJ22.readVLQLong(logReader.data, p);
            p += Util.unsignedVLQSize(valueLen);

            // Compare key bytes using vectorized mismatch
            if (logReader.data.equalsBytes(p, keyLen, key, keySegment)) {
              // Found it! Read value directly (VLQs already parsed - no re-read!)
              p += keyLen; // Skip past key
              if (valueLen > Integer.MAX_VALUE) {
                throw new IllegalStateException("Value size is " + valueLen +
                    " bytes, exceeds byte[] limit. Use getAsEntry() and getValueAsStream() instead.");
              }
              return logReader.data.readBytes(p, (int) valueLen);
            }
          }
        }
      }

      // Not found at this slot - continue linear probing
      displacement++;
      if (displacement > maxDisplacement) {
        return null;
      }

      // Move to next slot
      slot++;
      pos += slotSize;
      if (slot >= numSlots) {
        slot = 0;
        pos = headerSize;
      }
    }
  }

  /**
   * Lookup entry by key.
   * Optimized for uncompressed - no entry block handling needed.
   * Only used by getAsEntry() - other methods use bypass methods to avoid allocation.
   *
   * Fully inlined to avoid reading VLQs twice (hash table walk + log entry parsing in one pass).
   */
  SparkeyReader.Entry get(int keyLen, byte[] key) throws IOException {
    // Hash the key
    long hash = hashType.hash(keyLen, key, hashSeed);
    long wantedSlot = Long.remainderUnsigned(hash, numSlots);

    // Create MemorySegment for vectorized comparison (1.3-8x faster at all sizes)
    java.lang.foreign.MemorySegment keySegment =
        java.lang.foreign.MemorySegment.ofArray(key).asSlice(0, keyLen);

    // Start at hash bucket
    long slot = wantedSlot;
    long pos = headerSize + slot * slotSize;
    long displacement = 0;

    // Walk the hash table using linear probing
    while (true) {
      // Read hash from slot (fixed size)
      long hash2 = hashType.readHash(indexData, pos);

      // Read address from slot (fixed size)
      long logPosition = addressSize.readAddress(indexData, pos + hashSize);

      if (logPosition == 0) {
        // Empty slot - key not found
        return null;
      }

      // For uncompressed: address IS the log position (no entry index encoding)

      if (hash == hash2) {
        // Hash matches - check key in log and create entry if match (inline to read VLQs only once!)
        long p = logPosition;

        // Read keyLen VLQ
        int storedKeyLen = UncompressedUtilJ22.readVLQInt(logReader.data, p);
        p += Util.unsignedVLQSize(storedKeyLen);

        if (storedKeyLen == 0) {
          // DELETE entry - continue probing (hash collision with deleted key)
        } else {
          // Decode actual key length (stored as keyLen + 1)
          int actualKeyLen = storedKeyLen - 1;

          if (actualKeyLen == keyLen) {
            // Key length matches - read valueLen VLQ
            long valueLen = UncompressedUtilJ22.readVLQLong(logReader.data, p);
            p += Util.unsignedVLQSize(valueLen);

            // Compare key bytes using vectorized mismatch
            if (logReader.data.equalsBytes(p, keyLen, key, keySegment)) {
              // Found it! Create entry with lazy value loading (VLQs already parsed - no re-read!)
              p += keyLen; // Skip past key to value position
              return new UncompressedLogReaderJ22.ImmutableEntry(keyLen, key, valueLen, p, logReader.data);
            }
          }
        }
      }

      // Not found at this slot - continue linear probing
      displacement++;
      if (displacement > maxDisplacement) {
        return null;
      }

      // Move to next slot
      slot++;
      pos += slotSize;
      if (slot >= numSlots) {
        slot = 0;
        pos = headerSize;
      }
    }
  }

  /**
   * Check if the index points to a specific log position for the given key.
   * Used for hash-validated iteration to filter out superseded entries.
   *
   * @param keyLen Key length
   * @param key Key bytes
   * @param position Log position to check
   * @param entryIndex Entry index within block (always 0 for uncompressed)
   * @return true if index points to this exact position, false otherwise
   */
  boolean isAt(int keyLen, byte[] key, long position, int entryIndex) throws IOException {
    // Hash the key
    long hash = hashType.hash(keyLen, key, hashSeed);
    long wantedSlot = Long.remainderUnsigned(hash, numSlots);

    // Start at hash bucket
    long slot = wantedSlot;
    long pos = headerSize + slot * slotSize;

    long displacement = 0;

    // Walk the hash table using linear probing
    while (true) {
      // Read hash from slot (fixed size) - position-based read
      long hash2 = hashType.readHash(indexData, pos);

      // Read address from slot (fixed size) - position-based read
      long logPosition = addressSize.readAddress(indexData, pos + hashSize);

      if (logPosition == 0) {
        // Empty slot - key not in index
        return false;
      }

      // For uncompressed: address IS the log position (no entry index encoding)

      if (hash == hash2 && logPosition == position) {
        // Hash matches and position matches - this entry is current!
        return true;
      }

      // Check max displacement optimization
      displacement++;
      if (displacement > maxDisplacement) {
        return false;
      }

      // Move to next slot (linear probing)
      slot++;
      pos += slotSize;
      if (slot >= numSlots) {
        slot = 0;
        pos = headerSize;
      }
    }
  }

  IndexHeader getIndexHeader() {
    return header;
  }

  LogHeader getLogHeader() {
    return logHeader;
  }
}
