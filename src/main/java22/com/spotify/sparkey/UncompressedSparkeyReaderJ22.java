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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/**
 * Sparkey reader optimized for uncompressed files - EXPERIMENTAL.
 *
 * This reader is fully immutable with ZERO mutable state and requires NO synchronization.
 * It can be safely shared across unlimited threads with zero overhead.
 *
 * Only supports UNCOMPRESSED files.
 * Compressed files will throw UnsupportedOperationException.
 *
 * Performance characteristics:
 * - No ThreadLocal overhead
 * - No pooling overhead
 * - No synchronization overhead
 * - Zero allocations on happy path
 * - Lock-free, wait-free lookups
 */
public final class UncompressedSparkeyReaderJ22 implements SparkeyReader {
  private final UncompressedIndexHashJ22 indexHash;
  private final File indexFile;
  private final File logFile;
  private final ReadOnlyMemMapJ22 indexData;
  private final ReadOnlyMemMapJ22 logData;
  private final LogHeader logHeader;

  private UncompressedSparkeyReaderJ22(File indexFile, File logFile,
                                    UncompressedIndexHashJ22 indexHash,
                                    ReadOnlyMemMapJ22 indexData,
                                    ReadOnlyMemMapJ22 logData, LogHeader logHeader) {
    this.indexFile = indexFile;
    this.logFile = logFile;
    this.indexHash = indexHash;
    this.indexData = indexData;
    this.logData = logData;
    this.logHeader = logHeader;
  }

  /**
   * Open an uncompressed Sparkey reader.
   * Only supports uncompressed files.
   *
   * @throws UnsupportedOperationException if file is compressed
   */
  public static UncompressedSparkeyReaderJ22 open(File file) throws IOException {
    File indexFile = Sparkey.getIndexFile(file);
    File logFile = Sparkey.getLogFile(file);

    // Check compression type
    LogHeader logHeader = Sparkey.getLogHeader(logFile);
    return open(indexFile, logFile, logHeader);
  }

  /**
   * Open an uncompressed Sparkey reader with pre-read LogHeader.
   * Only supports uncompressed files.
   *
   * This overload is useful when the LogHeader has already been read
   * (e.g., to check compression type), avoiding a redundant file read.
   *
   * @param indexFile the index file (.spi)
   * @param logFile the log file (.spl)
   * @param logHeader the already-read log header
   * @throws UnsupportedOperationException if file is compressed
   */
  static UncompressedSparkeyReaderJ22 open(File indexFile, File logFile, LogHeader logHeader)
      throws IOException {
    if (logHeader.getCompressionType() != CompressionType.NONE) {
      throw new UnsupportedOperationException(
        "UncompressedSparkeyReaderJ22 only supports uncompressed files. " +
        "File " + logFile + " uses " + logHeader.getCompressionType());
    }

    // Open memory-mapped files (these are immutable)
    ReadOnlyMemMapJ22 indexData = null;
    ReadOnlyMemMapJ22 logData = null;
    try {
      indexData = new ReadOnlyMemMapJ22(indexFile);
      logData = new ReadOnlyMemMapJ22(logFile);

      // Read headers
      IndexHeader indexHeader = IndexHeader.read(indexFile);

      // Validate that index and log files match
      if (logHeader.getFileIdentifier() != indexHeader.getFileIdentifier()) {
        throw new IllegalArgumentException("Log file did not match index file");
      }

      // Validate that index doesn't reference more data than exists in log
      if (indexHeader.getDataEnd() > logHeader.getDataEnd()) {
        throw new IOException("Corrupt index file '" + indexFile.toString() +
          "': referencing more data than exists in the log file");
      }

      // Create immutable components (specialized for uncompressed)
      UncompressedLogReaderJ22 logReader =
        new UncompressedLogReaderJ22(logData, logHeader);
      UncompressedIndexHashJ22 indexHash =
        new UncompressedIndexHashJ22(indexData, logReader, indexHeader, logHeader);

      // Validate index file size
      long slotSize = indexHeader.getSlotSize();
      long hashCapacity = indexHeader.getHashCapacity();
      long expectedFileSize = IndexHeader.HEADER_SIZE + slotSize * hashCapacity;
      if (expectedFileSize != indexFile.length()) {
        throw new RuntimeException("Corrupt index file - incorrect size. Expected " +
          expectedFileSize + " but was " + indexFile.length());
      }

      return new UncompressedSparkeyReaderJ22(indexFile, logFile, indexHash, indexData, logData, logHeader);
    } catch (Exception e) {
      // Clean up any opened resources on error
      if (indexData != null) {
        try {
          indexData.close();
        } catch (Exception closeEx) {
          // Suppress close exception, throw original
        }
      }
      if (logData != null) {
        try {
          logData.close();
        } catch (Exception closeEx) {
          // Suppress close exception, throw original
        }
      }
      throw e;
    }
  }

  @Override
  public String getAsString(String key) throws IOException {
    byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
    // Use bypass method to avoid Entry allocation
    byte[] valueBytes = indexHash.getValueBytes(keyBytes.length, keyBytes);
    if (valueBytes == null) {
      return null;
    }
    return new String(valueBytes, StandardCharsets.UTF_8);
  }

  @Override
  public byte[] getAsByteArray(byte[] key) throws IOException {
    // Use bypass method to avoid Entry allocation
    return indexHash.getValueBytes(key.length, key);
  }

  @Override
  public Entry getAsEntry(byte[] key) throws IOException {
    // Only this method needs Entry allocation
    return indexHash.get(key.length, key);
  }

  @Override
  public IndexHeader getIndexHeader() {
    return indexHash.getIndexHeader();
  }

  @Override
  public LogHeader getLogHeader() {
    return indexHash.getLogHeader();
  }

  @Override
  public SparkeyReader duplicate() {
    // Already immutable and thread-safe - just return self
    return this;
  }

  @Override
  public void close() {
    // Close memory-mapped files to free resources
    indexData.close();
    logData.close();
  }

  @Override
  public Iterator<Entry> iterator() {
    // Create raw log iterator
    Iterator<Entry> rawIterator = new UncompressedLogIteratorJ22(logData, logHeader);

    // Wrap with hash validation to filter out DELETEs and superseded entries
    return new Iterator<Entry>() {
      private Entry nextValidEntry = null;
      private boolean hasNextCached = false;

      @Override
      public boolean hasNext() {
        if (hasNextCached) {
          return true;
        }

        // Search for next valid entry
        while (rawIterator.hasNext()) {
          Entry entry = rawIterator.next();

          // Skip DELETE entries
          if (entry.getType() == SparkeyReader.Type.DELETE) {
            continue;
          }

          // Check if this entry is current (not superseded)
          try {
            // Direct cast - we know the exact type since we created the iterator
            UncompressedLogIteratorJ22.LogEntry logEntry =
              (UncompressedLogIteratorJ22.LogEntry) entry;

            int keyLen = logEntry.getKeyLength();
            byte[] key = logEntry.getKeyBuf();
            long position = logEntry.getPosition();
            int entryIndex = logEntry.getEntryIndex();

            // Validate against index hash
            if (indexHash.isAt(keyLen, key, position, entryIndex)) {
              // This is the current version!
              nextValidEntry = entry;
              hasNextCached = true;
              return true;
            }
            // else: superseded entry, skip it
          } catch (IOException e) {
            throw new RuntimeException("Error validating entry", e);
          }
        }

        // No more valid entries
        return false;
      }

      @Override
      public Entry next() {
        if (!hasNext()) {
          throw new java.util.NoSuchElementException();
        }
        hasNextCached = false;
        return nextValidEntry;
      }
    };
  }

  @Override
  public long getLoadedBytes() {
    // MemorySegment is all-or-nothing, not lazily paged in like MappedByteBuffer
    return 0;
  }

  @Override
  public long getTotalBytes() {
    return indexFile.length() + logFile.length();
  }
}
