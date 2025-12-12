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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NoSuchElementException;

public final class SingleThreadedSparkeyReaderJ22 implements SparkeyReader {
  private final IndexHashJ22 index;
  private final File indexFile;
  private final File logFile;
  private final IndexHeader header;
  private final LogHeader logHeader;

  private SingleThreadedSparkeyReaderJ22(File indexFile, File logFile) throws IOException {
    this(indexFile, logFile, IndexHashJ22.open(indexFile, logFile));
  }

  private SingleThreadedSparkeyReaderJ22(File indexFile, File logFile, IndexHashJ22 index) {
    this.index = index;
    this.indexFile = indexFile;
    this.logFile = logFile;
    header = index.header;
    logHeader = index.logHeader;
  }

  @Override
  public SingleThreadedSparkeyReaderJ22 duplicate() {
    return new SingleThreadedSparkeyReaderJ22(indexFile, logFile, index.duplicate());
  }

  public static SparkeyReader open(File file) throws IOException {
    return new SingleThreadedSparkeyReaderJ22(Sparkey.getIndexFile(file), Sparkey.getLogFile(file));
  }

  @Override
  public void close() {
    index.close();
  }

  @Override
  public String getAsString(String key) throws IOException {
    byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
    Entry res = getAsEntry(keyBytes);
    if (res == null) {
      return null;
    }
    return new String(res.getValue(), StandardCharsets.UTF_8);
  }

  @Override
  public byte[] getAsByteArray(byte[] key) throws IOException {
    Entry entry = getAsEntry(key);
    if (entry == null) {
      return null;
    }
    return entry.getValue();
  }

  /**
   * Get entry for the given key.
   *
   * IMPORTANT: The returned entry must be consumed immediately (call getValue() or
   * getValueAsStream()) before making another lookup, as the underlying stream
   * position will be invalidated.
   */
  @Override
  public SparkeyReader.Entry getAsEntry(byte[] key) throws IOException {
    return index.get(key.length, key);
  }


  /**
   * @return a new iterator that can be safely used from a single thread.
   * Note that entries will be reused and modified, so any data you want from it must be consumed before
   * continuing iteration. You should not pass this entry on in any way.
   */
  @Override
  public Iterator<SparkeyReader.Entry> iterator() {
    SparkeyLogIterator logIterator;
    final IndexHashJ22 indexHash;
    try {
      logIterator = new SparkeyLogIterator(logFile, -1, index.header.getDataEnd());
      indexHash = index.duplicate();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    final Iterator<SparkeyReader.Entry> iterator = logIterator.iterator();

    return new Iterator<SparkeyReader.Entry>() {
      private SparkeyReader.Entry entry;
      private boolean ready;

      public boolean hasNext() {
        if (ready) {
          return true;
        }
        while (iterator.hasNext()) {
          // Safe cast, since the iterator is guaranteed to be a SparkeyLogIterator
          SparkeyLogIterator.Entry next = (SparkeyLogIterator.Entry) iterator.next();

          if (next.getType() == SparkeyReader.Type.PUT) {
            int keyLen = next.getKeyLength();
            try {
              if (isValid(keyLen, next.getKeyBuf(), next.getPosition(), next.getEntryIndex(), indexHash)) {
                entry = next;
                ready = true;
                return true;
              }
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
        indexHash.closeDuplicate();

        return false;
      }

      public Entry next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        ready = false;
        Entry localEntry = entry;
        entry = null;
        return localEntry;
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public long getLoadedBytes() {
    return index.getLoadedBytes();
  }

  @Override
  public long getTotalBytes() {
    return indexFile.length() + logFile.length();
  }

  private static boolean isValid(int keyLen, byte[] keyBuf, long position, int entryIndex, IndexHashJ22 indexHash) throws IOException {
    return indexHash.isAt(keyLen, keyBuf, position, entryIndex);
  }

  @Override
  public IndexHeader getIndexHeader() {
    return header;
  }

  @Override
  public LogHeader getLogHeader() {
    return logHeader;
  }

}
