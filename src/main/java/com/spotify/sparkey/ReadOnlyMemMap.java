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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

final class ReadOnlyMemMap implements RandomAccessData {
  // Not marked as final to support tweaking for test purposes
  static int MAP_SIZE_BITS = 30;
  private final int mapBits = MAP_SIZE_BITS;
  private final long mapSize = 1 << mapBits;
  private final long mapBitmask = ((1L << mapBits) - 1);
  private final File file;
  private final boolean heapBacked;

  private final ReadOnlyMemMap source;
  private volatile ByteBuffer[] chunks;
  private final RandomAccessFile randomAccessFile;
  private final long size;
  private final int numChunks;

  private int curChunkIndex;
  private volatile ByteBuffer curChunk;

  // Used for making sure we close all instances before cleaning up the byte buffers
  private final Set<ReadOnlyMemMap> allInstances;

  ReadOnlyMemMap(File file) throws IOException {
    this.source = this;
    this.file = file;
    this.heapBacked = false;
    this.allInstances = Collections.newSetFromMap(new IdentityHashMap<>());
    this.allInstances.add(this);

    if (mapBits > 30) {
      throw new IllegalStateException("Map bits may not exceed 30");
    }

    if (mapBits < 10) {
      throw new IllegalStateException("Map bits may not be less than 10");
    }

    this.randomAccessFile = new RandomAccessFile(file, "r");
    Sparkey.incrOpenFiles();
    try {
      this.size = file.length();
      if (size <= 0) {
        throw new IllegalArgumentException("Non-positive size: " + size);
      }
      final ArrayList<ByteBuffer> chunksBuffer = new ArrayList<>();
      long offset = 0;
      while (offset < size) {
        long remaining = size - offset;
        long chunkSize = Math.min(remaining, mapSize);
        chunksBuffer.add(createMappedChunk(offset, chunkSize));
        offset += mapSize;
      }
      chunks = chunksBuffer.toArray(new ByteBuffer[0]);
      numChunks = chunks.length;

      curChunkIndex = 0;
      curChunk = chunks[0];
      curChunk.position(0);
      Sparkey.incrOpenMaps();
    } catch (Throwable e) {
      Sparkey.decrOpenFiles();
      this.randomAccessFile.close();
      throw e;
    }
  }

  private ReadOnlyMemMap(File file, ByteBuffer[] chunks, long size) {
    this.source = this;
    this.file = file;
    this.heapBacked = true;
    this.randomAccessFile = null;
    this.allInstances = Collections.newSetFromMap(new IdentityHashMap<>());
    this.allInstances.add(this);
    this.size = size;
    this.chunks = chunks;
    this.numChunks = chunks.length;
    curChunkIndex = 0;
    curChunk = chunks[0];
    curChunk.position(0);
    Sparkey.incrOpenMaps();
  }

  static ReadOnlyMemMap fromHeap(File file) throws IOException {
    long size = file.length();
    if (size <= 0) {
      throw new IllegalArgumentException("Non-positive size: " + size);
    }
    int mapBits = MAP_SIZE_BITS;
    long mapSize = 1L << mapBits;

    ArrayList<ByteBuffer> chunksBuffer = new ArrayList<>();
    try (FileInputStream fis = new FileInputStream(file)) {
      long remaining = size;
      while (remaining > 0) {
        int chunkSize = (int) Math.min(remaining, mapSize);
        byte[] data = new byte[chunkSize];
        int read = 0;
        while (read < chunkSize) {
          int n = fis.read(data, read, chunkSize - read);
          if (n < 0) {
            throw new IOException("Unexpected end of file: " + file);
          }
          read += n;
        }
        ByteBuffer buf = ByteBuffer.wrap(data);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        chunksBuffer.add(buf);
        remaining -= chunkSize;
      }
    }
    return new ReadOnlyMemMap(file, chunksBuffer.toArray(new ByteBuffer[0]), size);
  }

  private MappedByteBuffer createMappedChunk(final long offset, final long size) throws IOException {
    final MappedByteBuffer map = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, offset, size);
    map.order(ByteOrder.LITTLE_ENDIAN);
    return map;
  }

  private ReadOnlyMemMap(ReadOnlyMemMap source, ByteBuffer[] chunks) {
    this.source = source;
    this.file = source.file;
    this.heapBacked = source.heapBacked;
    this.allInstances = source.allInstances;
    this.randomAccessFile = source.randomAccessFile;
    this.size = source.size;
    this.numChunks = source.numChunks;
    this.chunks = chunks;
    curChunkIndex = 0;
    curChunk = chunks[0];
    curChunk.position(0);
  }

  public void close() {
    final ByteBuffer[] chunks;
    final boolean onlyUser;
    synchronized (allInstances) {
      if (this.chunks == null) {
        return;
      }
      chunks = this.chunks;
      onlyUser = allInstances.size() == 1;
      for (ReadOnlyMemMap map : allInstances) {
        map.chunks = null;
        map.curChunk = null;
      }
      if (randomAccessFile != null) {
        Sparkey.decrOpenFiles();
        Util.nonThrowingClose(randomAccessFile);
      }
    }
    Sparkey.decrOpenMaps();
    ByteBufferCleaner.cleanChunks(chunks, !onlyUser);
  }

  void closeDuplicate() {
    this.chunks = null;
    this.curChunk = null;
    synchronized (allInstances) {
      allInstances.remove(this);
    }
  }

  public void seek(long pos) throws IOException {
    if (pos > size) {
      throw corruptionException();
    }
    int partIndex = (int) (pos >>> mapBits);
    curChunkIndex = partIndex;
    ByteBuffer[] chunks = getChunks();
    ByteBuffer curChunk = chunks[partIndex];
    curChunk.position((int) (pos & mapBitmask));
    this.curChunk = curChunk;
  }

  private void next() throws IOException {
    ByteBuffer[] chunks = getChunks();
    curChunkIndex++;
    if (curChunkIndex >= chunks.length) {
      throw corruptionException();
    }
    ByteBuffer curChunk = chunks[curChunkIndex];
    if (curChunk == null) {
      throw new RuntimeException("chunk == null");
    }
    curChunk.position(0);
    this.curChunk = curChunk;
  }

  @Override
  public int readUnsignedByte() throws IOException {
    ByteBuffer curChunk = getCurChunk();
    if (curChunk.remaining() == 0) {
      next();
      curChunk = getCurChunk();
    }
    return ((int) curChunk.get()) & 0xFF;
  }

  @Override
  public int readLittleEndianInt() throws IOException {
    ByteBuffer curChunk = getCurChunk();
    if (curChunk.remaining() >= 4) {
      return curChunk.getInt();
    }

    // Value is on the chunk boundary - edge case so it is ok if it's a bit slower.
    return Util.readLittleEndianIntSlowly(this);
  }

  @Override
  public long readLittleEndianLong() throws IOException {
    ByteBuffer curChunk = getCurChunk();
    if (curChunk.remaining() >= 8) {
      return curChunk.getLong();
    }

    // Value is on the chunk boundary - edge case so it is ok if it's a bit slower.
    return Util.readLittleEndianLongSlowly(this);
  }

  public void readFully(byte[] buffer, int offset, int length) throws IOException {
    ByteBuffer curChunk = getCurChunk();
    long remaining = curChunk.remaining();
    if (remaining >= length) {
      curChunk.get(buffer, offset, length);
    } else {
      int remaining1 = (int) remaining;
      curChunk.get(buffer, offset, remaining1);
      length -= remaining1;
      offset += remaining1;
      next();
      readFully(buffer, offset, length);
    }
  }

  public boolean readFullyCompare(int length, byte[] key) throws IOException {
    ByteBuffer curChunk = getCurChunk();
    int remaining = curChunk.remaining();
    if (remaining >= length) {
      // Fast path: all bytes are in current chunk
      int pos = curChunk.position();
      for (int i = 0; i < length; i++) {
        if (curChunk.get(pos + i) != key[i]) {
          // Still advance position even on mismatch (matches readFully semantics)
          curChunk.position(pos + length);
          return false;
        }
      }
      curChunk.position(pos + length);
      return true;
    } else {
      // Slow path: comparison spans chunk boundary
      int keyOffset = 0;
      while (keyOffset < length) {
        curChunk = getCurChunk();
        int available = Math.min(curChunk.remaining(), length - keyOffset);
        int pos = curChunk.position();
        for (int i = 0; i < available; i++) {
          if (curChunk.get(pos + i) != key[keyOffset + i]) {
            // Still advance position even on mismatch
            curChunk.position(pos + available);
            skipBytes(length - keyOffset - available);
            return false;
          }
        }
        curChunk.position(pos + available);
        keyOffset += available;
        if (keyOffset < length) {
          next();
        }
      }
      return true;
    }
  }

  public void skipBytes(long amount) throws IOException {
    ByteBuffer curChunk = getCurChunk();
    int remaining = curChunk.remaining();
    if (remaining >= amount) {
      curChunk.position((int) (curChunk.position() + amount));
    } else {
      next();
      skipBytes(amount - remaining);
    }
  }

  public long getLoadedBytes() {
    if (heapBacked) {
      return size;
    }
    long bytes = 0;
    for (ByteBuffer chunk : source.chunks) {
      if (((MappedByteBuffer) chunk).isLoaded()) {
        bytes += chunk.capacity();
      }
    }
    return bytes;
  }

  /** Load all chunks into the OS page cache (mmap) or no-op (heap). */
  void loadPages() {
    if (heapBacked) {
      return;
    }
    ByteBuffer[] localChunks = source.chunks;
    if (localChunks != null) {
      for (ByteBuffer chunk : localChunks) {
        ((MappedByteBuffer) chunk).load();
      }
    }
  }

  boolean isHeapBacked() {
    return heapBacked;
  }

  long size() {
    return size;
  }

  private ByteBuffer[] getChunks() throws SparkeyReaderClosedException {
    ByteBuffer[] localChunks = chunks;
    if (localChunks == null) {
      throw closedException();
    }
    return localChunks;
  }

  private ByteBuffer getCurChunk() throws SparkeyReaderClosedException {
    final ByteBuffer curChunk = this.curChunk;
    if (curChunk == null) {
      throw closedException();
    }
    return curChunk;
  }

  private IOException corruptionException() {
    return new CorruptedIndexException("Index is likely corrupt (" + file.getPath() + "), referencing data outside of range");
  }

  private SparkeyReaderClosedException closedException() {
    return new SparkeyReaderClosedException("Reader has been closed");
  }

  public ReadOnlyMemMap duplicate() {
    synchronized (allInstances) {
      if (chunks == null) {
        // Duplicating a closed instance is silly, and there's no point in actually duplicating it
        return this;
      }
      ByteBuffer[] chunks = new ByteBuffer[numChunks];
      for (int i = 0; i < numChunks; i++) {
        chunks[i] = this.chunks[i].duplicate();
        chunks[i].order(ByteOrder.LITTLE_ENDIAN);
      }
      ReadOnlyMemMap duplicate = new ReadOnlyMemMap(source, chunks);
      allInstances.add(duplicate);
      return duplicate;
    }
  }

  @Override
  public String toString() {
    return "ReadOnlyMemMap{" +
            ", randomAccessFile=" + randomAccessFile +
            ", size=" + size +
            '}';
  }

}
