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

  private volatile MappedByteBuffer[] chunks;
  private final RandomAccessFile randomAccessFile;
  private final long size;
  private final int numChunks;

  private int curChunkIndex;
  private volatile MappedByteBuffer curChunk;

  // Used for making sure we close all instances before cleaning up the byte buffers
  private final Set<ReadOnlyMemMap> allInstances;

  ReadOnlyMemMap(File file) throws IOException {
    this.file = file;
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
      final ArrayList<MappedByteBuffer> chunksBuffer = new ArrayList<>();
      long offset = 0;
      while (offset < size) {
        long remaining = size - offset;
        long chunkSize = Math.min(remaining, mapSize);
        chunksBuffer.add(createChunk(offset, chunkSize));
        offset += mapSize;
      }
      chunks = chunksBuffer.toArray(new MappedByteBuffer[chunksBuffer.size()]);
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

  private MappedByteBuffer createChunk(final long offset, final long size) throws IOException {
    final MappedByteBuffer map = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, offset, size);
    map.order(ByteOrder.LITTLE_ENDIAN);
    return map;
  }

  private ReadOnlyMemMap(ReadOnlyMemMap source, MappedByteBuffer[] chunks) {
    this.file = source.file;
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
    final MappedByteBuffer[] chunks;
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
      Sparkey.decrOpenFiles();
      Util.nonThrowingClose(randomAccessFile);
    }
    if (!onlyUser) {
      // Wait a bit with closing so that all threads have a chance to see the that
      // chunks and curChunks are null. If the sleep time is too short, the JVM can crash
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    Sparkey.decrOpenMaps();
    for (MappedByteBuffer chunk : chunks) {
      ByteBufferCleaner.cleanMapping(chunk);
    }
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
    MappedByteBuffer[] chunks = getChunks();
    MappedByteBuffer curChunk = chunks[partIndex];
    curChunk.position((int) (pos & mapBitmask));
    this.curChunk = curChunk;
  }

  private void next() throws IOException {
    MappedByteBuffer[] chunks = getChunks();
    curChunkIndex++;
    if (curChunkIndex >= chunks.length) {
      throw corruptionException();
    }
    MappedByteBuffer curChunk = chunks[curChunkIndex];
    if (curChunk == null) {
      throw new RuntimeException("chunk == null");
    }
    curChunk.position(0);
    this.curChunk = curChunk;
  }

  @Override
  public int readUnsignedByte() throws IOException {
    MappedByteBuffer curChunk = getCurChunk();
    if (curChunk.remaining() == 0) {
      next();
      curChunk = getCurChunk();
    }
    return ((int) curChunk.get()) & 0xFF;
  }

  @Override
  public int readLittleEndianInt() throws IOException {
    MappedByteBuffer curChunk = getCurChunk();
    if (curChunk.remaining() >= 4) {
      return curChunk.getInt();
    }

    // Value is on the chunk boundary - edge case so it is ok if it's a bit slower.
    return Util.readLittleEndianIntSlowly(this);
  }

  @Override
  public long readLittleEndianLong() throws IOException {
    MappedByteBuffer curChunk = getCurChunk();
    if (curChunk.remaining() >= 8) {
      return curChunk.getLong();
    }

    // Value is on the chunk boundary - edge case so it is ok if it's a bit slower.
    return Util.readLittleEndianLongSlowly(this);
  }

  public void readFully(byte[] buffer, int offset, int length) throws IOException {
    MappedByteBuffer curChunk = getCurChunk();
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

  public void skipBytes(long amount) throws IOException {
    MappedByteBuffer curChunk = getCurChunk();
    int remaining = curChunk.remaining();
    if (remaining >= amount) {
      curChunk.position((int) (curChunk.position() + amount));
    } else {
      next();
      skipBytes(amount - remaining);
    }
  }

  public long getLoadedBytes() {
    long bytes = 0;
    for (MappedByteBuffer chunk : chunks) {
      if (chunk.isLoaded()) {
        bytes += chunk.capacity();
      }
    }
    return bytes;
  }

  private MappedByteBuffer[] getChunks() throws SparkeyReaderClosedException {
    MappedByteBuffer[] localChunks = chunks;
    if (localChunks == null) {
      throw closedException();
    }
    return localChunks;
  }

  private MappedByteBuffer getCurChunk() throws SparkeyReaderClosedException {
    final MappedByteBuffer curChunk = this.curChunk;
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
      MappedByteBuffer[] chunks = new MappedByteBuffer[numChunks];
      for (int i = 0; i < numChunks; i++) {
        chunks[i] = (MappedByteBuffer) this.chunks[i].duplicate();
        chunks[i].order(ByteOrder.LITTLE_ENDIAN);
      }
      ReadOnlyMemMap duplicate = new ReadOnlyMemMap(this, chunks);
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
