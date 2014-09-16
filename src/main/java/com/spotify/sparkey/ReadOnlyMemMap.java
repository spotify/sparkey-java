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

import com.google.common.collect.Lists;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

final class ReadOnlyMemMap implements RandomAccessData {
  private static final ScheduledExecutorService CLEANER = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
    @Override
    public Thread newThread(Runnable r) {
      Thread thread = new Thread(r);
      thread.setName(ReadOnlyMemMap.class.getSimpleName() + "-cleaner");
      thread.setDaemon(true);
      return thread;
    }
  });

  private static final long MAP_SIZE = 1 << 30;
  private static final long BITMASK_30 = ((1L << 30) - 1);
  private final File file;

  private volatile MappedByteBuffer[] chunks;
  private final RandomAccessFile randomAccessFile;
  private final long size;
  private final int numChunks;

  private int curChunkIndex;
  private volatile MappedByteBuffer curChunk;

  // Used for making sure we close all instances before cleaning up the byte buffers
  private final List<ReadOnlyMemMap> allInstances;

  ReadOnlyMemMap(File file) throws IOException {
    this.file = file;
    this.allInstances = Lists.newArrayList();
    this.allInstances.add(this);

    this.randomAccessFile = new RandomAccessFile(file, "r");
    this.size = file.length();
    if (size <= 0) {
      throw new IllegalArgumentException("Non-positive size: " + size);
    }
    long numFullMaps = (size - 1) >> 30;
    if (numFullMaps >= Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Too large size: " + size);
    }
    long sizeFullMaps = numFullMaps * MAP_SIZE;

    numChunks = (int) (numFullMaps + 1);
    chunks = new MappedByteBuffer[numChunks];
    long offset = 0;
    for (int i = 0; i < numFullMaps; i++) {
      chunks[i] = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, offset, MAP_SIZE);
      offset += MAP_SIZE;
    }
    long lastSize = size - sizeFullMaps;
    if (lastSize > 0) {
      chunks[numChunks - 1] = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, offset, lastSize);
    }

    curChunkIndex = 0;
    curChunk = chunks[0];
    curChunk.position(0);
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
    synchronized (allInstances) {
      if (this.chunks == null) {
        return;
      }
      chunks = this.chunks;
      for (ReadOnlyMemMap map : allInstances) {
        map.chunks = null;
        map.curChunk = null;
        try {
          map.randomAccessFile.close();
        } catch (IOException e) {
          e.printStackTrace(System.err);
        }
      }
    }
    // Wait a bit with closing so that all threads have a chance to see the that
    // chunks and curChunks are null
    CLEANER.schedule(new Runnable() {
      @Override
      public void run() {
        for (MappedByteBuffer chunk : chunks) {
          ByteBufferCleaner.cleanMapping(chunk);
        }
      }
    }, 1000, TimeUnit.MILLISECONDS);
  }

  public void seek(long pos) throws IOException {
    if (pos > size) {
      throw corruptionException();
    }
    int partIndex = (int) (pos >>> 30);
    curChunkIndex = partIndex;
    MappedByteBuffer[] chunks = getChunks();
    MappedByteBuffer curChunk = chunks[partIndex];
    curChunk.position((int) (pos & BITMASK_30));
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

  private MappedByteBuffer[] getChunks() throws SparkeyReaderClosedException {
    MappedByteBuffer[] localChunks = chunks;
    if (localChunks == null) {
      throw closedException();
    }
    return localChunks;
  }

  private MappedByteBuffer getCurChunk() throws SparkeyReaderClosedException {
    MappedByteBuffer curChunk = this.curChunk;
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
