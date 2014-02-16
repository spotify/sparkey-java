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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

final class ReadOnlyMemMap implements RandomAccessData {
  private static final long MAP_SIZE = 1 << 30;
  private static final long BITMASK_30 = ((1L << 30) - 1);

  private final MappedByteBuffer[] chunks;
  private final RandomAccessFile randomAccessFile;
  private final long size;
  private final int numChunks;

  private int curChunkIndex;
  private MappedByteBuffer curChunk;


  ReadOnlyMemMap(File file) throws IOException {
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

  private ReadOnlyMemMap(ReadOnlyMemMap largeMemMap) {
    this.randomAccessFile = largeMemMap.randomAccessFile;
    this.size = largeMemMap.size;
    this.numChunks = largeMemMap.numChunks;
    this.chunks = new MappedByteBuffer[numChunks];

    for (int i = 0; i < numChunks; i++) {
      chunks[i] = (MappedByteBuffer) largeMemMap.chunks[i].duplicate();
    }

    curChunkIndex = 0;
    curChunk = chunks[0];
    curChunk.position(0);
  }

  public void close() {
    for (int i = 0; i < numChunks; i++) {
      chunks[i] = null;
    }
    curChunk = null;
    try {
      randomAccessFile.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void seek(long pos) throws IOException {
    if (pos > size) {
      throw new IOException("Corrupt index: referencing data outside of range");
    }
    int partIndex = (int) (pos >>> 30);
    curChunkIndex = partIndex;
    curChunk = chunks[partIndex];
    curChunk.position((int) (pos & BITMASK_30));
  }

  private void next() throws IOException {
    curChunkIndex++;
    if (curChunkIndex >= chunks.length) {
      throw new IOException("Corrupt index: referencing data outside of range");
    }
    curChunk = chunks[curChunkIndex];
    curChunk.position(0);
  }

  @Override
  public int readUnsignedByte() throws IOException {
    if (curChunk.remaining() == 0) {
      next();
    }
    return ((int) curChunk.get()) & 0xFF;
  }

  public void readFully(byte[] buffer, int offset, int length) throws IOException {
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
    int remaining = curChunk.remaining();
    if (remaining >= amount) {
      curChunk.position((int) (curChunk.position() + amount));
    } else {
      next();
      skipBytes(amount - remaining);
    }
  }

  public ReadOnlyMemMap duplicate() {
    return new ReadOnlyMemMap(this);
  }

  @Override
  public String toString() {
    return "ReadOnlyMemMap{" +
            ", randomAccessFile=" + randomAccessFile +
            ", size=" + size +
            '}';
  }
}
