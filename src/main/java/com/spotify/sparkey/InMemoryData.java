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

import java.io.FileOutputStream;
import java.io.IOException;

final class InMemoryData implements RandomAccessData {
  private static final int CHUNK_SIZE = 1 << 30;
  private static final int BITMASK_30 = ((1 << 30) - 1);

  private final byte[][] chunks;
  private final long size;
  private final int numChunks;

  private int curChunkIndex;
  private byte[] curChunk;
  private int curChunkPos;

  InMemoryData(long size) {
    this.size = size;
    if (size < 0) {
      throw new IllegalArgumentException("Negative size: " + size);
    }
    long numFullMaps = (size - 1) >> 30;
    if (numFullMaps >= Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Too large size: " + size);
    }
    long sizeFullMaps = numFullMaps * CHUNK_SIZE;

    numChunks = (int) (numFullMaps + 1);
    chunks = new byte[numChunks][];
    for (int i = 0; i < numFullMaps; i++) {
      chunks[i] = new byte[CHUNK_SIZE];
    }
    long lastSize = size - sizeFullMaps;
    if (lastSize > 0) {
      chunks[numChunks - 1] = new byte[(int) lastSize];
    }

    curChunkIndex = 0;
    curChunk = chunks[0];
  }

  void close() {
    for (int i = 0; i < numChunks; i++) {
      chunks[i] = null;
    }
    curChunk = null;
  }

  void seek(long pos) {
    int chunkIndex = (int) (pos >>> 30);
    curChunkIndex = chunkIndex;
    curChunk = chunks[chunkIndex];
    curChunkPos = ((int) pos) & BITMASK_30;
  }

  void writeUnsignedByte(int value) {
    if (curChunkPos == CHUNK_SIZE) {
      next();
    }
    curChunk[curChunkPos++] = (byte) value;
  }

  private void next() {
    curChunkIndex++;
    curChunk = chunks[curChunkIndex];
    curChunkPos = 0;
  }

  @Override
  public int readUnsignedByte() {
    if (curChunkPos == CHUNK_SIZE) {
      next();
    }
    return Util.unsignedByte(curChunk[curChunkPos++]);
  }

  @Override
  public String toString() {
    return "InMemoryData{" +
            "size=" + size +
            '}';
  }

  public void flushToFile(FileOutputStream stream) throws IOException {
    for (byte[] chunk : chunks) {
      stream.write(chunk);
    }
  }
}
