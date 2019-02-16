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

import java.io.IOException;
import java.util.ArrayList;

class InMemoryData implements ReadWriteData {
  private static final int CHUNK_SIZE = 1 << 30;
  private static final int BITMASK_30 = ((1 << 30) - 1);

  protected final byte[][] chunks;
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

    final ArrayList<byte[]> chunksBuffer = new ArrayList<>();
    long offset = 0;
    while (offset < size) {
      long remaining = size - offset;
      int chunkSize = (int) Math.min(remaining, CHUNK_SIZE);
      chunksBuffer.add(new byte[chunkSize]);
      offset += CHUNK_SIZE;
    }
    chunks = chunksBuffer.toArray(new byte[chunksBuffer.size()][]);
    numChunks = chunks.length;

    curChunkIndex = 0;
    curChunk = chunks[0];
  }

  public void writeLittleEndianLong(long value) throws IOException {
    writeUnsignedByte((int) ((value) & 0xFF));
    writeUnsignedByte((int) ((value >>> 8) & 0xFF));
    writeUnsignedByte((int) ((value >>> 16) & 0xFF));
    writeUnsignedByte((int) ((value >>> 24) & 0xFF));
    writeUnsignedByte((int) ((value >>> 32) & 0xFF));
    writeUnsignedByte((int) ((value >>> 40) & 0xFF));
    writeUnsignedByte((int) ((value >>> 48) & 0xFF));
    writeUnsignedByte((int) ((value >>> 56) & 0xFF));
  }

  public void writeLittleEndianInt(int value) throws IOException {
    writeUnsignedByte((value) & 0xFF);
    writeUnsignedByte((value >>> 8) & 0xFF);
    writeUnsignedByte((value >>> 16) & 0xFF);
    writeUnsignedByte((value >>> 24) & 0xFF);
  }

  @Override
  public void close() throws IOException {
    for (int i = 0; i < numChunks; i++) {
      chunks[i] = null;
    }
    curChunk = null;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos > size) {
      throw new IOException("Corrupt index: referencing data outside of range");
    }
    int chunkIndex = (int) (pos >>> 30);
    curChunkIndex = chunkIndex;
    curChunk = chunks[chunkIndex];
    curChunkPos = ((int) pos) & BITMASK_30;
  }

  @Override
  public void writeUnsignedByte(int value) throws IOException {
    if (curChunkPos == CHUNK_SIZE) {
      next();
    }
    curChunk[curChunkPos++] = (byte) value;
  }

  private void next() throws IOException {
    curChunkIndex++;
    if (curChunkIndex >= chunks.length) {
      throw new IOException("Corrupt index: referencing data outside of range");
    }
    curChunk = chunks[curChunkIndex];
    curChunkPos = 0;
  }

  @Override
  public int readUnsignedByte() throws IOException {
    if (curChunkPos == CHUNK_SIZE) {
      next();
    }
    return Util.unsignedByte(curChunk[curChunkPos++]);
  }

  @Override
  public int readLittleEndianInt() throws IOException {
    return Util.readLittleEndianIntSlowly(this);
  }

  @Override
  public long readLittleEndianLong() throws IOException {
    return Util.readLittleEndianLongSlowly(this);
  }

  @Override
  public String toString() {
    return "InMemoryData{" +
            "size=" + size +
            '}';
  }
}
