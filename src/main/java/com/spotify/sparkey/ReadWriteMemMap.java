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

final class ReadWriteMemMap implements ReadWriteData {
  private static final long MAP_SIZE = 1 << 30;
  private static final long BITMASK_30 = ((1L << 30) - 1);
  private final File file;

  private volatile MappedByteBuffer[] chunks;
  private final RandomAccessFile randomAccessFile;
  private final IndexHeader header;
  private final boolean fsync;
  private final long size;

  private int curChunkIndex;
  private volatile MappedByteBuffer curChunk;
  private boolean closed = false;

  ReadWriteMemMap(final long size, final File file, final IndexHeader header, final boolean fsync)
      throws IOException {
    this.size = size;
    if (this.size <= 0) {
      throw new IllegalArgumentException("Non-positive size: " + this.size);
    }

    this.file = file;

    this.randomAccessFile = new RandomAccessFile(file, "rw");
    Sparkey.incrOpenFiles();
    this.header = header;
    this.fsync = fsync;
    this.randomAccessFile.setLength(header.size() + size);
    try {
      final ArrayList<MappedByteBuffer> chunksBuffer = new ArrayList<>();
      long offset = 0;
      while (offset < this.size) {
        long remaining = this.size - offset;
        long chunkSize = Math.min(remaining, MAP_SIZE);
        chunksBuffer.add(createChunk(header.size() + offset, chunkSize));
        offset += MAP_SIZE;
      }
      chunks = chunksBuffer.toArray(new MappedByteBuffer[chunksBuffer.size()]);

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

  @Override
  public void writeLittleEndianLong(long value) throws IOException {
    MappedByteBuffer curChunk = getCurChunk();
    if (curChunk.remaining() >= 8) {
      curChunk.putLong(value);
      return;
    }

    // Value is on the chunk boundary - edge case so it is ok if it's a bit slower.
    writeUnsignedByte((int) ((value) & 0xFF));
    writeUnsignedByte((int) ((value >>> 8) & 0xFF));
    writeUnsignedByte((int) ((value >>> 16) & 0xFF));
    writeUnsignedByte((int) ((value >>> 24) & 0xFF));
    writeUnsignedByte((int) ((value >>> 32) & 0xFF));
    writeUnsignedByte((int) ((value >>> 40) & 0xFF));
    writeUnsignedByte((int) ((value >>> 48) & 0xFF));
    writeUnsignedByte((int) ((value >>> 56) & 0xFF));
  }

  @Override
  public void writeLittleEndianInt(int value) throws IOException {
    MappedByteBuffer curChunk = getCurChunk();
    if (curChunk.remaining() >= 4) {
      curChunk.putInt(value);
      return;
    }

    // Value is on the chunk boundary - edge case so it is ok if it's a bit slower.
    writeUnsignedByte((value) & 0xFF);
    writeUnsignedByte((value >>> 8) & 0xFF);
    writeUnsignedByte((value >>> 16) & 0xFF);
    writeUnsignedByte((value >>> 24) & 0xFF);
  }

  private MappedByteBuffer createChunk(final long offset, final long size) throws IOException {
    final MappedByteBuffer map = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, offset, size);
    map.order(ByteOrder.LITTLE_ENDIAN);
    return map;
  }

  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    randomAccessFile.seek(0);
    randomAccessFile.write(header.asBytes());
    if (fsync) {
      randomAccessFile.getFD().sync();
    }
    randomAccessFile.close();
    final MappedByteBuffer[] chunks = this.chunks;
    this.chunks = null;
    curChunk = null;
    Sparkey.decrOpenFiles();
    Util.nonThrowingClose(randomAccessFile);

    // Clean it up immediately since this should only be used from a single thread anyway
    Sparkey.decrOpenMaps();
    for (int i = 0; i < chunks.length; i++) {
      final MappedByteBuffer byteBuffer = chunks[i];
      chunks[i] = null;
      ByteBufferCleaner.cleanMapping(byteBuffer);
    }
  }

  @Override
  public void writeUnsignedByte(final int value) throws IOException {
    MappedByteBuffer curChunk = getCurChunk();
    if (curChunk.remaining() == 0) {
      next();
      curChunk = getCurChunk();
    }
    curChunk.put((byte) value);
  }

  @Override
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

  @Override
  public String toString() {
    return "ReadWriteMemMap{" +
            ", randomAccessFile=" + randomAccessFile +
            ", size=" + size +
            '}';
  }

}
