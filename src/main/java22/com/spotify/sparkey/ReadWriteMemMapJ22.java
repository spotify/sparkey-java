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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

final class ReadWriteMemMapJ22 implements ReadWriteData {
  // ValueLayouts for little-endian access
  private static final ValueLayout.OfByte JAVA_BYTE = ValueLayout.JAVA_BYTE;
  private static final ValueLayout.OfInt JAVA_INT_LE = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
  private static final ValueLayout.OfLong JAVA_LONG_LE = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

  private final File file;
  private final Arena arena;
  private final RandomAccessFile randomAccessFile;
  private final IndexHeader header;
  private final boolean fsync;
  private final long size;

  private final MemorySegment segment;
  private long position;  // Current position within the segment
  private boolean closed = false;

  ReadWriteMemMapJ22(final long size, final File file, final IndexHeader header, final boolean fsync)
      throws IOException {
    this.size = size;
    if (this.size <= 0) {
      throw new IllegalArgumentException("Non-positive size: " + this.size);
    }

    this.file = file;
    this.arena = Arena.ofShared();  // Shared arena for multi-threaded access

    this.randomAccessFile = new RandomAccessFile(file, "rw");
    Sparkey.incrOpenFiles();
    this.header = header;
    this.fsync = fsync;
    this.randomAccessFile.setLength(header.size() + size);
    try {
      // Map the entire file as a single segment - no 2GB limit with MemorySegment!
      try (FileChannel channel = FileChannel.open(file.toPath(),
                                                 StandardOpenOption.READ,
                                                 StandardOpenOption.WRITE)) {
        this.segment = channel.map(FileChannel.MapMode.READ_WRITE, header.size(), size, arena);
      }
      this.position = 0;
      Sparkey.incrOpenMaps();
    } catch (Throwable e) {
      arena.close();
      Sparkey.decrOpenFiles();
      this.randomAccessFile.close();
      throw e;
    }
  }

  @Override
  public void writeLittleEndianLong(long value) throws IOException {
    try {
      segment.set(JAVA_LONG_LE, position, value);
      position += 8;
    } catch (IllegalStateException e) {
      throw closedException();
    }
  }

  @Override
  public void writeLittleEndianInt(int value) throws IOException {
    try {
      segment.set(JAVA_INT_LE, position, value);
      position += 4;
    } catch (IllegalStateException e) {
      throw closedException();
    }
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
    Sparkey.decrOpenFiles();
    Util.nonThrowingClose(randomAccessFile);

    // Arena-based cleanup: deterministic and safe!
    // No need to null out segment - Arena.close() makes it inaccessible
    Sparkey.decrOpenMaps();
    arena.close();
  }

  @Override
  public void writeUnsignedByte(final int value) throws IOException {
    try {
      segment.set(JAVA_BYTE, position, (byte) value);
      position++;
    } catch (IllegalStateException e) {
      throw closedException();
    }
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos > size) {
      throw corruptionException();
    }
    this.position = pos;
  }

  @Override
  public int readUnsignedByte() throws IOException {
    try {
      byte b = segment.get(JAVA_BYTE, position);
      position++;
      return ((int) b) & 0xFF;
    } catch (IllegalStateException e) {
      throw closedException();
    }
  }

  @Override
  public int readLittleEndianInt() throws IOException {
    try {
      int value = segment.get(JAVA_INT_LE, position);
      position += 4;
      return value;
    } catch (IllegalStateException e) {
      throw closedException();
    }
  }

  @Override
  public long readLittleEndianLong() throws IOException {
    try {
      long value = segment.get(JAVA_LONG_LE, position);
      position += 8;
      return value;
    } catch (IllegalStateException e) {
      throw closedException();
    }
  }

  private IOException corruptionException() {
    return new CorruptedIndexException("Index is likely corrupt (" + file.getPath() + "), referencing data outside of range");
  }

  private SparkeyReaderClosedException closedException() {
    return new SparkeyReaderClosedException("Reader has been closed");
  }

  @Override
  public String toString() {
    return "ReadWriteMemMapJ22{" +
            ", randomAccessFile=" + randomAccessFile +
            ", size=" + size +
            '}';
  }

}
