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
import java.util.Random;

public final class LogHeader extends CommonHeader {
  private static final int MAGIC_NUMBER = 0x49b39c95;
  private static final int HEADER_SIZE = 84;
  private static final int MAJOR_VERSION = 1;
  private static final int MINOR_VERSION = 0;

  private long numDeletes;
  private long deleteSize;
  private long putSize;

  private final CompressionType compressionType;
  private final int compressionBlockSize;
  private int maxEntriesPerBlock;

  private LogHeader(int majorVersion, int minorVersion, int fileIdentifier, long numPuts, long numDeletes,
                    long dataEnd, long maxKeyLen, long maxValueLen, long deleteSize, CompressionType compressionType,
                    int compressionBlockSize, long putSize, int maxEntriesPerBlock) throws IOException {
    super(majorVersion, minorVersion, fileIdentifier, dataEnd, maxKeyLen, maxValueLen, numPuts);
    this.numDeletes = numDeletes;
    this.deleteSize = deleteSize;
    this.compressionType = compressionType;
    this.compressionBlockSize = compressionBlockSize;
    this.putSize = putSize;
    this.maxEntriesPerBlock = maxEntriesPerBlock;
  }

  LogHeader(CompressionType compressionType, int compressionBlockSize) throws IOException {
    this(MAJOR_VERSION, MINOR_VERSION, new Random().nextInt(), 0, 0, HEADER_SIZE, 0, 0, 0, compressionType, compressionBlockSize, 0,
            0);
  }

  static LogHeader read(File file) throws IOException {
    try (FileInputStream inputStream = new FileInputStream(file)) {
      int magicNumber = Util.readLittleEndianInt(inputStream);
      if (magicNumber != MAGIC_NUMBER) {
        throw new IOException("File is not a Sparkey log file: " + file);
      }
      int majorVersion = Util.readLittleEndianInt(inputStream);
      if (majorVersion != MAJOR_VERSION) {
        throw new IOException(String.format("Incompatible major version. Expected %d, but got %d: ", MAJOR_VERSION, majorVersion));
      }
      int minorVersion = Util.readLittleEndianInt(inputStream);
      if (minorVersion > MINOR_VERSION) {
        throw new IOException(String.format("Incompatible minor version. Can handle up to version %d, but got %d: ", MINOR_VERSION, minorVersion));
      }
      int fileIdentifier = Util.readLittleEndianInt(inputStream);
      long numPuts = Util.readLittleEndianLong(inputStream);
      long numDeletes = Util.readLittleEndianLong(inputStream);
      long dataEnd = Util.readLittleEndianLong(inputStream);
      long maxKeyLen = Util.readLittleEndianLong(inputStream);
      long maxValueLen = Util.readLittleEndianLong(inputStream);
      long deleteSize = Util.readLittleEndianLong(inputStream);
      int compressionType = Util.readLittleEndianInt(inputStream);
      int compressionBlockSize = Util.readLittleEndianInt(inputStream);
      long putSize = Util.readLittleEndianLong(inputStream);
      int maxEntriesPerBlock = Util.readLittleEndianInt(inputStream);

      if (dataEnd > file.length()) {
        throw new IOException("Corrupt log file '" + file.toString() + "': expected at least " + dataEnd + " size but was " + file.length());
      }

      return new LogHeader(majorVersion, minorVersion, fileIdentifier, numPuts, numDeletes, dataEnd, maxKeyLen, maxValueLen, deleteSize, CompressionType.values()[compressionType], compressionBlockSize, putSize,
              maxEntriesPerBlock);
    }
  }

  void write(File file, boolean fsync) throws IOException {
    try (RandomAccessFile rw = new RandomAccessFile(file, "rw")) {
      rw.seek(0);
      Util.writeLittleEndianInt(MAGIC_NUMBER, rw);
      Util.writeLittleEndianInt(majorVersion, rw);
      Util.writeLittleEndianInt(minorVersion, rw);
      Util.writeLittleEndianInt(fileIdentifier, rw);
      Util.writeLittleEndianLong(numPuts, rw);
      Util.writeLittleEndianLong(numDeletes, rw);
      Util.writeLittleEndianLong(dataEnd, rw);
      Util.writeLittleEndianLong(maxKeyLen, rw);
      Util.writeLittleEndianLong(maxValueLen, rw);
      Util.writeLittleEndianLong(deleteSize, rw);
      Util.writeLittleEndianInt(compressionType.ordinal(), rw);
      Util.writeLittleEndianInt(compressionBlockSize, rw);
      Util.writeLittleEndianLong(putSize, rw);
      Util.writeLittleEndianInt(maxEntriesPerBlock, rw);

      if (rw.getFilePointer() != HEADER_SIZE) {
        throw new RuntimeException("Programming error! Header size was incorrect, expected " + HEADER_SIZE + " but was " + rw.getFilePointer());
      }
      if (fsync) {
        rw.getFD().sync();
      }
    }
  }

  public int getMajorVersion() {
    return majorVersion;
  }

  public int getMinorVersion() {
    return minorVersion;
  }

  public int getFileIdentifier() {
    return fileIdentifier;
  }

  public long getNumPuts() {
    return numPuts;
  }

  public long getNumDeletes() {
    return numDeletes;
  }

  public int getCompressionBlockSize() {
    return compressionBlockSize;
  }

  public CompressionType getCompressionType() {
    return compressionType;
  }

  CompressionTypeBackend getCompressionTypeBackend() {
    return getCompressionType().getBackend();
  }

  public long getDataEnd() {
    return dataEnd;
  }

  public long getMaxKeyLen() {
    return maxKeyLen;
  }

  public long getMaxValueLen() {
    return maxValueLen;
  }

  void put(int keyLen, long valueLen) {
    numPuts++;
    maxKeyLen = Math.max(maxKeyLen, keyLen);
    maxValueLen = Math.max(maxValueLen, valueLen);
    putSize += Util.unsignedVLQSize(keyLen + 1) + Util.unsignedVLQSize(valueLen) + keyLen + valueLen;
  }

  void delete(int keyLen) {
    numDeletes++;
    int size = 1 + Util.unsignedVLQSize(keyLen) + keyLen;
    deleteSize += size;
  }

  public long size() {
    return HEADER_SIZE;
  }

  void setDataEnd(long dataEnd) {
    this.dataEnd = dataEnd;
  }

  @Override
  public String toString() {
    return "LogHeader{" +
            "\nmajorVersion=" + majorVersion +
            ",\n minorVersion=" + minorVersion +
            ",\n fileIdentifier=" + fileIdentifier +
            ",\n numPuts=" + numPuts +
            ",\n numDeletes=" + numDeletes +
            ",\n dataEnd=" + dataEnd +
            ",\n maxKeyLen=" + maxKeyLen +
            ",\n maxValueLen=" + maxValueLen +
            ",\n deleteSize=" + deleteSize +
            ",\n putSize=" + putSize +
            ",\n compressionType=" + compressionType +
            ",\n compressionBlockSize=" + compressionBlockSize +
            ",\n maxEntriesPerBlock=" + maxEntriesPerBlock +
            '}';
  }

  public long getPutSize() {
    return putSize;
  }

  public long getDeleteSize() {
    return deleteSize;
  }

  public int getMaxEntriesPerBlock() {
    return maxEntriesPerBlock;
  }

  void setMaxEntriesPerBlock(int maxEntriesPerBlock) {
    this.maxEntriesPerBlock = maxEntriesPerBlock;
  }
}
