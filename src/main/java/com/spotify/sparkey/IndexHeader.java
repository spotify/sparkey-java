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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class IndexHeader extends CommonHeader {
  private static final int MAGIC_NUMBER = 0x9a11318f;
  static final int HEADER_SIZE = 112;
  private static final int MAJOR_VERSION = 1;
  private static final int MINOR_VERSION = 1;

  private final int hashSeed;

  private long garbageSize;
  private long numEntries;
  private final int addressSize;
  private final int hashSize;
  private final long hashCapacity;
  private long maxDisplacement;
  private final int entryBlockBits;
  private long hashCollisions;
  private long totalDisplacement;

  private final HashType hashType;
  private final AddressSize addressData;

  private IndexHeader(int majorVersion, int minorVersion, int fileIdentifier, int hashSeed, long dataEnd,
                      long maxKeyLen, long maxValueLen, long garbageSize, long numEntries, int addressSize,
                      int hashSize, long hashCapacity, long maxDisplacement, long numPuts, int entryBlockBits, long hashCollisions, long totalDisplacement) throws IOException {
    super(majorVersion, minorVersion, fileIdentifier, dataEnd, maxKeyLen, maxValueLen, numPuts);
    this.hashSeed = hashSeed;
    this.garbageSize = garbageSize;
    this.numEntries = numEntries;
    this.addressSize = addressSize;
    this.hashSize = hashSize;
    this.hashCapacity = hashCapacity;
    this.maxDisplacement = maxDisplacement;
    this.entryBlockBits = entryBlockBits;
    this.hashCollisions = hashCollisions;
    this.totalDisplacement = totalDisplacement;

    this.hashType = getHashType(hashSize);
    this.addressData = getAddressData(addressSize);
  }

  IndexHeader(int fileIdentifier, long dataEnd, long maxKeyLen, long maxValueLen, int addressSize, int hashSize,
              long capacity, long numPuts, int hashSeed, int entryBlockBits) throws IOException {
    this(MAJOR_VERSION, MINOR_VERSION, fileIdentifier, hashSeed, dataEnd, maxKeyLen, maxValueLen, 0, 0, addressSize, hashSize, capacity, 0,
            numPuts, entryBlockBits, 0, 0);
  }

  static IndexHeader read(File file) throws IOException {
    try (FileInputStream inputStream = new FileInputStream(file)) {
      int magicNumber = Util.readLittleEndianInt(inputStream);
      if (magicNumber != MAGIC_NUMBER) {
        throw new IOException("File is not a Sparkey index file: " + file);
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
      int hashSeed = Util.readLittleEndianInt(inputStream);

      long dataEnd = Util.readLittleEndianLong(inputStream);
      long maxKeyLen = Util.readLittleEndianLong(inputStream);
      long maxValueLen = Util.readLittleEndianLong(inputStream);
      long numPuts = Util.readLittleEndianLong(inputStream);

      long garbageSize = Util.readLittleEndianLong(inputStream);
      long numEntries = Util.readLittleEndianLong(inputStream);
      int addressSize = Util.readLittleEndianInt(inputStream);
      int hashSize = Util.readLittleEndianInt(inputStream);
      long hashCapacity = Util.readLittleEndianLong(inputStream);
      long maxDisplacement = Util.readLittleEndianLong(inputStream);
      int entryBlockBits = Util.readLittleEndianInt(inputStream);
      long hashCollisions = Util.readLittleEndianLong(inputStream);
      long totalDisplacement = Util.readLittleEndianLong(inputStream);

      return new IndexHeader(majorVersion, minorVersion, fileIdentifier, hashSeed, dataEnd, maxKeyLen, maxValueLen, garbageSize, numEntries, addressSize, hashSize, hashCapacity, maxDisplacement,
              numPuts, entryBlockBits, hashCollisions, totalDisplacement);
    }
  }

  private static AddressSize getAddressData(int size) {
    if (size == 4) {
      return AddressSize.INT;
    } else if (size == 8) {
      return AddressSize.LONG;
    } else {
      throw new IllegalArgumentException("Can't support address size " + size);
    }
  }

  private static HashType getHashType(int size) {
    if (size == 4) {
      return HashType.HASH_32_BITS;
    } else if (size == 8) {
      return HashType.HASH_64_BITS;
    } else {
      throw new IllegalArgumentException("Can't support hash size " + size);
    }
  }

  byte[] asBytes() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(HEADER_SIZE);
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
    byteBuffer.putInt(MAGIC_NUMBER); // 0
    byteBuffer.putInt(majorVersion); // 4
    byteBuffer.putInt(minorVersion); // 8

    byteBuffer.putInt(fileIdentifier); // 12
    byteBuffer.putInt(hashSeed); // 16

    byteBuffer.putLong(dataEnd); // 20
    byteBuffer.putLong(maxKeyLen); // 28
    byteBuffer.putLong(maxValueLen); // 36
    byteBuffer.putLong(numPuts); // 44

    byteBuffer.putLong(garbageSize); // 52
    byteBuffer.putLong(numEntries); // 60
    byteBuffer.putInt(addressSize); // 68
    byteBuffer.putInt(hashSize); // 72
    byteBuffer.putLong(hashCapacity); // 76
    byteBuffer.putLong(maxDisplacement); // 84
    byteBuffer.putInt(entryBlockBits); // 92
    byteBuffer.putLong(hashCollisions); // 96
    byteBuffer.putLong(totalDisplacement); // 104
    // End at 112

    if (byteBuffer.position() != HEADER_SIZE) {
      throw new RuntimeException("Programming error! Header size was incorrect, expected " + HEADER_SIZE + " but was " + byteBuffer.position());
    }
    return byteBuffer.array();
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

  public long getDataEnd() {
    return dataEnd;
  }

  public long getMaxKeyLen() {
    return maxKeyLen;
  }

  public long getMaxValueLen() {
    return maxValueLen;
  }

  public long getGarbageSize() {
    return garbageSize;
  }

  public long getNumEntries() {
    return numEntries;
  }

  public int getAddressSize() {
    return addressSize;
  }

  public int getHashSize() {
    return hashSize;
  }

  public long getHashCapacity() {
    return hashCapacity;
  }

  public long getMaxDisplacement() {
    return maxDisplacement;
  }

  public int getHashSeed() {
    return hashSeed;
  }

  public int size() {
    return HEADER_SIZE;
  }

  public int getSlotSize() {
    return hashSize + addressSize;
  }

  void addedEntry() {
    numEntries++;
  }

  void replacedEntry(int keyLen2, int valueLen2) {
    garbageSize += keyLen2 + valueLen2 + Util.unsignedVLQSize(keyLen2 + 1) + Util.unsignedVLQSize(valueLen2);
  }

  void deletedEntry(int keyLen2, int valueLen2) {
    garbageSize += keyLen2 + valueLen2 + Util.unsignedVLQSize(keyLen2 + 1) + Util.unsignedVLQSize(valueLen2);
    numEntries--;
  }

  public long getHashLength() {
    return getSlotSize() * hashCapacity;
  }

  public long getNumPuts() {
    return numPuts;
  }

  @Override
  public String toString() {
    return "IndexHeader{" +
            "\n majorVersion=" + majorVersion +
            ",\n minorVersion=" + minorVersion +
            ",\n fileIdentifier=" + fileIdentifier +
            ",\n hashSeed=" + hashSeed +
            ",\n dataEnd=" + dataEnd +
            ",\n maxKeyLen=" + maxKeyLen +
            ",\n maxValueLen=" + maxValueLen +
            ",\n garbageSize=" + garbageSize +
            ",\n numEntries=" + numEntries +
            ",\n addressSize=" + addressSize +
            ",\n hashSize=" + hashType +
            ",\n hashCapacity=" + hashCapacity +
            ",\n maxDisplacement=" + maxDisplacement +
            ",\n numPuts=" + numPuts +
            ",\n entryBlockBits=" + entryBlockBits +
            ",\n hashCollisions=" + hashCollisions +
            ",\n totalDisplacement=" + totalDisplacement +
            '}';
  }

  public int getEntryBlockBits() {
    return entryBlockBits;
  }

  int getEntryBlockBitsBitmask() {
    return (1 << entryBlockBits) - 1;
  }

  void setMaxDisplacement(long maxDisplacement) {
    this.maxDisplacement = maxDisplacement;
  }

  void setHashCollisions(long numHashCollisions) {
    hashCollisions = numHashCollisions;
  }

  void setTotalDisplacement(long totalDisplacement) {
    this.totalDisplacement = totalDisplacement;
  }

  public long getHashCollisions() {
    return hashCollisions;
  }

  public long getTotalDisplacement() {
    return totalDisplacement;
  }

  public HashType getHashType() {
    return hashType;
  }

  public AddressSize getAddressData() {
    return addressData;
  }

  @Override
  public IndexHeader clone() {
    try {
      return new IndexHeader(
          majorVersion, minorVersion, fileIdentifier, hashSeed, dataEnd, maxKeyLen, maxValueLen, garbageSize, numEntries,
          addressSize, hashSize, hashCapacity, maxDisplacement, numPuts, entryBlockBits, hashCollisions, totalDisplacement);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
