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

public enum HashType {
  HASH_64_BITS(8) {
    @Override
    long readHash(RandomAccessData data) throws IOException {
      return Util.readLittleEndianLong(data);
    }

    @Override
    void writeHash(long hash, InMemoryData data) throws IOException {
      Util.writeLittleEndianLong(hash, data);
    }

    @Override
    long hash(int keyLen, byte[] key, int seed) {
      return MurmurHash3.murmurHash3_x64_64(key, keyLen, seed);
    }
  },
  HASH_32_BITS(4) {
    @Override
    long readHash(RandomAccessData data) throws IOException {
      return Util.readLittleEndianInt(data) & INT_MASK;
    }

    @Override
    void writeHash(long hash, InMemoryData data) throws IOException {
      Util.writeLittleEndianInt((int) hash, data);
    }

    @Override
    long hash(int keyLen, byte[] key, int seed) {
      return MurmurHash3.murmurHash3_x86_32(key, keyLen, seed) & BITS_32;
    }
  };

  private static final long BITS_32 = ((1L << 32) - 1);
  private static final long INT_MASK = (1L << 32) - 1;
  private final int size;

  private HashType(int size) {
    this.size = size;
  }


  abstract long readHash(RandomAccessData data) throws IOException;

  abstract void writeHash(long hash, InMemoryData data) throws IOException;

  abstract long hash(int keyLen, byte[] key, int seed);

  public int size() {
    return size;
  }
}
