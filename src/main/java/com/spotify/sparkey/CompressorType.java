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

import com.github.luben.zstd.Zstd;

import org.xerial.snappy.Snappy;

public enum CompressorType {
  SNAPPY {
    @Override
    int maxCompressedLength(int blockSize) {
      return Snappy.maxCompressedLength(blockSize);
    }

    @Override
    int uncompress(byte[] compressed, int compressedSize, byte[] uncompressed) throws IOException {
      return Snappy.uncompress(compressed, 0, compressedSize, uncompressed, 0);
    }
  
    @Override
    int compress(byte[] uncompressed, int uncompressedSize, byte[] compressed) throws IOException {
      return Snappy.compress(uncompressed, 0, uncompressedSize, compressed, 0);
    }
  },

  ZSTD {
    @Override
    int maxCompressedLength(int blockSize) {
      return (int)Zstd.compressBound(blockSize);
    }

    @Override
    int uncompress(byte[] compressed, int compressedSize, byte[] uncompressed) throws IOException {
      return (int)Zstd.decompressByteArray(uncompressed, 0, uncompressed.length, compressed, 0, compressedSize);
    }
  
    @Override
    int compress(byte[] uncompressed, int uncompressedSize, byte[] compressed) throws IOException {
      return (int)Zstd.compressByteArray(compressed, 0, compressed.length, uncompressed, 0, uncompressedSize, 3);
    }
  },;

  abstract int maxCompressedLength(int blockSize);

  abstract int uncompress(byte[] compressed, int compressedSize, byte[] uncompressed) throws IOException;

  abstract int compress(byte[] uncompressed, int uncompressedSize, byte[] compressed) throws IOException;
}