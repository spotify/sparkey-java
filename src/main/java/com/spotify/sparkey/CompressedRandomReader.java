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

final class CompressedRandomReader implements BlockRandomInput {
  private final CompressorType compressor;

  private long position;

  private final BlockRandomInput data;
  private final int maxBlockSize;

  private final byte[] uncompressedBuf;
  private final byte[] compressedBuf;
  private int bufPos;
  private int blockSize;

  CompressedRandomReader(CompressorType compressor, BlockRandomInput data, int maxBlockSize) {
    this.compressor = compressor;
    this.data = data;
    this.maxBlockSize = maxBlockSize;
    blockSize = 0;
    bufPos = 0;
    uncompressedBuf = new byte[maxBlockSize];
    compressedBuf = new byte[compressor.maxCompressedLength(maxBlockSize)];
  }

  @Override
  public int readUnsignedByte() throws IOException {
    return ((int) readSignedByte()) & 0xFF;
  }

  private byte readSignedByte() throws IOException {
    if (bufPos >= blockSize) {
      fetchBlock();
    }
    return uncompressedBuf[bufPos++];
  }

  private void fetchBlock() throws IOException {
    int compressedSize = Util.readUnsignedVLQInt(data);
    data.readFully(compressedBuf, 0, compressedSize);
    bufPos = 0;
    blockSize = compressor.uncompress(compressedBuf, compressedSize, uncompressedBuf);
    position = -1;
  }

  @Override
  public void readFully(byte[] b, int off, int len) throws IOException {
    int remaining = blockSize - bufPos;
    if (remaining >= len) {
      System.arraycopy(uncompressedBuf, bufPos, b, off, len);
      bufPos += len;
    } else {
      System.arraycopy(uncompressedBuf, bufPos, b, off, remaining);
      fetchBlock();
      readFully(b, off + remaining, len - remaining);
    }
  }

  @Override
  public void close() {
    data.close();
  }

  /**
   * It's only valid to seek to known block starts.
   *
   * @param position
   */
  @Override
  public void seek(long position) throws IOException {
    if (position != this.position) {
      this.position = position;
      blockSize = 0;
      data.seek(position);
    }
    bufPos = 0;
  }

  @Override
  public void skipBytes(long n) throws IOException {
    int remaining = blockSize - bufPos;
    if (n < remaining) {
      bufPos += n;
    } else {
      fetchBlock();
      skipBytes(n - remaining);
    }
  }

  @Override
  public BlockRandomInput duplicate() {
    CompressedRandomReader duplicate = new CompressedRandomReader(compressor, data.duplicate(), maxBlockSize);
    duplicate.bufPos = this.bufPos;
    duplicate.blockSize = this.blockSize;
    duplicate.position = this.position;
    System.arraycopy(this.uncompressedBuf, 0, duplicate.uncompressedBuf, 0, this.blockSize);
    return duplicate;
  }

  @Override
  public void closeDuplicate() {
    data.closeDuplicate();
  }

  @Override
  public long getLoadedBytes() {
    return data.getLoadedBytes();
  }
}
