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
import java.io.InputStream;

final class CompressedReader extends BlockPositionedInputStream {
  private final CompressorType compressor;
  private final byte[] uncompressedBuf;
  private final byte[] compressedBuf;
  private int bufPos;
  private int blockSize;

  private long curBlockStart;
  private long nextBlockStart;

  public CompressedReader(CompressorType compressor, InputStream data, int maxBlockSize, long start) {
    super(data);
    this.compressor = compressor;
    blockSize = 0;
    bufPos = 0;
    curBlockStart = start;
    nextBlockStart = start;
    uncompressedBuf = new byte[maxBlockSize];
    compressedBuf = new byte[compressor.maxCompressedLength(maxBlockSize)];
  }

  @Override
  public int read() throws IOException {
    if (bufPos == blockSize) {
      fetchBlock();
    }
    return ((int) uncompressedBuf[bufPos++]) & 0xFF;
  }

  private void fetchBlock() throws IOException {
    int compressedSize = Util.readUnsignedVLQInt(input);
    input.read(compressedBuf, 0, compressedSize);
    int uncompressedSize = compressor.uncompress(compressedBuf, compressedSize, uncompressedBuf);
    bufPos = 0;
    blockSize = uncompressedSize;

    curBlockStart = nextBlockStart;
    nextBlockStart = curBlockStart + Util.unsignedVLQSize(compressedSize) + compressedSize;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int remain = len;
    while (remain > 0) {
      int didRead = readImpl(b, off, remain);
      off += didRead;
      remain -= didRead;
    }
    return len;
  }

  private int readImpl(byte[] b, int off, int len) throws IOException {
    int available = available();
    if (len <= available) {
      System.arraycopy(uncompressedBuf, bufPos, b, off, len);
      bufPos += len;
      return len;
    } else {
      System.arraycopy(uncompressedBuf, bufPos, b, off, available);
      bufPos = blockSize;
      fetchBlock();
      return available;
    }
  }

  @Override
  public long skip(long n) throws IOException {
    long remain = n;
    while (remain > 0) {
      remain -= skipImpl(remain);
    }
    return n;
  }

  private long skipImpl(long n) throws IOException {
    int available = available();
    if (n <= available) {
      bufPos += n;
      return n;
    } else {
      bufPos = blockSize;
      fetchBlock();
      return available;
    }
  }

  @Override
  long getBlockPosition() {
    if (bufPos == blockSize) {
      return nextBlockStart;
    }
    return curBlockStart;
  }

  @Override
  public int available() throws IOException {
    return blockSize - bufPos;
  }
}
