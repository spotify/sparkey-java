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

import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.io.InputStream;

final class SnappyReader extends BlockPositionedInputStream {
  private final InputStream data;

  private final byte[] uncompressedBuf;
  private final byte[] compressedBuf;
  private int bufPos;
  private int blockSize;

  private long curBlockStart;
  private long nextBlockStart;

  public SnappyReader(InputStream data, int maxBlockSize, long start) {
    this.data = data;
    blockSize = 0;
    bufPos = 0;
    curBlockStart = start;
    nextBlockStart = start;
    uncompressedBuf = new byte[maxBlockSize];
    compressedBuf = new byte[Snappy.maxCompressedLength(maxBlockSize)];
  }

  @Override
  public int read() throws IOException {
    if (bufPos == blockSize) {
      fetchBlock();
    }
    return ((int) uncompressedBuf[bufPos++]) & 0xFF;
  }

  private void fetchBlock() throws IOException {
    int compressedSize = Util.readUnsignedVLQInt(data);
    data.read(compressedBuf, 0, compressedSize);
    int uncompressedSize = Snappy.uncompress(compressedBuf, 0, compressedSize, uncompressedBuf, 0);
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
  public int read(byte[] b, int off, final int len) throws IOException {
    int remaining = len;
    while (true) {
      int available = available();
      if (len <= available) {
        System.arraycopy(uncompressedBuf, bufPos, b, off, remaining);
        bufPos += remaining;
        return len;
      } else {
        System.arraycopy(uncompressedBuf, bufPos, b, off, available);
        bufPos = blockSize;
        fetchBlock();
        off += available;
        remaining -= available;
      }
    }
  }

  @Override
  public long skip(final long n) throws IOException {
    long remaining = n;
    while (true) {
      int available = available();
      if (remaining <= available) {
        bufPos += remaining;
        return n;
      } else {
        bufPos = blockSize;
        fetchBlock();
        remaining -= available;
      }
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
