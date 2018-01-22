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

import java.io.FileDescriptor;
import java.io.IOException;
import java.io.OutputStream;
import java.io.SyncFailedException;

final class SnappyOutputStream extends OutputStream {
  private final int maxBlockSize;
  private final OutputStream output;

  private final byte[] uncompressedBuffer;
  private final byte[] compressedBuffer;
  private final FileDescriptor fileDescriptor;
  private int pending;
  private SnappyWriter listener = SnappyWriter.DUMMY;

  SnappyOutputStream(int maxBlockSize, OutputStream output, FileDescriptor fileDescriptor) throws IOException {
    this.fileDescriptor = fileDescriptor;
    if (maxBlockSize < 10) {
      throw new IOException("Too small block size - won't be able to fit keylen + valuelen in a single block");
    }
    this.maxBlockSize = maxBlockSize;
    this.output = output;
    uncompressedBuffer = new byte[maxBlockSize];
    compressedBuffer = new byte[Snappy.maxCompressedLength(maxBlockSize)];
  }

  @Override
  public void flush() throws IOException {
    if (pending == 0) {
      return;
    }

    int compressedSize = Snappy.compress(uncompressedBuffer, 0, pending, compressedBuffer, 0);
    Util.writeUnsignedVLQ(compressedSize, output);
    output.write(compressedBuffer, 0, compressedSize);
    output.flush();
    pending = 0;
    listener.afterFlush();
  }

  public void fsync() throws SyncFailedException {
    fileDescriptor.sync();
  }

  @Override
  public void close() throws IOException {
    flush();
    output.close();
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    while (true) {
      int remaining = remaining();
      if (len < remaining) {
        System.arraycopy(b, off, uncompressedBuffer, pending, len);
        pending += len;
        return;
      } else {
        System.arraycopy(b, off, uncompressedBuffer, pending, remaining);
        pending = maxBlockSize;
        flush();
        off += remaining;
        len -= remaining;
      }
    }
  }

  @Override
  public void write(int b) throws IOException {
    uncompressedBuffer[pending++] = (byte) b;
    if (pending == maxBlockSize) {
      flush();
    }
  }

  int getPending() {
    return pending;
  }

  int remaining() {
    return maxBlockSize - pending;
  }

  void setListener(SnappyWriter snappyWriter) {
    listener = snappyWriter;
  }

  int getMaxBlockSize() {
    return maxBlockSize;
  }
}
