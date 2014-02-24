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

import com.google.common.io.CountingOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

final class UncompressedBlockOutput implements BlockOutput {
  private final byte[] buf = new byte[1024*1024];
  private final CountingOutputStream outputStream;

  UncompressedBlockOutput(OutputStream outputStream) {
    this.outputStream = new CountingOutputStream(outputStream);
  }

  @Override
  public long put(byte[] key, int keyLen, byte[] value, int valueLen) throws IOException {
    long pre = outputStream.getCount();
    Util.writeUnsignedVLQ(keyLen + 1, outputStream);
    Util.writeUnsignedVLQ(valueLen, outputStream);
    outputStream.write(key, 0, keyLen);
    outputStream.write(value, 0, valueLen);
    return outputStream.getCount() - pre;
  }

  @Override
  public long put(byte[] key, int keyLen, InputStream value, long valueLen) throws IOException {
    long pre = outputStream.getCount();
    Util.writeUnsignedVLQ(keyLen + 1, outputStream);
    Util.writeUnsignedVLQ(valueLen, outputStream);
    outputStream.write(key, 0, keyLen);
    Util.copy(valueLen, value, outputStream, buf);
    return outputStream.getCount() - pre;
  }

  @Override
  public long delete(byte[] key, int keyLen) throws IOException {
    long pre = outputStream.getCount();
    outputStream.write(0);
    Util.writeUnsignedVLQ(keyLen, outputStream);
    outputStream.write(key, 0, keyLen);
    return outputStream.getCount() - pre;
  }

  @Override
  public void flush() throws IOException {
    outputStream.flush();
  }

  @Override
  public void close() throws IOException {
    outputStream.close();
  }

  @Override
  public int getMaxEntriesPerBlock() {
    return 1;
  }
}
