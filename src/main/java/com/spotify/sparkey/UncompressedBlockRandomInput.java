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

class UncompressedBlockRandomInput implements BlockRandomInput {
  private final ReadOnlyMemMap data;

  UncompressedBlockRandomInput(ReadOnlyMemMap data) {
    this.data = data;
  }

  @Override
  public void close() {
    data.close();
  }

  @Override
  public void seek(long pos) throws IOException {
    data.seek(pos);
  }

  @Override
  public int readUnsignedByte() throws IOException {
    return data.readUnsignedByte();
  }

  @Override
  public void readFully(byte[] buffer, int offset, int length) throws IOException {
    data.readFully(buffer, offset, length);
  }

  @Override
  public boolean readFullyCompare(int length, byte[] key) throws IOException {
    return data.readFullyCompare(length, key);
  }

  @Override
  public void skipBytes(long amount) throws IOException {
    data.skipBytes(amount);
  }

  @Override
  public UncompressedBlockRandomInput duplicate() {
    return new UncompressedBlockRandomInput(data.duplicate());
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
