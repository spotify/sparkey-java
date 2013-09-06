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
  public void seek(long pos) {
    data.seek(pos);
  }

  @Override
  public int readUnsignedByte() {
    return data.readUnsignedByte();
  }

  @Override
  public void readFully(byte[] buffer, int offset, int length) {
    data.readFully(buffer, offset, length);
  }

  @Override
  public void skipBytes(long amount) {
    data.skipBytes(amount);
  }

  @Override
  public BlockRandomInput duplicate() {
    return new UncompressedBlockRandomInput(data.duplicate());
  }
}
