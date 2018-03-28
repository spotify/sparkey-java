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

import java.io.DataOutputStream;
import java.io.IOException;

enum AddressSize {
  LONG() {
    @Override
    long readAddress(RandomAccessData data) throws IOException {
      return data.readLittleEndianLong();
    }

    @Override
    void writeAddress(long address, ReadWriteData data) throws IOException {
      data.writeLittleEndianLong(address);
    }

    @Override
    void writeAddress(final long address, final DataOutputStream data) throws IOException {
      data.writeLong(address);
    }
  },
  INT() {
    @Override
    long readAddress(RandomAccessData data) throws IOException {
      return data.readLittleEndianInt() & INT_MASK;
    }

    @Override
    void writeAddress(long address, ReadWriteData data) throws IOException {
      data.writeLittleEndianInt((int) address);
    }

    @Override
    void writeAddress(final long address, final DataOutputStream data) throws IOException {
      data.writeInt((int) address); // TODO: overflow?
    }
  };

  private static final long INT_MASK = (1L << 32) - 1;

  abstract long readAddress(RandomAccessData data) throws IOException;

  abstract void writeAddress(long address, ReadWriteData data) throws IOException;
  abstract void writeAddress(long address, DataOutputStream data) throws IOException;
}
