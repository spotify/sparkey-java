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
import java.io.OutputStream;

public enum CompressionType {
  NONE {
    @Override
    BlockPositionedInputStream createBlockInput(InputStream inputStream, int maxBlockSize, long start) {
      return new UncompressedBlockPositionedInputStream(inputStream, start);
    }

    @Override
    BlockRandomInput createRandomAccessData(ReadOnlyMemMap data, int maxBlockSize) {
      return new UncompressedBlockRandomInput(data);
    }

    @Override
    BlockOutput createBlockOutput(OutputStream outputStream, int maxBlockSize, int maxEntriesPerBlock) throws IOException {
      return new UncompressedBlockOutput(outputStream);
    }

  },

  SNAPPY {
    @Override
    BlockPositionedInputStream createBlockInput(InputStream inputStream, int maxBlockSize, long start) {
      return new SnappyReader(inputStream, maxBlockSize, start);
    }

    @Override
    BlockRandomInput createRandomAccessData(ReadOnlyMemMap data, int maxBlockSize) {
      return new SnappyRandomReader(new UncompressedBlockRandomInput(data), maxBlockSize);
    }

    @Override
    BlockOutput createBlockOutput(OutputStream outputStream, int maxBlockSize, int maxEntriesPerBlock) throws IOException {
      return new SnappyWriter(new SnappyOutputStream(maxBlockSize, outputStream), maxEntriesPerBlock);
    }
  },;

  abstract BlockOutput createBlockOutput(OutputStream outputStream, int maxBlockSize, int maxEntriesPerBlock) throws IOException;

  abstract BlockPositionedInputStream createBlockInput(InputStream inputStream, int maxBlockSize, long start);

  abstract BlockRandomInput createRandomAccessData(ReadOnlyMemMap data, int maxBlockSize);
}
