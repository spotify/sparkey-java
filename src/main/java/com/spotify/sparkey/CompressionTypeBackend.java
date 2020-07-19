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

import java.io.FileDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

interface CompressionTypeBackend {
    BlockOutput createBlockOutput(FileDescriptor fd, OutputStream outputStream, int maxBlockSize, int maxEntriesPerBlock) throws IOException;
    BlockPositionedInputStream createBlockInput(InputStream inputStream, int maxBlockSize, long start);
    BlockRandomInput createRandomAccessData(ReadOnlyMemMap data, int maxBlockSize);
}

class CompressionTypeBackendUncompressed implements CompressionTypeBackend {
    @Override
    public BlockPositionedInputStream createBlockInput(InputStream inputStream, int maxBlockSize, long start) {
        return new UncompressedBlockPositionedInputStream(inputStream, start);
    }

    @Override
    public BlockRandomInput createRandomAccessData(ReadOnlyMemMap data, int maxBlockSize) {
        return new UncompressedBlockRandomInput(data);
    }

    @Override
    public BlockOutput createBlockOutput(FileDescriptor fd, OutputStream outputStream, int maxBlockSize, int maxEntriesPerBlock) throws IOException {
        return new UncompressedBlockOutput(outputStream, fd);
    }
}

class CompressionTypeBackendCompressed implements CompressionTypeBackend {
    private final CompressorType compressor;

    public CompressionTypeBackendCompressed(CompressorType compressor) {
        this.compressor = compressor;
    }

    @Override
    public BlockPositionedInputStream createBlockInput(InputStream inputStream, int maxBlockSize, long start) {
        return new CompressedReader(compressor, inputStream, maxBlockSize, start);
    }

    @Override
    public BlockRandomInput createRandomAccessData(ReadOnlyMemMap data, int maxBlockSize) {
        return new CompressedRandomReader(compressor, new UncompressedBlockRandomInput(data), maxBlockSize);
    }

    @Override
    public BlockOutput createBlockOutput(FileDescriptor fd, OutputStream outputStream, int maxBlockSize, int maxEntriesPerBlock) throws IOException {
        return new CompressedWriter(new CompressedOutputStream(CompressorType.SNAPPY, maxBlockSize, outputStream, fd), maxEntriesPerBlock);
    }
}

