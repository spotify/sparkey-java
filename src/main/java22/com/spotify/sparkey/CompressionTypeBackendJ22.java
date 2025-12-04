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

/**
 * Java 22+ version of CompressionTypeBackend that works with J22 types.
 */
interface CompressionTypeBackendJ22 {
    BlockRandomInput createRandomAccessData(ReadOnlyMemMapJ22 data, int maxBlockSize);
}

class CompressionTypeBackendJ22Uncompressed implements CompressionTypeBackendJ22 {
    @Override
    public BlockRandomInput createRandomAccessData(ReadOnlyMemMapJ22 data, int maxBlockSize) {
        return new UncompressedBlockRandomInputJ22(data);
    }
}

class CompressionTypeBackendJ22Compressed implements CompressionTypeBackendJ22 {
    private final CompressorType compressor;

    public CompressionTypeBackendJ22Compressed(CompressorType compressor) {
        this.compressor = compressor;
    }

    @Override
    public BlockRandomInput createRandomAccessData(ReadOnlyMemMapJ22 data, int maxBlockSize) {
        return new CompressedRandomReader(compressor, new UncompressedBlockRandomInputJ22(data), maxBlockSize);
    }
}
