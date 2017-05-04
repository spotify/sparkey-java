package com.spotify.sparkey.cleaner;

import java.nio.MappedByteBuffer;

public interface MappedByteBufferCleaner {
    void cleanup(MappedByteBuffer... chunks);
}
