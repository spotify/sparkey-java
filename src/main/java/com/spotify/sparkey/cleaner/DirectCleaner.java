package com.spotify.sparkey.cleaner;

import java.nio.MappedByteBuffer;

public class DirectCleaner implements MappedByteBufferCleaner {
    @Override
    public void cleanup(MappedByteBuffer... chunks) {
        for (MappedByteBuffer chunk : chunks) {
            ByteBufferCleaner.cleanMapping(chunk);
        }
    }
}
