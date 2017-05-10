package com.spotify.sparkey.cleaner;

import java.nio.MappedByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class SingleThreadedCleanerWithWait implements MappedByteBufferCleaner {
    private static final ScheduledExecutorService CLEANER = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName(SingleThreadedCleanerWithWait.class.getSimpleName() + "-cleaner");
            thread.setDaemon(true);
            return thread;
        }
    });

    @Override
    public void cleanup(final MappedByteBuffer... chunks) {
        // Wait a bit with closing so that all threads have a chance to see the that
        // chunks and curChunks are null
        CLEANER.schedule(new Runnable() {
            @Override
            public void run() {
                for (MappedByteBuffer chunk : chunks) {
                    ByteBufferCleaner.cleanMapping(chunk);
                }
            }
        }, 1000, TimeUnit.MILLISECONDS);
    }
}
