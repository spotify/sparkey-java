package com.spotify.sparkey;

import org.junit.Test;
import org.xerial.snappy.Snappy;

import java.io.*;

import static org.junit.Assert.assertEquals;

/**
 * Tests SnappyReader
 */
public class SnappyReaderTest {
    // A stream that reads the same array repeatedly, forever.
    private class RepeatingInputStream extends InputStream {
        private byte[] buffer;
        private int pos = 0;

        public RepeatingInputStream(byte[] buf) throws IOException {
            buffer = buf;
        }

        public int read() throws IOException {
            int ret = buffer[pos];
            skip(1);
            return ret;
        }

        public int read(byte[] b, int off, int len) throws IOException {
            int remain = len;
            while (remain > 0) {
                int avail = buffer.length - pos;
                int copy = Math.min(avail, remain);
                System.arraycopy(buffer, pos, b, off, copy);
                skip(copy);
                off += copy;
                remain -= copy;
            }
            return len;
        }

        public long skip(long n) throws IOException {
            pos = (int)((n + pos) % buffer.length);
            return n;
        }
    }

    private SnappyReader reader() throws IOException {
        byte[] uncompressed = new byte[10];
        for (int i = 0; i < uncompressed.length; ++i) {
            uncompressed[i] = (byte)i;
        }

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        byte[] compressed = Snappy.compress(uncompressed);
        Util.writeUnsignedVLQ(compressed.length, bytes);
        bytes.write(compressed);

        InputStream buf = new RepeatingInputStream(bytes.toByteArray());
        return new SnappyReader(buf, uncompressed.length, 0);
    }

    @Test
    public void testLargeSkip() throws IOException {
        long ret = reader().skip(1000 * 1000);
        assertEquals(1000 * 1000, ret);
    }

    @Test
    public void testLargeRead() throws IOException {
        byte[] buf = new byte[1000 * 1000];
        int ret = reader().read(buf);
        assertEquals(1000 * 1000, ret);
    }
}
