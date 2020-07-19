package com.spotify.sparkey;

import org.junit.Test;

import java.io.*;

import static org.junit.Assert.assertEquals;

/**
 * Tests CompressedReader
 */
public class CompressedReaderTest {
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

    private CompressedReader reader(CompressorType compressor) throws IOException {
        byte[] uncompressed = new byte[10];
        for (int i = 0; i < uncompressed.length; ++i) {
            uncompressed[i] = (byte)i;
        }

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        byte[] compressed = new byte[compressor.maxCompressedLength(uncompressed.length)];
        int length = compressor.compress(uncompressed, uncompressed.length, compressed);
        Util.writeUnsignedVLQ(length, bytes);
        bytes.write(compressed, 0, length);

        InputStream buf = new RepeatingInputStream(bytes.toByteArray());
        return new CompressedReader(compressor, buf, uncompressed.length, 0);
    }

    @Test
    public void testLargeSkip() throws IOException {
        for (CompressorType compressor : CompressorType.values()) {
            long ret = reader(compressor).skip(1000 * 1000);
            assertEquals(1000 * 1000, ret);
        }
    }

    @Test
    public void testLargeRead() throws IOException {
        for (CompressorType compressor : CompressorType.values()) {
            byte[] buf = new byte[1000 * 1000];
            int ret = reader(compressor).read(buf);
            assertEquals(1000 * 1000, ret);
        }
    }
}
