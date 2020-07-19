package com.spotify.sparkey;

import org.junit.Test;

import java.io.*;

/**
 * Tests CompressedOutputStream
 */
public class CompressedOutputStreamTest {
    @Test
    public void testLargeWrite() throws IOException {
        for (CompressorType compressor : CompressorType.values()) {
            File testFile = File.createTempFile("sparkey-test", "");
            testFile.deleteOnExit();
            FileOutputStream fos = new FileOutputStream(testFile);

            byte[] buf = new byte[1000 * 1000];
            CompressedOutputStream os = new CompressedOutputStream(compressor, 10, fos, fos.getFD());
            os.write(buf);

            testFile.delete();
        }
    }
}
