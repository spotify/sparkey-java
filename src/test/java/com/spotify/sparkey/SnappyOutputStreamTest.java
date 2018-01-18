package com.spotify.sparkey;

import org.junit.Assert;
import org.junit.Test;

import java.io.*;

/**
 * Tests SnappyOutputStream
 */
public class SnappyOutputStreamTest {
    @Test
    public void testLargeWrite() throws IOException {
        File testFile = File.createTempFile("sparkey-test", "");
        testFile.deleteOnExit();
        FileOutputStream fos = new FileOutputStream(testFile);

        byte[] buf = new byte[1000 * 1000];
        SnappyOutputStream os = new SnappyOutputStream(10, fos, fos.getFD());
        os.write(buf);

        testFile.delete();
    }
}
