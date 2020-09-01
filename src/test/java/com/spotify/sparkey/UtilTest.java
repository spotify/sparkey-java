package com.spotify.sparkey;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests Util
 */
public class UtilTest {

  public static void setMapBits(int bits) {
    ReadOnlyMemMap.MAP_SIZE_BITS = bits;
  }

  @Test
  public void testUnsignedByte() {
    assertEquals(0, Util.unsignedByte((byte) 0));
    assertEquals(127, Util.unsignedByte((byte) 127));
    assertEquals(128, Util.unsignedByte((byte) -128));
    assertEquals(128, Util.unsignedByte((byte) 128));
    assertEquals(255, Util.unsignedByte((byte) -1));
  }

  @Test
  public void testUnsignedVLQSize() {
    assertEquals(1, Util.unsignedVLQSize(1L << 4));
    assertEquals(1, Util.unsignedVLQSize(1 << 6));
    assertEquals(2, Util.unsignedVLQSize(1L << 7));
    assertEquals(2, Util.unsignedVLQSize(1 << 13));
    assertEquals(3, Util.unsignedVLQSize(1L << 14));
    assertEquals(3, Util.unsignedVLQSize(1 << 20));
    assertEquals(4, Util.unsignedVLQSize(1L << 21));
    assertEquals(4, Util.unsignedVLQSize(1 << 27));
    assertEquals(5, Util.unsignedVLQSize(1 << 28));
    assertEquals(5, Util.unsignedVLQSize(1L << 34));
    assertEquals(6, Util.unsignedVLQSize(1L << 35));
    assertEquals(6, Util.unsignedVLQSize(1L << 41));
    assertEquals(7, Util.unsignedVLQSize(1L << 42));
    assertEquals(7, Util.unsignedVLQSize(1L << 48));
    assertEquals(8, Util.unsignedVLQSize(1L << 49));
    assertEquals(8, Util.unsignedVLQSize(1L << 55));
    assertEquals(9, Util.unsignedVLQSize(1L << 56));
    assertEquals(9, Util.unsignedVLQSize(Long.MAX_VALUE));
  }

  @Test
  public void testReadUnsignedVLQInt() throws Exception {
    checkVLQ(         3, new byte[] {(byte)0x03});
    checkVLQ(     14330, new byte[] {(byte)0xfa, (byte)0x6f});
    checkVLQ(     48030, new byte[] {(byte)0x9e, (byte)0xf7, (byte)0x02});
    checkVLQ( 205897768, new byte[] {(byte)0xa8, (byte)0x80, (byte)0x97, (byte)0x62});
    checkVLQ(1325939940, new byte[] {(byte)0xe4, (byte)0xf9, (byte)0xa0, (byte)0xf8, (byte)0x04});
    checkVLQ(1898691403, new byte[] {(byte)0xcb, (byte)0xf6, (byte)0xae, (byte)0x89, (byte)0x07});
    try {
      Util.readUnsignedVLQInt(new DummyBlockRandomInput(new byte[]{(byte) 0xcb, (byte) 0xcb,
          (byte) 0xf6, (byte) 0xae, (byte) 0x89, (byte) 0x07}));
      Assert.fail("Should have thrown RuntimeException");
    } catch (RuntimeException e) {
      //pass
    }
    try {
      Util.readUnsignedVLQInt(new ByteArrayInputStream(new byte[]{(byte) 0xcb, (byte) 0xcb,
          (byte) 0xf6, (byte) 0xae, (byte) 0x89, (byte) 0x07}));
      Assert.fail("Should have thrown RuntimeException");
    } catch (RuntimeException e) {
      //pass
    }
  }

  @Test
  public void testReadByte() throws Exception {
    ByteArrayInputStream bais = new ByteArrayInputStream(new byte[] {(byte)0x00});
    assertEquals((byte) 0x00, Util.readByte(bais));
    try {
      Util.readByte(bais);
      Assert.fail("Should have thrown EOFException");
    } catch (EOFException e) {
      // pass
    }
  }

  @Test
  public void testCopy() throws Exception {
    byte[] bytes = "deadbeefdeadbeef".getBytes();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Util.copy(bytes.length, new ByteArrayInputStream(bytes), baos, new byte[5]);
    Assert.assertArrayEquals(bytes, baos.toByteArray());
  }

  @Test
  public void testEquals() throws Exception {
    assertTrue(Util.equals(3, "apa".getBytes(), "apa".getBytes()));
    assertFalse(Util.equals(3, "apa".getBytes(), "foo".getBytes()));
  }

  @Test
  public void testReadFully() throws Exception {
    byte[] bytes = "testgurka".getBytes();
    byte[] buf = new byte[bytes.length];
    Util.readFully(new ByteArrayInputStream(bytes), buf, bytes.length);
    Assert.assertArrayEquals(bytes, buf);
    try {
      Util.readFully(new ByteArrayInputStream(bytes), new byte[100], 100);
      Assert.fail("Should have thrown EOFException");
    } catch (EOFException e) {
      // pass
    }
  }

  private void checkVLQ(int i, byte[] bytes) {
    try {
      assertEquals(i, Util.readUnsignedVLQInt(new ByteArrayInputStream(bytes)));
      assertEquals(i, Util.readUnsignedVLQInt(new DummyBlockRandomInput(bytes)));
    } catch (IOException e) {
      throw new Error(e);
    }
  }

  static class DummyBlockRandomInput implements BlockRandomInput {

    private final byte[] data;
    private int pos;

    public DummyBlockRandomInput(byte[] data) {
      this.data = data;
    }

    @Override
    public void close() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void seek(long pos) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int readUnsignedByte() {
      return data[pos++];
    }

    @Override
    public void readFully(byte[] buffer, int offset, int length) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void skipBytes(long amount) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BlockRandomInput duplicate() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void closeDuplicate() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getLoadedBytes() {
      return data.length;
    }
  }

  @Test
  public void testRenameDestExists() throws Exception {
    File src = new File("dummy-file-src");
    File dest = new File("dummy-file-dest");

    try {
      FileUtils.write(src, "src");
      FileUtils.write(dest, "dest");

      assertEquals("src", FileUtils.readFileToString(src));
      assertEquals("dest", FileUtils.readFileToString(dest));

      Util.renameFile(src, dest);

      assertFalse(src.exists());
      assertEquals("src", FileUtils.readFileToString(dest));
    } finally {
      src.delete();
      dest.delete();
    }
  }

  @Test
  public void testRenameSameFile() throws Exception {
    File src = new File("dummy-file-src");
    File dest = new File("dummy-file-src");

    try {
      FileUtils.write(src, "src");
      Util.renameFile(src, dest);
      assertTrue(src.exists());
      assertEquals("src", FileUtils.readFileToString(src));
    } finally {
      src.delete();
      dest.delete();
    }
  }

  @Test(expected = FileNotFoundException.class)
  public void testRenameFileDoesNotExist() throws Exception {
    File src = new File("dummy-file-src");
    File dest = new File("dummy-file-dest");

    try {
      Util.renameFile(src, dest);
      fail();
    } finally {
      src.delete();
      dest.delete();
    }
  }
}
