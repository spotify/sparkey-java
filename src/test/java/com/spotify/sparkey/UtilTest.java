package com.spotify.sparkey;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;

/**
 * Tests Util
 */
public class UtilTest {

  @Test
  public void testUnsignedByte() {
    Assert.assertEquals(0, Util.unsignedByte((byte)0));
    Assert.assertEquals(127, Util.unsignedByte((byte)127));
    Assert.assertEquals(128, Util.unsignedByte((byte)-128));
    Assert.assertEquals(128, Util.unsignedByte((byte)128));
    Assert.assertEquals(255, Util.unsignedByte((byte)-1));
  }

  @Test
  public void testUnsignedVLQSize() {
    Assert.assertEquals(1, Util.unsignedVLQSize(1L << 4));
    Assert.assertEquals(1, Util.unsignedVLQSize(1 << 6));
    Assert.assertEquals(2, Util.unsignedVLQSize(1L << 7));
    Assert.assertEquals(2, Util.unsignedVLQSize(1 << 13));
    Assert.assertEquals(3, Util.unsignedVLQSize(1L << 14));
    Assert.assertEquals(3, Util.unsignedVLQSize(1 << 20));
    Assert.assertEquals(4, Util.unsignedVLQSize(1L << 21));
    Assert.assertEquals(4, Util.unsignedVLQSize(1 << 27));
    Assert.assertEquals(5, Util.unsignedVLQSize(1 << 28));
    Assert.assertEquals(5, Util.unsignedVLQSize(1L << 34));
    Assert.assertEquals(6, Util.unsignedVLQSize(1L << 35));
    Assert.assertEquals(6, Util.unsignedVLQSize(1L << 41));
    Assert.assertEquals(7, Util.unsignedVLQSize(1L << 42));
    Assert.assertEquals(7, Util.unsignedVLQSize(1L << 48));
    Assert.assertEquals(8, Util.unsignedVLQSize(1L << 49));
    Assert.assertEquals(8, Util.unsignedVLQSize(1L << 55));
    Assert.assertEquals(9, Util.unsignedVLQSize(1L << 56));
    Assert.assertEquals(9, Util.unsignedVLQSize(Long.MAX_VALUE));
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
    Assert.assertEquals((byte) 0x00, Util.readByte(bais));
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
    Assert.assertTrue(Util.equals(3, "apa".getBytes(), "apa".getBytes()));
    Assert.assertFalse(Util.equals(3, "apa".getBytes(), "foo".getBytes()));
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
      Assert.assertEquals(i, Util.readUnsignedVLQInt(new ByteArrayInputStream(bytes)));
      Assert.assertEquals(i, Util.readUnsignedVLQInt(new DummyBlockRandomInput(bytes)));
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
  }
}
