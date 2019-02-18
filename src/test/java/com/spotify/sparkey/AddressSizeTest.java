package com.spotify.sparkey;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests AddressSize
 */
public class AddressSizeTest extends OpenMapsAsserter {



  @Test
  public void testAddressSizeLong() throws IOException {
    byte[] BYTES = new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
    InMemoryData imd = new InMemoryData(BYTES.length);
    for (byte b : BYTES) {
      imd.writeUnsignedByte(b);
    }
    imd.seek(0);
    Assert.assertEquals(0x0807060504030201L, AddressSize.LONG.readAddress(imd));
    imd.seek(0);
    AddressSize.LONG.writeAddress(0x0807060504030201L, imd);
    imd.seek(0);
    for (byte b : BYTES) {
      Assert.assertEquals(imd.readUnsignedByte(), b);
    }
  }


  @Test
  public void testAddressSizeInt() throws IOException {
    byte[] BYTES = new byte[] {0x01, 0x02, 0x03, 0x04};
    InMemoryData imd = new InMemoryData(BYTES.length);
    for (byte b : BYTES) {
      imd.writeUnsignedByte(b);
    }
    imd.seek(0);
    Assert.assertEquals(0x04030201L, AddressSize.INT.readAddress(imd));
    imd.seek(0);
    AddressSize.INT.writeAddress(0x04030201L, imd);
    imd.seek(0);
    for (byte b : BYTES) {
      Assert.assertEquals(imd.readUnsignedByte(), b);
    }
  }
}
