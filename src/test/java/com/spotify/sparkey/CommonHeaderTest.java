package com.spotify.sparkey;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests CommonHeader
 */
public class CommonHeaderTest {

  @Test
  public void testCommonHeader() throws IOException {
    CommonHeader ch = new CommonHeader(1,2,3,4,5,6,7) { };
    try {
      ch = new CommonHeader(1,2,3,4,5,-1,7) { };
      Assert.fail("Negative key len size should trigger IOException");
    } catch (IOException e) {
      // pass
    }
    try {
      ch = new CommonHeader(1,2,3,4,4294967296L,6,7) { };
      Assert.fail("Key len size larger than 2**31 should trigger IOException");
    } catch (IOException e) {
      // pass
    }
    try {
      ch = new CommonHeader(1,2,3,4,-1,6,7) { };
      Assert.fail("Value len size smaller than 0 should trigger IOException");
    } catch (IOException e) {
      // pass
    }
  }
}
