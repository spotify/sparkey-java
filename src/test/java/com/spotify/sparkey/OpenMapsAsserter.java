package com.spotify.sparkey;

import org.junit.After;
import org.junit.Before;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class OpenMapsAsserter {

  @Before
  public void setUp() throws Exception {
    assumeTrue(0 == Sparkey.getOpenMaps());
    assumeTrue(0 == Sparkey.getOpenFiles());
  }

  @After
  public void tearDown() throws Exception {
    assertEquals(0, Sparkey.getOpenMaps());
    assertEquals(0, Sparkey.getOpenFiles());
  }
}
