package com.spotify.sparkey;

import org.junit.After;
import org.junit.Before;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class OpenMapsAsserter {

  private int openMaps;
  private int openFiles;

  @Before
  public void setUp() throws Exception {
    openMaps = Sparkey.getOpenMaps();
    openFiles = Sparkey.getOpenFiles();
  }

  @After
  public void tearDown() throws Exception {
    assertEquals(openMaps, Sparkey.getOpenMaps());
    assertEquals(openFiles, Sparkey.getOpenFiles());
  }
}
