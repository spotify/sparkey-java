/*
 * Copyright (c) 2011-2013 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.spotify.sparkey.system;

import com.spotify.sparkey.OpenMapsAsserter;
import com.spotify.sparkey.Sparkey;
import com.spotify.sparkey.UtilTest;
import com.sun.management.UnixOperatingSystemMXBean;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

public class BaseSystemTest extends OpenMapsAsserter {
  protected File indexFile;
  protected File logFile;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    UtilTest.setMapBits(10);
    indexFile = File.createTempFile("sparkey", ".spi");
    logFile = Sparkey.getLogFile(indexFile);
    indexFile.deleteOnExit();
    logFile.deleteOnExit();
  }

  @After
  public void tearDown() throws Exception {
    indexFile.delete();
    logFile.delete();
    super.tearDown();
  }

  @Test
  public void testDummy() throws Exception {
  }

  static long countOpenFileDescriptors() {
    OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
    if(os instanceof UnixOperatingSystemMXBean){
      long openFileDescriptorCount = ((UnixOperatingSystemMXBean) os).getOpenFileDescriptorCount();
      return openFileDescriptorCount;
    }
    return -1;
  }
}
