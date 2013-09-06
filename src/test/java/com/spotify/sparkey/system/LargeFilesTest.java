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

import com.spotify.sparkey.*;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

// Ignore since it requires a lot of memory to run.
@Ignore
public class LargeFilesTest extends BaseSystemTest {
  @Test
  public void testLargeLogFile() throws IOException {
    String expectedValue = "value";
    while (expectedValue.length() < 1024*1024) {
      expectedValue += expectedValue;
    }

    byte[] value = expectedValue.getBytes();

    SparkeyWriter writer = Sparkey.createNew(indexFile, CompressionType.NONE, 1024);
    for (int i = 0; i < 2000; i++) {
      writer.put(("key_" + i).getBytes(), value);
    }
    writer.writeHash();
    writer.close();

    assertTrue(logFile.length() > 2L*1024*1024*1024);
    SparkeyReader reader = Sparkey.open(indexFile);
    for (int i = 0; i < 2000; i += 100) {
      assertEquals(expectedValue, reader.getAsString("key_" + i));
    }
    assertEquals(null, reader.getAsString("key_" + 2000));
    reader.close();
  }

  @Test
  public void testLargeIndexFile() throws IOException {
    SparkeyWriter writer = Sparkey.createNew(indexFile, CompressionType.NONE, 1024);
    for (int i = 0; i < 70000000; i++) {
      writer.put(("key_" + i), "" + (i % 13));
    }
    writer.writeHash(HashType.HASH_64_BITS);
    writer.close();

    assertTrue(indexFile.length() > 1L*1024*1024*1024);
    SparkeyReader reader = Sparkey.open(indexFile);
    for (int i = 0; i < 70000000; i += 1000) {
      assertEquals("" + (i % 13), reader.getAsString("key_" + i));
    }
    assertEquals(null, reader.getAsString("key_" + 70000000));
    reader.close();
  }
}
