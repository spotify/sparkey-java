package com.spotify.sparkey;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ReadOnlyMemMapTest extends OpenMapsAsserter {

  @Test
  public void testDontRunOutOfFileDescriptors() throws Exception {
    for (int iter = 0; iter < 100; iter++) {
      ReadOnlyMemMap memMap = new ReadOnlyMemMap(new File("README.md"));
      ArrayList<ReadOnlyMemMap> maps = new ArrayList<>();
      for (int i = 0; i < 100; i++) {
        maps.add(memMap.duplicate());
      }
      memMap.close();
      for (ReadOnlyMemMap map : maps) {
        try {
          map.readUnsignedByte();
          fail();
        } catch (SparkeyReaderClosedException e) {
        }
        try {
          map.seek(1);
          fail();
        } catch (SparkeyReaderClosedException e) {
        }
        try {
          map.skipBytes(1);
          fail();
        } catch (SparkeyReaderClosedException e) {
        }
      }
      assertEquals(0, Sparkey.getOpenFiles());
      assertEquals(0, Sparkey.getOpenMaps());
    }
  }

  @Test
  public void testConcurrentReadWhileClosing() throws Exception {
    final AtomicBoolean running = new AtomicBoolean(true);
    final ReadOnlyMemMap memMap = new ReadOnlyMemMap(new File("README.md"));
    final List<Exception> failures = Collections.synchronizedList(new ArrayList<>());
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      Thread thread = new Thread(() -> {
        ReadOnlyMemMap map = memMap.duplicate();
        while (running.get()) {
          try {
            map.seek(1);
            map.readUnsignedByte();
            map.skipBytes(1);
          } catch (IOException e) {
            if (!e.getMessage().equals("Reader has been closed")) {
              e.printStackTrace();
              failures.add(e);
            }
          }
        }
      });
      threads.add(thread);
      thread.start();
    }
    memMap.close();
    Thread.sleep(100);
    running.set(false);
    for (Thread thread : threads) {
      thread.join();
    }
    assertEquals(0, failures.size());

  }
}
