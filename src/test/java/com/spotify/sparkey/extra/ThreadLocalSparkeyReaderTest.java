package com.spotify.sparkey.extra;

import com.spotify.sparkey.CompressionType;
import com.spotify.sparkey.Sparkey;
import com.spotify.sparkey.SparkeyReader;
import com.spotify.sparkey.SparkeyWriter;
import com.spotify.sparkey.system.BaseSystemTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotSame;
import static org.mockito.Mockito.*;

public class ThreadLocalSparkeyReaderTest extends BaseSystemTest {
  private ThreadLocalSparkeyReader reader;

  @Before
  public void setUp() throws Exception {
    super.setUp();

    SparkeyWriter writer = Sparkey.createNew(indexFile, CompressionType.NONE, 1024);
    for (int i = 0; i < 10; i++) {
      writer.put("key_" + i, "value_"+i);
    }
    writer.writeHash();
    writer.close();

    reader = new ThreadLocalSparkeyReader(indexFile);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    if (reader != null) {
      reader.close();
    }
    super.tearDown();
  }

  @Test
  public void testCorrectness() throws IOException {
    for (int i=0; i<10; i++) {
      assertEquals("value_"+i, reader.getAsString("key_"+i));
    }
  }

  @Test
  public void testDifferentReaders() throws Exception {
    final SparkeyReader localReader = reader.getDelegateReader();

    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    final SparkeyReader remoteReader = executorService.submit(() -> reader.getDelegateReader()).get();
    executorService.shutdown();

    assertNotSame(localReader, remoteReader);
  }

  @Test
  public void testClose() throws Exception {
    final List<SparkeyReader> allReaders = new ArrayList<>();
    final SparkeyReader reader = mock(SparkeyReader.class);
    when(reader.duplicate()).thenAnswer((Answer<SparkeyReader>) invocation -> {
      SparkeyReader result = mock(SparkeyReader.class);
      allReaders.add(result);
      return result;
    });
    allReaders.add(reader);

    new ThreadLocalSparkeyReader(reader).close();
    for (SparkeyReader r: allReaders) {
      verify(r).close();
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testAlreadyClosed() throws IOException {
    reader.close();
    reader.getAsString("key_1");
  }

}
