package com.spotify.sparkey.extra;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.spotify.sparkey.CompressionType;
import com.spotify.sparkey.OpenMapsAsserter;
import com.spotify.sparkey.Sparkey;
import com.spotify.sparkey.SparkeyWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

public class ReloadableSparkeyReaderTest extends OpenMapsAsserter {
  private final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
  private File logFile1;
  private File logFile2;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    logFile1 = createLogFile("key1", "value1");
    logFile2 = createLogFile("key2", "value2");

    logFile1.deleteOnExit();
    logFile2.deleteOnExit();
  }

  @After
  public void tearDown() throws Exception {
    logFile1.delete();
    Sparkey.getIndexFile(logFile1).delete();
    logFile2.delete();
    Sparkey.getIndexFile(logFile2).delete();
    super.tearDown();
  }

  private static File createLogFile(String key, String value) throws IOException {
    final File logFile = File.createTempFile("sparkey", ".spl");

    SparkeyWriter writer = Sparkey.createNew(logFile, CompressionType.NONE, 1024);
    writer.put(key, value);
    writer.writeHash();
    writer.close();

    return logFile;
  }

  @Test
  public void testFromLogFile() throws ExecutionException, InterruptedException, IOException {
    try (ReloadableSparkeyReader reader = ReloadableSparkeyReader.fromLogFile(logFile1, executorService)
            .toCompletableFuture().get()) {
      assertEquals("value1", reader.getAsString("key1"));
    }
  }

  @Test
  public void testReload() throws ExecutionException, InterruptedException, IOException {
    try (ReloadableSparkeyReader reader = ReloadableSparkeyReader.fromLogFile(logFile1, executorService)
            .toCompletableFuture().get()) {
      reader.load(logFile2).toCompletableFuture().get();
      assertEquals("value2", reader.getAsString("key2"));

      reader.load(logFile1).toCompletableFuture().get();
      assertEquals("value1", reader.getAsString("key1"));
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullExecutorService() {
    ReloadableSparkeyReader.fromLogFile(logFile1, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidLogFile() {
    ReloadableSparkeyReader.fromLogFile(new File("some-nonexisting-file"), executorService);
  }

}
