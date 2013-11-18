package com.spotify.sparkey.extra;

import com.spotify.sparkey.SparkeyReader;

import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;

public class DelegatingSparkeyReaderTest {

  private static final class MockDelegatingSparkeyReader extends AbstractDelegatingSparkeyReader {
    private final SparkeyReader delegate = mock(SparkeyReader.class);

    @Override
    protected SparkeyReader getDelegateReader() {
      return this.delegate;
    }
  }

  @Test
  public void testDelegation() throws IOException {
    final MockDelegatingSparkeyReader reader = new MockDelegatingSparkeyReader();
    final SparkeyReader delegate = reader.getDelegateReader();
    final String key = "key";

    reader.getAsString(key);
    verify(delegate).getAsString(key);

    reader.getAsByteArray(key.getBytes());
    verify(delegate).getAsByteArray(key.getBytes());

    reader.getAsEntry(key.getBytes());
    verify(delegate).getAsEntry(key.getBytes());

    reader.getIndexHeader();
    verify(delegate).getIndexHeader();

    reader.getLogHeader();
    verify(delegate).getLogHeader();

    reader.duplicate();
    verify(delegate).duplicate();

    reader.iterator();
    verify(delegate).iterator();

    reader.close();
    verify(delegate).close();
  }

}
