package com.spotify.sparkey.extra;

import com.spotify.sparkey.SparkeyReader;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class DelegatingSparkeyReaderTest {

  @Test
  public void testDelegation() throws IOException {
    final SparkeyReader delegate = mock(SparkeyReader.class);
    final SparkeyReader reader = new DelegatingSparkeyReader(delegate);
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

  @Test
  public void testConstructorDelegate() {
    final SparkeyReader delegate = mock(SparkeyReader.class);
    final DelegatingSparkeyReader reader = new DelegatingSparkeyReader(delegate);
    assertEquals(delegate, reader.getDelegateReader());
  }

  @Test
  public void testDelegateOverride() throws IOException {
    final SparkeyReader delegate = mock(SparkeyReader.class);
    final DelegatingSparkeyReader reader = spy(new DelegatingSparkeyReader() {
      @Override
      protected SparkeyReader getDelegateReader() {
        return delegate;
      }
    });

    reader.getAsString("key");
    verify(reader).getDelegateReader();
  }

  @Test(expected = NullPointerException.class)
  public void testNullDelegate() {
    final SparkeyReader reader = new DelegatingSparkeyReader(null);
  }

  @Test(expected = NullPointerException.class)
  public void testDelegateNotSet() throws IOException {
    final DelegatingSparkeyReader reader = new DelegatingSparkeyReader();
    reader.getAsString("key");
  }

}
