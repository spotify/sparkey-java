package com.spotify.sparkey;

import com.carrotsearch.sizeof.RamUsageEstimator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SortHelperTest {

  @Test
  public void testEntrySize() {
    long size = RamUsageEstimator.sizeOf(SortHelper.Entry.fromHash(123, 456, 789));
    assertEquals(SortHelper.ENTRY_SIZE, size);

  }
}