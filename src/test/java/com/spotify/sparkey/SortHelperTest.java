package com.spotify.sparkey;

import static com.spotify.sparkey.SortHelper.ENTRY_COMPARATOR;
import static org.junit.Assert.assertEquals;

import com.carrotsearch.sizeof.RamUsageEstimator;
import com.google.common.primitives.Longs;
import java.util.Comparator;
import org.junit.Test;

public class SortHelperTest {

  static final Comparator<SortHelper.Entry> REFERENCE_COMPARATOR = new Comparator<SortHelper.Entry>() {
    @Override
    public int compare(final SortHelper.Entry o1, final SortHelper.Entry o2) {
      final int v = Longs.compare(o1.wantedSlot, o2.wantedSlot);
      if (v != 0) {
        return v;
      }
      return Longs.compare(o1.address, o2.address);
    }
  };

  @Test
  public void testComparator() {
    long[] values = new long[]{0, 1, 2, 3, Long.MAX_VALUE, Long.MAX_VALUE - 1, Long.MAX_VALUE / 2};
    for (long capacity : values) {
      if (capacity == 0) {
        continue;
      }
      for (long a : values) {
        for (long b : values) {
          SortHelper.Entry first = new SortHelper.Entry(a, b, capacity);
          for (long c : values) {
            for (long d : values) {
              SortHelper.Entry second = new SortHelper.Entry(c, d, capacity);
              int expected = REFERENCE_COMPARATOR.compare(first, second);
              int actual = ENTRY_COMPARATOR.compare(first, second);
              assertEquals(Integer.signum(expected), Integer.signum(actual));
            }
          }
        }
      }
    }
  }

  @Test
  public void testEntrySize() {
    long size = RamUsageEstimator.sizeOf(new SortHelper.Entry(123, 456, 789));
    assertEquals(SortHelper.ENTRY_SIZE, size);

  }
}