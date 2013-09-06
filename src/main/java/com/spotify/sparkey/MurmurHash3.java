//-----------------------------------------------------------------------------
// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.

// Note - The x86 and x64 versions do _not_ produce the same results, as the
// algorithms are optimized for their respective platforms. You can still
// compile and run any of them on any platform, but your performance with the
// non-native version will be less than optimal.

package com.spotify.sparkey;

/**
 * Java port of the MurmurHash3 algorithm.
 * Copied from http://code.google.com/p/smhasher/source/browse/trunk/MurmurHash3.cpp
 */
final class MurmurHash3 {

  static int murmurHash3_x86_32(byte[] data, int len, int seed) {
    final int nblocks = len / 4;

    int h1 = seed;

    int c1 = 0xcc9e2d51;
    int c2 = 0x1b873593;

    //----------
    // body

    for (int i = 0; i < nblocks; i++) {
      int k1 = getBlock32(data, 4 * i);

      k1 *= c1;
      k1 = (k1 << 15) | (k1 >>> (32 - 15));
      k1 *= c2;

      h1 ^= k1;
      h1 = (h1 << 13) | (h1 >>> (32 - 13));
      h1 = h1 * 5 + 0xe6546b64;
    }

    //----------
    // tail

    int tail = 4 * nblocks;

    int k1 = 0;

    switch (len & 3) {
      case 3:
        k1 ^= Util.unsignedByte(data[tail + 2]) << 16;
      case 2:
        k1 ^= Util.unsignedByte(data[tail + 1]) << 8;
      case 1:
        k1 ^= Util.unsignedByte(data[tail]);
        k1 *= c1;
        k1 = (k1 << 15) | (k1 >>> (32 - 15));
        k1 *= c2;
        h1 ^= k1;
    }

    //----------
    // finalization

    h1 ^= len;

    int h = h1;
    h ^= h >>> 16;
    h *= 0x85ebca6b;
    h ^= h >>> 13;
    h *= 0xc2b2ae35;
    h ^= h >>> 16;
    h1 = h;

    return h1;
  }

  private static int getBlock32(byte[] data, int i) {
    return Util.unsignedByte(data[i]) |
            Util.unsignedByte(data[i + 1]) << 8 |
            Util.unsignedByte(data[i + 2]) << 16 |
            Util.unsignedByte(data[i + 3]) << 24;
  }

  private static long getBlock64(byte[] data, int i) {
    long low = ((long) getBlock32(data, 8 * i)) & 0xFFFFFFFFL;
    long high = ((long) getBlock32(data, 8 * i + 4)) & 0xFFFFFFFFL;
    return low | high << 32;
  }

  private static long fmix64(long k) {
    k ^= k >>> 33;
    k *= 0xff51afd7ed558ccdL;
    k ^= k >>> 33;
    k *= 0xc4ceb9fe1a85ec53L;
    k ^= k >>> 33;

    return k;
  }

  static long murmurHash3_x64_64(byte[] data, int len, int seed) {
    final int nblocks = len / 16;

    long h1 = ((long) seed) & 0xFFFFFFFFL;
    long h2 = h1;

    final long c1 = 0x87c37b91114253d5L;
    final long c2 = 0x4cf5ad432745937fL;

    //----------
    // body

    for (int i = 0; i < nblocks; i++) {
      long k1 = getBlock64(data, 2 * i);
      long k2 = getBlock64(data, 2 * i + 1);

      k1 *= c1;
      k1 = ROTL64(k1, 31);
      k1 *= c2;
      h1 ^= k1;

      h1 = ROTL64(h1, 27);
      h1 += h2;
      h1 = h1 * 5 + 0x52dce729L;

      k2 *= c2;
      k2 = ROTL64(k2, 33);
      k2 *= c1;
      h2 ^= k2;

      h2 = ROTL64(h2, 31);
      h2 += h1;
      h2 = h2 * 5 + 0x38495ab5L;
    }

    //----------
    // tail

    int tail = 16 * nblocks;

    long k1 = 0;
    long k2 = 0;

    switch (len & 15) {
      case 15:
        k2 ^= (long) (Util.unsignedByte(data[tail + 14])) << 48;
      case 14:
        k2 ^= (long) (Util.unsignedByte(data[tail + 13])) << 40;
      case 13:
        k2 ^= (long) (Util.unsignedByte(data[tail + 12])) << 32;
      case 12:
        k2 ^= (long) (Util.unsignedByte(data[tail + 11])) << 24;
      case 11:
        k2 ^= (long) (Util.unsignedByte(data[tail + 10])) << 16;
      case 10:
        k2 ^= (long) (Util.unsignedByte(data[tail + 9])) << 8;
      case 9:
        k2 ^= (long) (Util.unsignedByte(data[tail + 8])) << 0;
        k2 *= c2;
        k2 = ROTL64(k2, 33);
        k2 *= c1;
        h2 ^= k2;

      case 8:
        k1 ^= (long) (Util.unsignedByte(data[tail + 7])) << 56;
      case 7:
        k1 ^= (long) (Util.unsignedByte(data[tail + 6])) << 48;
      case 6:
        k1 ^= (long) (Util.unsignedByte(data[tail + 5])) << 40;
      case 5:
        k1 ^= (long) (Util.unsignedByte(data[tail + 4])) << 32;
      case 4:
        k1 ^= (long) (Util.unsignedByte(data[tail + 3])) << 24;
      case 3:
        k1 ^= (long) (Util.unsignedByte(data[tail + 2])) << 16;
      case 2:
        k1 ^= (long) (Util.unsignedByte(data[tail + 1])) << 8;
      case 1:
        k1 ^= (long) (Util.unsignedByte(data[tail + 0])) << 0;
        k1 *= c1;
        k1 = ROTL64(k1, 31);
        k1 *= c2;
        h1 ^= k1;
    }

    //----------
    // finalization

    h1 ^= len;
    h2 ^= len;

    h1 += h2;
    h2 += h1;

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 += h2;
    h2 += h1;

    return h1;
  }

  private static long ROTL64(long x, int r) {
    return (x << r) | (x >>> (64 - r));
  }

}
