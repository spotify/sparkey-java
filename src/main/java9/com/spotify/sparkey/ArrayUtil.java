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
package com.spotify.sparkey;

import java.util.Arrays;

/**
 * Java 9+ optimized array comparison using Arrays.equals() intrinsic.
 *
 * The JIT compiler recognizes Arrays.equals() with ranges and generates
 * vectorized code (AVX2/AVX-512) for ~2-4x speedup on modern CPUs.
 */
class ArrayUtil {

  /**
   * Compare byte array ranges using Java 9+ Arrays.equals() intrinsic.
   * JIT-compiled to SIMD instructions (AVX2/AVX-512).
   */
  static boolean equals(int len, byte[] a, int aOffset, byte[] b, int bOffset) {
    return Arrays.equals(a, aOffset, aOffset + len, b, bOffset, bOffset + len);
  }

  private ArrayUtil() {
    // Utility class
  }
}
