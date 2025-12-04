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

/**
 * Array comparison utilities.
 * Overridden in Java 9+ MRJAR layer to use Arrays.equals() intrinsic (SIMD optimized).
 */
class ArrayUtil {

  /**
   * Compare byte array ranges.
   * Java 8: Manual loop.
   * Java 9+: Arrays.equals() intrinsic with SIMD optimization.
   */
  static boolean equals(int len, byte[] a, int aOffset, byte[] b, int bOffset) {
    for (int i = 0; i < len; i++) {
      if (a[aOffset + i] != b[bOffset + i]) {
        return false;
      }
    }
    return true;
  }

  private ArrayUtil() {
    // Utility class
  }
}
