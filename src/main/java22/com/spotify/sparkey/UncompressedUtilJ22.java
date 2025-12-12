/*
 * Copyright (c) 2025 Spotify AB
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

import java.io.IOException;

/**
 * Utility methods for reading from immutable memory-mapped files.
 * All methods take explicit positions. Use Util.unsignedVLQSize() to determine byte count.
 */
final class UncompressedUtilJ22 {

  private UncompressedUtilJ22() {}

  /**
   * Read variable-length quantity (VLQ) integer at given position.
   * Returns the decoded value. Use Util.unsignedVLQSize(value) to determine bytes consumed.
   */
  static int readVLQInt(ReadOnlyMemMapJ22 data, long position) throws IOException {
    long p = position;
    int value = 0;
    int shift = 0;

    while (true) {
      int b = data.readUnsignedByte(p++);
      value |= (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        break;
      }
      shift += 7;
    }

    return value;
  }

  /**
   * Read variable-length quantity (VLQ) as long at given position.
   * Supports values larger than Integer.MAX_VALUE.
   * Returns the decoded value. Use Util.unsignedVLQSize(value) to determine bytes consumed.
   */
  static long readVLQLong(ReadOnlyMemMapJ22 data, long position) throws IOException {
    long p = position;
    long value = 0;
    int shift = 0;

    while (true) {
      int b = data.readUnsignedByte(p++);
      value |= (long)(b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        break;
      }
      shift += 7;
      if (shift >= 64) {
        throw new RuntimeException("VLQ overflow - value too large for long");
      }
    }

    return value;
  }
}
