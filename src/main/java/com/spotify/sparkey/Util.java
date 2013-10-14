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

import java.io.*;

final class Util {
  static int unsignedByte(byte b) {
    return ((int) b) & 0xFF;
  }

  static void writeLittleEndianInt(int value, InMemoryData data) {
    data.writeUnsignedByte((value) & 0xFF);
    data.writeUnsignedByte((value >>> 8) & 0xFF);
    data.writeUnsignedByte((value >>> 16) & 0xFF);
    data.writeUnsignedByte((value >>> 24) & 0xFF);
  }

  static void writeLittleEndianInt(int value, RandomAccessFile rw) throws IOException {
    rw.write((value) & 0xFF);
    rw.write((value >>> 8) & 0xFF);
    rw.write((value >>> 16) & 0xFF);
    rw.write((value >>> 24) & 0xFF);
  }

  static void writeLittleEndianLong(long value, InMemoryData data) {
    data.writeUnsignedByte((int) ((value) & 0xFF));
    data.writeUnsignedByte((int) ((value >>> 8) & 0xFF));
    data.writeUnsignedByte((int) ((value >>> 16) & 0xFF));
    data.writeUnsignedByte((int) ((value >>> 24) & 0xFF));
    data.writeUnsignedByte((int) ((value >>> 32) & 0xFF));
    data.writeUnsignedByte((int) ((value >>> 40) & 0xFF));
    data.writeUnsignedByte((int) ((value >>> 48) & 0xFF));
    data.writeUnsignedByte((int) ((value >>> 56) & 0xFF));
  }

  static void writeLittleEndianLong(long value, RandomAccessFile rw) throws IOException {
    writeLittleEndianInt((int) value, rw);
    writeLittleEndianInt((int) (value >>> 32), rw);
  }

  static long readLittleEndianLong(RandomAccessData data) {
    long res = (long) data.readUnsignedByte();
    res |= ((long) data.readUnsignedByte()) << 8;
    res |= ((long) data.readUnsignedByte()) << 16;
    res |= ((long) data.readUnsignedByte()) << 24;
    res |= ((long) data.readUnsignedByte()) << 32;
    res |= ((long) data.readUnsignedByte()) << 40;
    res |= ((long) data.readUnsignedByte()) << 48;
    res |= ((long) data.readUnsignedByte()) << 56;
    return res;
  }

  static long readLittleEndianLong(InputStream input) throws IOException {
    return (long) readByte(input) | (long) readByte(input) << 8 | (long) readByte(
            input) << 16 | (long) readByte(input) << 24
            | (long) readByte(input) << 32 | (long) readByte(input) << 40 | (long) readByte(
            input) << 48 | (long) readByte(input) << 56;
  }

  static int readLittleEndianInt(RandomAccessData data) {
    int a = data.readUnsignedByte();
    int b = data.readUnsignedByte();
    int c = data.readUnsignedByte();
    int d = data.readUnsignedByte();
    return a << 0 | b << 8 | c << 16 | d << 24;
  }

  static int readLittleEndianInt(InputStream input) throws IOException {
    int a = readByte(input);
    int b = readByte(input);
    int c = readByte(input);
    int d = readByte(input);
    return (a) | (b << 8) | (c << 16) | (d << 24);
  }

  static int unsignedVLQSize(int value) {
    if (value < 1 << 7) {
      return 1;
    }
    if (value < 1 << 14) {
      return 2;
    }
    if (value < 1 << 21) {
      return 3;
    }
    if (value < 1 << 28) {
      return 4;
    }
    return 5;
  }

  static int unsignedVLQSize(long value) {
      if (value < 1L << 7) {
          return 1;
      }
      if (value < 1L << 14) {
          return 2;
      }
      if (value < 1L << 21) {
          return 3;
      }
      if (value < 1L << 28) {
          return 4;
      }
      if (value < 1L << 35) {
          return 5;
      }
      if (value < 1L << 42) {
          return 6;
      }
      if (value < 1L << 49) {
          return 7;
      }
      if (value < 1L << 56) {
          return 8;
      }
      return 9;
  }

  static void writeUnsignedVLQ(long value, OutputStream output) throws IOException {
    while (value >= 1 << 7) {
      output.write((int) (value & 0x7f) | 0x80);
      value >>>= 7;
    }
    output.write((int) value);
  }

  static void writeUnsignedVLQ(int value, OutputStream output) throws IOException {
    while (value >= 1 << 7) {
      output.write(value & 0x7f | 0x80);
      value >>>= 7;
    }
    output.write(value);
  }

  static int readUnsignedVLQInt(BlockRandomInput input) {
    int b = input.readUnsignedByte();
    int b2 = b & 0x7f;
    if (b2 == b) {
      return b;
    }
    int value = b2 & 0x7f;

    b = input.readUnsignedByte();
    b2 = b & 0x7f;
    if (b2 == b) {
      return value | b << 7;
    }
    value |= b2 << 7;

    b = input.readUnsignedByte();
    b2 = b & 0x7f;
    if (b2 == b) {
      return value | b << 14;
    }
    value |= b2 << 14;

    b = input.readUnsignedByte();
    b2 = b & 0x7f;
    if (b2 == b) {
      return value | b << 21;
    }
    value |= b2 << 21;

    b = input.readUnsignedByte();
    b2 = b & 0x7f;
    if (b2 == b) {
      return value | b << 28;
    }
    throw new RuntimeException("Too long VLQ value");
  }

  static int readUnsignedVLQInt(InputStream input) throws IOException {
    int b = readByte(input);
    int b2 = b & 0x7f;
    if (b2 == b) {
      return b;
    }
    int value = b2 & 0x7f;

    b = readByte(input);
    b2 = b & 0x7f;
    if (b2 == b) {
      return value | b << 7;
    }
    value |= b2 << 7;

    b = readByte(input);
    b2 = b & 0x7f;
    if (b2 == b) {
      return value | b << 14;
    }
    value |= b2 << 14;

    b = readByte(input);
    b2 = b & 0x7f;
    if (b2 == b) {
      return value | b << 21;
    }
    value |= b2 << 21;

    b = readByte(input);
    b2 = b & 0x7f;
    if (b2 == b) {
      return value | b << 28;
    }
    throw new RuntimeException("Too long VLQ value");
  }

  static int readByte(InputStream input) throws IOException {
    int read = input.read();
    if (read < 0) {
      throw new EOFException();
    }
    return read;
  }

  static void copy(long len, InputStream inputStream, OutputStream outputStream, byte[] buf) throws IOException {
    long full = len / buf.length;
    while (full > 0) {
      full--;
      readFully(inputStream, buf, buf.length);
      outputStream.write(buf, 0, buf.length);
    }
    int tail = (int) (len % buf.length);
    readFully(inputStream, buf, tail);
    outputStream.write(buf, 0, tail);
  }

  static void readFully(InputStream inputStream, byte[] buf, int len) throws IOException {
    int pos = 0;
    int remaining = len;
    while (pos < len) {
      int read = inputStream.read(buf, pos, remaining);
      if (read == -1) {
        throw new EOFException();
      }
      pos += read;
      remaining -= read;
    }
  }

  static boolean equals(int len, byte[] a, byte[] b) {
    while (--len >= 0) {
      if (a[len] != b[len]) {
        return false;
      }
    }
    return true;
  }
}
