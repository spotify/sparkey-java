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
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An iterable view of all the entries in the log file.
 *
 * The iterator object is not thread safe,
 * and the entry objects are highly volatile
 * and will be invalidated by the next
 * iteration step. Don't leak this entry,
 * copy whatever data you want from it instead.
 *
 */
public final class SparkeyLogIterator implements Iterable<SparkeyReader.Entry> {
  private final File logFile;
  final LogHeader header;
  private final long start;
  private final long end;

  public SparkeyLogIterator(File logFile) throws IOException {
    header = LogHeader.read(logFile);
    this.logFile = logFile;
    this.start = header.size();
    this.end = header.getDataEnd();
  }

  SparkeyLogIterator(File logFile, long start, long end) throws IOException {
    header = LogHeader.read(logFile);

    this.logFile = logFile;
    this.start = start == -1 ? header.size() : start;
    this.end = end;
  }

  /**
   * Get an iterator over all the entries in the log file.
   *
   * The iterator object is not thread safe,
   * and the entry objects are highly volatile
   * and will be invalidated by the next
   * iteration step. Don't leak this entry,
   * copy whatever data you want from it instead.
   *
   * @return an iterator
   */
  @Override
  public Iterator<SparkeyReader.Entry> iterator() {
    try {
      final BlockPositionedInputStream stream;
      {
        final InputStream stream2 = new BufferedInputStream(new FileInputStream(logFile), 128 * 1024);
        Sparkey.incrOpenFiles();
        stream2.skip(start);

        stream = header.getCompressionTypeBackend().createBlockInput(stream2, header.getCompressionBlockSize(), start);
      }

      return new Iterator<SparkeyReader.Entry>() {
        private long prevPos;
        private long pos = start;
        private final byte[] keyBuf = new byte[(int) header.getMaxKeyLen()];
        private final Entry entry = new Entry(stream, keyBuf);
        private boolean ready = false;
        private int entryIndex;
        private boolean closed = false;

        public boolean hasNext() {
          if (ready) {
            return true;
          }
          try {
            entry.stream.skipRemaining();
            pos = stream.getBlockPosition();
            if (pos >= end) {
              closeStream();
              return false;
            }
            if (pos == prevPos) {
              entryIndex++;
            } else {
              entryIndex = 0;
            }
            prevPos = pos;

            entry.position = pos;
            entry.entryIndex = entryIndex;

            entry.type = null;
            entry.keyLen = 0;
            entry.valueLen = 0;
            int first;
            try {
              first = Util.readUnsignedVLQInt(stream);
            } catch (EOFException e) {
              closeStream();
              return false;
            }
            int second = Util.readUnsignedVLQInt(stream);
            long remaining;
            if (first == 0) {
              entry.type = SparkeyReader.Type.DELETE;
              entry.keyLen = second;
              entry.valueLen = 0;
              remaining = 0;
            } else {
              entry.type = SparkeyReader.Type.PUT;
              entry.keyLen = first - 1;
              entry.valueLen = second;
              remaining = entry.valueLen;
            }
            stream.read(keyBuf, 0, entry.keyLen);
            entry.stream.setRemaining(remaining);
            ready = true;
            return true;
          } catch (IOException e) {
            closeStream();
            throw new RuntimeException(e);
          }
        }

        private void closeStream() {
          if (closed) {
            return;
          }
          closed = true;
          Sparkey.decrOpenFiles();
          Util.nonThrowingClose(stream);
        }

        public Entry next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          ready = false;
          return entry;
        }

        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static class Entry implements SparkeyReader.Entry {
    private final SafeStream stream;

    private SparkeyReader.Type type;
    private final byte[] keyBuf;
    private int keyLen;
    private long valueLen;
    private long position;
    private int entryIndex;

    public Entry(InputStream stream, byte[] keyBuf) {
      this.keyBuf = keyBuf;
      this.stream = new SafeStream(stream);
    }

    @Override
    public SparkeyReader.Type getType() {
      return type;
    }

    @Override
    public int getKeyLength() {
      return keyLen;
    }

    public byte[] getKeyBuf() {
      return keyBuf;
    }

    @Override
    public byte[] getKey() {
      if (keyBuf.length == keyLen) {
        return keyBuf;
      }
      byte[] key = new byte[keyLen];
      System.arraycopy(keyBuf, 0, key, 0, keyLen);
      return key;
    }

    @Override
    public String getKeyAsString() {
      return new String(keyBuf, 0, keyLen, StandardCharsets.UTF_8);
    }

    @Override
    public long getValueLength() {
      return valueLen;
    }

    @Override
    public String getValueAsString() throws IOException {
      return new String(getValue(), StandardCharsets.UTF_8);
    }

    @Override
    public InputStream getValueAsStream() {
      return stream;
    }

    @Override
    public byte[] getValue() throws IOException {
      if (valueLen > Integer.MAX_VALUE) {
        throw new IllegalStateException("Value size is " + valueLen + " bytes, can't store in byte[]");
      }
      return readChunk((int) valueLen);
    }

    private byte[] readChunk(int size) throws IOException {
      byte[] res = new byte[size];
      stream.read(res);
      return res;
    }

    public long getPosition() {
      return position;
    }

    @Override
    public String toString() {
      return "Entry{" +
              "stream=" + stream +
              ", type=" + type +
              ", keyLen=" + keyLen +
              ", valueLen=" + valueLen +
              ", position=" + position +
              '}';
    }

    public int getEntryIndex() {
      return entryIndex;
    }
  }

  static class SafeStream extends InputStream {
    private final InputStream stream;
    private long remaining;

    private SafeStream(InputStream stream) {
      this.stream = stream;
    }

    @Override
    public int read() throws IOException {
      if (remaining >= 0) {
        remaining--;
        return stream.read();
      }
      throw new EOFException();
    }

    @Override
    public int read(byte[] b) throws IOException {
      return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (len < 0) {
        throw new IllegalArgumentException();
      }
      if (remaining >= len) {
        remaining -= len;
        return stream.read(b, off, len);
      }
      throw new EOFException();
    }

    @Override
    public long skip(long n) throws IOException {
      if (n < 0) {
        throw new IllegalArgumentException();
      }
      if (remaining >= n) {
        while (n > 0) {
          long skipped = stream.skip(n);
          if (skipped == 0) {
            throw new EOFException();
          }
          remaining -= skipped;
          n -= skipped;
        }
        return n;
      }
      throw new EOFException();
    }

    @Override
    public int available() throws IOException {
      int available = stream.available();
      if (remaining >= Integer.MAX_VALUE) {
        return available;
      }
      return Math.min(available, (int) remaining);
    }

    @Override
    public void close() {
      try {
        stream.close();
        super.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void mark(int readlimit) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void reset() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean markSupported() {
      return false;
    }

    private void setRemaining(long remaining) {
      this.remaining = remaining;
    }

    private void skipRemaining() throws IOException {
      skip(remaining);
    }


  }

  @Override
  public String toString() {
    return "SparkeyLogIterator{" +
            "logFile=" + logFile +
            ", header=" + header +
            ", start=" + start +
            ", end=" + end +
            '}';
  }
}
