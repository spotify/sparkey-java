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

import com.google.common.base.Charsets;

import java.io.*;

final class LogWriter {
  private final LogHeader header;
  private final File file;
  private final BlockOutput logStream;

  private LogWriter(File file, CompressionType compressionType, int compressionBlockSize) throws IOException {
    this.file = file;
    header = new LogHeader(compressionType, compressionBlockSize);
    header.write(file);
    logStream = setup(header, file);
  }

  private LogWriter(File file) throws IOException {
    this.file = file;
    if (!file.exists()) {
      throw new FileNotFoundException(file.getCanonicalPath());
    }
    header = LogHeader.read(file);
    logStream = setup(header, file);
  }

  File getFile() {
    return file;
  }

  private static BlockOutput setup(LogHeader header, File file) throws IOException {
    truncate(file, header.getDataEnd());
    OutputStream stream = new BufferedOutputStream(new FileOutputStream(file, true), 1024 * 1024);
    return header.getCompressionType().createBlockOutput(stream, header.getCompressionBlockSize(),
            header.getMaxEntriesPerBlock());
  }

  private static void truncate(File file, long size) throws IOException {
    RandomAccessFile rw = new RandomAccessFile(file, "rw");
    try {
      rw.setLength(size);
    } finally {
      rw.close();
    }
  }

  static LogWriter createNew(File file, CompressionType compressionType, int compressionBlockSize) throws IOException {
    return new LogWriter(file, compressionType, compressionBlockSize);
  }

  static LogWriter openExisting(File file) throws IOException {
    return new LogWriter(file);
  }

  void flush() throws IOException {
    logStream.flush();
    header.setMaxEntriesPerBlock(logStream.getMaxEntriesPerBlock());
    header.setDataEnd(file.length());
    header.write(file);
  }

  void close() throws IOException {
    flush();
    logStream.close();
    header.setDataEnd(file.length());
    header.write(file);
  }

  void put(String key, String value) throws IOException {
    put(key.getBytes(Charsets.UTF_8), value.getBytes(Charsets.UTF_8));
  }

  void put(byte[] key, byte[] value) throws IOException {
    logStream.put(key, key.length, value, value.length);
    header.put(key.length, value.length);
  }

  void put(byte[] key, InputStream value, long valueLen) throws IOException {
    logStream.put(key, key.length, value, valueLen);
    header.put(key.length, valueLen);
  }

  void delete(String key) throws IOException {
    delete(key.getBytes(Charsets.UTF_8));
  }

  void delete(byte[] key) throws IOException {
    if (key.length <= header.getMaxKeyLen()) {
      logStream.delete(key, key.length);
      header.delete(key.length);
    }
  }

}
