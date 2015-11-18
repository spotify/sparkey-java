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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

final class SingleThreadedSparkeyWriter implements SparkeyWriter {
  private final LogWriter logWriter;
  private final File logFile;
  private final File indexFile;
  private double sparsity;
  private HashType hashType;
  private boolean fsync;

  private SingleThreadedSparkeyWriter(File indexFile, LogWriter logWriter) {
    this.logFile = logWriter.getFile();
    this.indexFile = indexFile;
    this.logWriter = logWriter;
  }

  static SingleThreadedSparkeyWriter createNew(File file) throws IOException {
    return createNew(file, CompressionType.NONE, 0);
  }

  static SingleThreadedSparkeyWriter createNew(File file, CompressionType compressionType, int blockSize) throws IOException {
    File indexFile = Sparkey.getIndexFile(file);
    if (indexFile.exists()) {
      indexFile.delete();
    }
    File logFile = Sparkey.getLogFile(file);
    if (logFile.exists()) {
      logFile.delete();
    }
    LogWriter logWriter = LogWriter.createNew(logFile, compressionType, blockSize);
    return new SingleThreadedSparkeyWriter(indexFile, logWriter);
  }

  static SingleThreadedSparkeyWriter append(File file) throws IOException {
    File logFile = Sparkey.getLogFile(file);
    if (!logFile.exists()) {
      throw new FileNotFoundException("File not found: " + logFile);
    }
    LogWriter writer = LogWriter.openExisting(logFile);

    File indexFile = Sparkey.getIndexFile(file);
    return new SingleThreadedSparkeyWriter(indexFile, writer);
  }

  static SingleThreadedSparkeyWriter appendOrCreate(File file, CompressionType type, int compressionBlockSize) throws IOException {
    File indexFile = Sparkey.getIndexFile(file);
    File logFile = Sparkey.getLogFile(file);

    LogWriter logWriter;
    if (logFile.exists()) {
      logWriter = LogWriter.openExisting(logFile);
    } else {
      logWriter = LogWriter.createNew(logFile, type, compressionBlockSize);
    }
    return new SingleThreadedSparkeyWriter(indexFile, logWriter);
  }

  @Override
  public void close() throws IOException {
    logWriter.close(fsync);
  }

  @Override
  public void writeHash() throws IOException {
    flush();

    File parentFile = indexFile.getCanonicalFile().getParentFile();
    File newFile = new File(parentFile, indexFile.getName() + "-tmp" + UUID.randomUUID().toString());
    try {
      IndexHash.createNew(newFile, logFile, hashType, sparsity, fsync);
      boolean successful = newFile.renameTo(indexFile);
      if (!successful) {
        throw new IOException("Could not rename " + newFile + " to " + indexFile);
      }
    } finally {
      boolean deleted = newFile.delete();
    }
  }

  @Override
  public void writeHash(HashType hashType) throws IOException {
    setHashType(hashType);
    writeHash();
  }

  public void setFsync(boolean fsync) {
    this.fsync = fsync;
  }

  @Override
  public void setHashType(HashType hashType) {
    this.hashType = hashType;
  }

  @Override
  public void setHashSparsity(double sparsity) {
    this.sparsity = sparsity;
  }

  @Override
  public void put(String key, String value) throws IOException {
    logWriter.put(key, value);
  }

  @Override
  public void put(byte[] key, byte[] value) throws IOException {
    logWriter.put(key, value);
  }

  @Override
  public void put(byte[] key, InputStream valueStream, long valueLen) throws IOException {
    logWriter.put(key, valueStream, valueLen);
  }

  @Override
  public void delete(String key) throws IOException {
    logWriter.delete(key);
  }

  @Override
  public void delete(byte[] key) throws IOException {
    logWriter.delete(key);
  }

  @Override
  public void flush() throws IOException {
    logWriter.flush(fsync);
  }
}
