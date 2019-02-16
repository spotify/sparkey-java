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

import com.spotify.sparkey.extra.ThreadLocalSparkeyReader;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public final class Sparkey {
  /**
   * Tracks number of memory mapped files - useful for detecting leaks
   */
  private static final AtomicInteger OPEN_MMAPS = new AtomicInteger();
  private static final AtomicInteger OPEN_FILES = new AtomicInteger();

  private Sparkey() {
  }

  /**
   * Creates a new sparkey writer without any compression.
   *
   * This is not a thread-safe class, only use it from one thread.
   *
   * @param file File base to use, the actual file endings will be set to .spi and .spl
   * @return a new writer,
   */
  public static SparkeyWriter createNew(File file) throws IOException {
    return SingleThreadedSparkeyWriter.createNew(file);
  }

  /**
   * Creates a new sparkey writer with the specified compression
   *
   * This is not a thread-safe class, only use it from one thread.
   *
   * @param file File base to use, the actual file endings will be set to .spi and .spl
   * @param compressionType
   * @param compressionBlockSize The maximum compression block size in bytes
   * @return a new writer,
   */
  public static SparkeyWriter createNew(File file, CompressionType compressionType, int compressionBlockSize) throws IOException {
    return SingleThreadedSparkeyWriter.createNew(file, compressionType, compressionBlockSize);
  }

  /**
   * Opens an existing file for append.
   *
   * This is not a thread-safe class, only use it from one thread.
   *
   * @param file File base to use, the actual file endings will be set to .spi and .spl
   * @return a new writer,
   */
  public static SparkeyWriter append(File file) throws IOException {
    return SingleThreadedSparkeyWriter.append(file);
  }

  /**
   * Opens an existing file for append, or create a new one if it doesn't exist.
   * The compression settings will only apply for new files.
   *
   * This is not a thread-safe class, only use it from one thread.
   *
   * @param file File base to use, the actual file endings will be set to .spi and .spl
   * @param compressionType
   * @param compressionBlockSize The maximum compression block size in bytes
   * @return a new writer,
   */
  public static SparkeyWriter appendOrCreate(File file, CompressionType compressionType, int compressionBlockSize) throws IOException {
    return SingleThreadedSparkeyWriter.appendOrCreate(file, compressionType, compressionBlockSize);
  }

  /**
   * Open a new, thread-safe, sparkey reader
   *
   *
   * @param file File base to use, the actual file endings will be set to .spi and .spl
   * @return a new reader,
   */
  public static SparkeyReader open(File file) throws IOException {
    return new ThreadLocalSparkeyReader(file);
  }

  /**
   * Open a new, thread-safe, sparkey reader
   *
   *
   * @param file File base to use, the actual file endings will be set to .spi and .spl
   * @return a new reader,
   */
  public static SparkeyReader openThreadLocalReader(File file) throws IOException {
    return new ThreadLocalSparkeyReader(file);
  }

  /**
   * Open a new sparkey reader
   *
   * This is not a thread-safe class, only use it from one thread.
   *
   * @param file File base to use, the actual file endings will be set to .spi and .spl
   * @return a new reader,
   */
  public static SparkeyReader openSingleThreadedReader(File file) throws IOException {
    return SingleThreadedSparkeyReader.open(file);
  }

  /**
   * Write (or rewrite) the index file for a given sparkey file.
   *
   * @param file File base to use, the actual file endings will be set to .spi and .spl
   */
  public static void writeHash(File file) throws IOException {
    SparkeyWriter writer = append(file);
    writer.writeHash();
    writer.close();
  }

  /**
   * Write (or rewrite) the index file for a given sparkey file.
   *
   * @param file File base to use, the actual file endings will be set to .spi and .spl
   * @param hashType choice of hash type, can be 32 or 64 bits.
   */
  public static void writeHash(File file, HashType hashType) throws IOException {
    SparkeyWriter writer = append(file);
    writer.writeHash(hashType);
    writer.close();
  }

  /**
   * Sets the file ending of the file to .spl (the log filename convention)
   * @param file
   * @return a file object with .spl as file ending
   */
  public static File getLogFile(File file) {
    return setEnding(file, ".spl");
  }

  /**
   * Sets the file ending of the file to .spi (the index filename convention)
   * @param file
   * @return a file object with .spi as file ending
   */
  public static File getIndexFile(File file) {
    return setEnding(file, ".spi");
  }

  static File setEnding(File file, String ending) {
    String fileName = file.getName();
    if (fileName.endsWith(ending)) {
      return file;
    }
    if (fileName.endsWith(".spi") || fileName.endsWith(".spl")) {
      return new File(file.getParentFile(), changeEnding(fileName, ending));
    }
    if (fileName.endsWith(".")) {
      return new File(file.getParentFile(), fileName.substring(0, fileName.length() - 1) + ending);
    }
    return new File(file.getParentFile(), fileName + ending);
  }

  private static String changeEnding(String s, String ending) {
    int index = s.lastIndexOf(".");
    if (index == -1) {
      return s + ending;
    }
    return s.substring(0, index) + ending;
  }


  /**
   * Extract the header information from the index file.
   * @param file
   * @return an index header
   */
  public static IndexHeader getIndexHeader(File file) throws IOException {
    return IndexHeader.read(file);
  }

  /**
   * Extract the header information from the log file.
   * @param file
   * @return an index header
   */
  public static LogHeader getLogHeader(File file) throws IOException {
    return LogHeader.read(file);
  }

  static void incrOpenMaps() {
    OPEN_MMAPS.incrementAndGet();
  }

  static void decrOpenMaps() {
    OPEN_MMAPS.decrementAndGet();
  }

  public static int getOpenMaps() {
    return OPEN_MMAPS.get();
  }

  static void incrOpenFiles() {
    OPEN_FILES.incrementAndGet();
  }

  static void decrOpenFiles() {
    OPEN_FILES.decrementAndGet();
  }

  public static int getOpenFiles() {
    return OPEN_FILES.get();
  }

}
