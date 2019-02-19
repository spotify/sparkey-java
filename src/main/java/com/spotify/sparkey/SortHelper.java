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

import static com.spotify.sparkey.IndexHash.calcEntryBlockBits;

import com.fasterxml.sort.DataReader;
import com.fasterxml.sort.DataReaderFactory;
import com.fasterxml.sort.DataWriter;
import com.fasterxml.sort.DataWriterFactory;
import com.fasterxml.sort.SortConfig;
import com.fasterxml.sort.Sorter;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Comparator;
import java.util.Iterator;

final class SortHelper {

  static final int ENTRY_SIZE = 40;

  static final Comparator<Entry> ENTRY_COMPARATOR = Comparator.comparingLong(SortHelper.Entry::getWantedSlot).thenComparingLong(SortHelper.Entry::getAddress);

  private static final EntryDataWriterFactory ENTRY_DATA_WRITER_FACTORY = new EntryDataWriterFactory();
  private static final int BUFFER_SIZE = 64 * 1024;

  static Iterator<SortHelper.Entry> sort(final File logFile, final long start, final long end,
                                         final HashType hashData, final long hashCapacity, final int hashSeed,
                                         final long maxMemory) throws IOException {
    SortConfig config = new SortConfig();
    if (maxMemory > 0) {
      config = config.withMaxMemoryUsage(maxMemory);
    }
    final EntryDataReaderFactory readerFactory = new EntryDataReaderFactory(hashCapacity);
    Sorter<SortHelper.Entry>
        sorter = new Sorter<>(config, readerFactory, ENTRY_DATA_WRITER_FACTORY, ENTRY_COMPARATOR);

    return sorter.sort(new LogFileEntryReader(logFile, start, end, hashData, hashCapacity, hashSeed));
  }

  private static class EntryDataReader extends DataReader<Entry> {

    private final DataInputStream dataInputStream;
    private final long hashCapacity;

    EntryDataReader(final DataInputStream dataInputStream, final long hashCapacity) {
      this.dataInputStream = dataInputStream;
      this.hashCapacity = hashCapacity;
    }

    @Override
    public Entry readNext() throws IOException {
      try {
        return Entry.fromHash(dataInputStream.readLong(), dataInputStream.readLong(), hashCapacity);
      } catch (EOFException e) {
        return null;
      }
    }

    @Override
    public int estimateSizeInBytes(final Entry entry) {
      return ENTRY_SIZE;
    }

    @Override
    public void close() throws IOException {
      dataInputStream.close();
    }
  }

  private static class EntryDataWriter extends DataWriter<Entry> {

    private final DataOutputStream dataOutputStream;

    EntryDataWriter(final DataOutputStream dataOutputStream) {this.dataOutputStream = dataOutputStream;}

    @Override
    public void writeEntry(final Entry entry) throws IOException {
      dataOutputStream.writeLong(entry.hash);
      dataOutputStream.writeLong(entry.address);
    }

    @Override
    public void close() throws IOException {
      dataOutputStream.close();
    }
  }

  private static class EntryDataWriterFactory extends DataWriterFactory<Entry> {

    @Override
    public DataWriter<Entry> constructWriter(final OutputStream outputStream) {
      return new EntryDataWriter(new DataOutputStream(new BufferedOutputStream(outputStream, BUFFER_SIZE)));
    }
  }

  private static class EntryDataReaderFactory extends DataReaderFactory<Entry> {

    private final long hashCapacity;

    private EntryDataReaderFactory(final long hashCapacity) {
      this.hashCapacity = hashCapacity;
    }

    @Override
    public DataReader<Entry> constructReader(final InputStream inputStream) {
      return new EntryDataReader(new DataInputStream(new BufferedInputStream(inputStream, BUFFER_SIZE)), hashCapacity);
    }
  }

  private static class LogFileEntryReader extends DataReader<Entry> {

    final Iterator<SparkeyReader.Entry> iterator;
    private final HashType hashData;
    private final long hashCapacity;
    private final int maxEntriesPerBlock;
    private final int entryBlockBits;
    private final int hashSeed;

    public LogFileEntryReader(final File logFile, final long start, final long end, final HashType hashData,
                              final long hashCapacity, final int hashSeed) throws IOException {
      this.hashData = hashData;
      this.hashCapacity = hashCapacity;
      this.hashSeed = hashSeed;
      SparkeyLogIterator entries = new SparkeyLogIterator(logFile, start, end);
      LogHeader header = entries.header;
      maxEntriesPerBlock = header.getMaxEntriesPerBlock();
      entryBlockBits = calcEntryBlockBits(maxEntriesPerBlock);
      iterator = entries.iterator();
    }

    @Override
    public Entry readNext() {
      if (!iterator.hasNext()) {
        return null;
      }
      // Safe cast, since the iterator is known to be a SparkeyLogIterator
      SparkeyLogIterator.Entry entry = (SparkeyLogIterator.Entry) iterator.next();

      int typeBit = entry.getType() == SparkeyReader.Type.DELETE ? 0 : 1;

      long hash = hashData.hash(entry.getKeyLength(), entry.getKey(), hashSeed);
      long position = entry.getPosition();
      long entryIndex = entry.getEntryIndex();
      long address = position << (entryBlockBits + 1) | (entryIndex << 1) | typeBit;

      if (position < 0) {
        throw new RuntimeException("Data size overflow");
      }
      return Entry.fromHash(hash, address, hashCapacity);
    }

    @Override
    public int estimateSizeInBytes(final Entry o) {
      return ENTRY_SIZE;
    }

    @Override
    public void close() throws IOException {
    }
  }

  final static class Entry {
    final long hash;
    final long address;
    final long wantedSlot;

    Entry(final long hash, final long address, final long wantedSlot) {
      this.hash = hash;
      this.address = address;
      this.wantedSlot = wantedSlot;
    }

    static Entry fromHash(final long hash, final long address, final long hashCapacity) {
      return new Entry(hash, address, IndexHash.getWantedSlot(hash, hashCapacity));
    }

    @Override
    public String toString() {
      return "Entry{" +
             "hash=" + hash +
             ", address=" + address +
             '}';
    }

    public long getHash() {
      return hash;
    }

    public long getAddress() {
      return address;
    }

    public long getWantedSlot() {
      return wantedSlot;
    }
  }
}
