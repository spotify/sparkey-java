package com.spotify.sparkey;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

class FileReadWriteData implements ReadWriteData {

  private final RandomAccessFile file;
  private final IndexHeader header;
  private final boolean fsync;
  private final int offset;

  FileReadWriteData(final long size, final File file, final IndexHeader header, final boolean fsync) throws IOException {
    offset = header.size();
    this.file = new RandomAccessFile(file, "rw");
    this.file.setLength(offset + size);
    this.header = header;
    this.fsync = fsync;
  }

  @Override
  public void close() throws IOException {
    file.seek(0);
    file.write(header.asBytes());
    if (fsync) {
      file.getFD().sync();
    }
    file.close();
  }

  @Override
  public void writeUnsignedByte(final int value) throws IOException {
    file.writeByte(value);
  }

  @Override
  public void seek(final long pos) throws IOException {
    file.seek(offset + pos);
  }

  @Override
  public int readUnsignedByte() throws IOException {
    return file.readUnsignedByte();
  }

  @Override
  public int readLittleEndianInt() throws IOException {
    // RandomAccessFile uses big-endian so this needs to be reversed
    return Integer.reverseBytes(file.readInt());
  }

  @Override
  public long readLittleEndianLong() throws IOException {
    // RandomAccessFile uses big-endian so this needs to be reversed
    return Long.reverseBytes(file.readLong());
  }
}
