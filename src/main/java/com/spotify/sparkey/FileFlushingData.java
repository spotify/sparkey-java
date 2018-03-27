package com.spotify.sparkey;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

class FileFlushingData extends InMemoryData {

  private final File file;
  private final IndexHeader header;
  private final boolean fsync;

  FileFlushingData(final long size, final File file, final IndexHeader header, final boolean fsync) {
    super(size);
    this.file = file;
    this.header = header;
    this.fsync = fsync;
  }

  @Override
  public void close() throws IOException {
    FileOutputStream stream = new FileOutputStream(file);
    try {
      stream.write(header.asBytes());
      for (byte[] chunk : chunks) {
        stream.write(chunk);
      }
      stream.flush(); // Not needed for FileOutputStream, but still semantically correct
      if (fsync) {
        stream.getFD().sync();
      }
    } finally {
      super.close();
      stream.close();
    }
  }
}
