package com.spotify.sparkey;

import java.io.IOException;
import java.io.InputStream;

class CompressedWriter implements BlockOutput {
  public static final CompressedWriter DUMMY = new CompressedWriter();

  private final byte[] buf = new byte[1024*1024];
  private final CompressedOutputStream compressedOutputStream;

  private int currentNumEntries;
  private int maxEntriesPerBlock;
  private boolean flushed;
  private final int maxBlockSize;

  // Only used to initialize dummy
  private CompressedWriter() {
    compressedOutputStream = null;
    maxBlockSize = 0;
  }

  public CompressedWriter(CompressedOutputStream compressedOutputStream, int maxEntriesPerBlock) {
    this.compressedOutputStream = compressedOutputStream;
    this.maxEntriesPerBlock = maxEntriesPerBlock;
    compressedOutputStream.setListener(this);
    maxBlockSize = this.compressedOutputStream.getMaxBlockSize();
  }

  public void afterFlush() {
    maxEntriesPerBlock = Math.max(currentNumEntries, maxEntriesPerBlock);
    currentNumEntries = 0;
    flushed = true;
  }

  @Override
  public void flush(boolean fsync) throws IOException {
    compressedOutputStream.flush();
    if (fsync) {
      compressedOutputStream.fsync();
    }
  }

  @Override
  public void put(byte[] key, int keyLen, byte[] value, int valueLen) throws IOException {
    int keySize = Util.unsignedVLQSize(keyLen + 1) + Util.unsignedVLQSize(valueLen);
    int totalSize = keySize + keyLen + valueLen;

    smartFlush(keySize, totalSize);
    flushed = false;
    currentNumEntries++;

    Util.writeUnsignedVLQ(keyLen + 1, compressedOutputStream);
    Util.writeUnsignedVLQ(valueLen, compressedOutputStream);
    compressedOutputStream.write(key, 0, keyLen);
    compressedOutputStream.write(value, 0, valueLen);


    // Make sure that the beginning of each block is the start of a key/value pair
    if (flushed && compressedOutputStream.getPending() > 0) {
      compressedOutputStream.flush();
    }
  }

  @Override
  public void put(byte[] key, int keyLen, InputStream value, long valueLen) throws IOException {
    int keySize = Util.unsignedVLQSize(keyLen + 1) + Util.unsignedVLQSize(valueLen);
    long totalSize = keySize + keyLen + valueLen;

    smartFlush(keySize, totalSize);
    flushed = false;
    currentNumEntries++;

    Util.writeUnsignedVLQ(keyLen + 1, compressedOutputStream);
    Util.writeUnsignedVLQ(valueLen, compressedOutputStream);
    compressedOutputStream.write(key, 0, keyLen);
    Util.copy(valueLen, value, compressedOutputStream, buf);

    // Make sure that the beginning of each block is the start of a key/value pair
    if (flushed && compressedOutputStream.getPending() > 0) {
      compressedOutputStream.flush();
    }
  }

  private void smartFlush(int keySize, long totalSize) throws IOException {
    int remaining = compressedOutputStream.remaining();
    if (remaining < keySize) {
      flush(false);
    } else if (remaining < totalSize && totalSize < maxBlockSize - remaining) {
      flush(false);
    }
  }

  @Override
  public void delete(byte[] key, int keyLen) throws IOException {
    int keySize = 1 + Util.unsignedVLQSize(keyLen + 1);
    smartFlush(keySize, keySize + keyLen);

    flushed = false;
    currentNumEntries++;

    compressedOutputStream.write(0);
    Util.writeUnsignedVLQ(keyLen, compressedOutputStream);
    compressedOutputStream.write(key, 0, keyLen);

    // Make sure that the beginning of each block is the start of a key/value pair
    if (flushed && compressedOutputStream.getPending() > 0) {
      compressedOutputStream.flush();
    }
  }

  @Override
  public void close(boolean fsync) throws IOException {
    flush(fsync);
    compressedOutputStream.close();
  }

  public int getMaxEntriesPerBlock() {
    return maxEntriesPerBlock;
  }
}
