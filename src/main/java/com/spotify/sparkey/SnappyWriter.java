package com.spotify.sparkey;

import java.io.IOException;
import java.io.InputStream;

public class SnappyWriter implements BlockOutput {
  public static final SnappyWriter DUMMY = new SnappyWriter();

  private final byte[] buf = new byte[1024*1024];
  private final SnappyOutputStream snappyOutputStream;

  private int currentNumEntries;
  private int maxEntriesPerBlock;
  private boolean flushed;
  private final int maxBlockSize;

  // Only used to initialize dummy
  private SnappyWriter() {
    snappyOutputStream = null;
    maxBlockSize = 0;
  }

  public SnappyWriter(SnappyOutputStream snappyOutputStream, int maxEntriesPerBlock) {
    this.snappyOutputStream = snappyOutputStream;
    this.maxEntriesPerBlock = maxEntriesPerBlock;
    snappyOutputStream.setListener(this);
    maxBlockSize = this.snappyOutputStream.getMaxBlockSize();
  }

  public void afterFlush() {
    maxEntriesPerBlock = Math.max(currentNumEntries, maxEntriesPerBlock);
    currentNumEntries = 0;
    flushed = true;
  }

  @Override
  public void flush() throws IOException {
    snappyOutputStream.flush();
  }

  @Override
  public long put(byte[] key, int keyLen, byte[] value, int valueLen) throws IOException {
    int keySize = Util.unsignedVLQSize(keyLen + 1) + Util.unsignedVLQSize(valueLen);
    int totalSize = keySize + keyLen + valueLen;

    smartFlush(keySize, totalSize);
    flushed = false;
    currentNumEntries++;

    long pre = snappyOutputStream.getCount();

    Util.writeUnsignedVLQ(keyLen + 1, snappyOutputStream);
    Util.writeUnsignedVLQ(valueLen, snappyOutputStream);
    snappyOutputStream.write(key, 0, keyLen);
    snappyOutputStream.write(value, 0, valueLen);

    // Make sure that the beginning of each block is the start of a key/value pair
    if (flushed && snappyOutputStream.getPending() > 0) {
      snappyOutputStream.flush();
    }

    return snappyOutputStream.getCount() - pre;
  }

  @Override
  public long put(byte[] key, int keyLen, InputStream value, long valueLen) throws IOException {
    int keySize = Util.unsignedVLQSize(keyLen + 1) + Util.unsignedVLQSize(valueLen);
    long totalSize = keySize + keyLen + valueLen;

    smartFlush(keySize, totalSize);
    flushed = false;
    currentNumEntries++;

    long pre = snappyOutputStream.getCount();

    Util.writeUnsignedVLQ(keyLen + 1, snappyOutputStream);
    Util.writeUnsignedVLQ(valueLen, snappyOutputStream);
    snappyOutputStream.write(key, 0, keyLen);
    Util.copy(valueLen, value, snappyOutputStream, buf);

    // Make sure that the beginning of each block is the start of a key/value pair
    if (flushed && snappyOutputStream.getPending() > 0) {
      snappyOutputStream.flush();
    }

    return snappyOutputStream.getCount() - pre;
  }

  private void smartFlush(int keySize, long totalSize) throws IOException {
    int remaining = snappyOutputStream.remaining();
    if (remaining < keySize) {
      flush();
    } else if (remaining < totalSize && totalSize < maxBlockSize - remaining) {
      flush();
    }
  }

  @Override
  public long delete(byte[] key, int keyLen) throws IOException {
    int keySize = 1 + Util.unsignedVLQSize(keyLen + 1);
    smartFlush(keySize, keySize + keyLen);

    flushed = false;
    currentNumEntries++;

    long pre = snappyOutputStream.getCount();

    snappyOutputStream.write(0);
    Util.writeUnsignedVLQ(keyLen, snappyOutputStream);
    snappyOutputStream.write(key, 0, keyLen);

    // Make sure that the beginning of each block is the start of a key/value pair
    if (flushed && snappyOutputStream.getPending() > 0) {
      snappyOutputStream.flush();
    }

    return snappyOutputStream.getCount() - pre;
  }

  @Override
  public void close() throws IOException {
    flush();
    snappyOutputStream.close();
  }

  public int getMaxEntriesPerBlock() {
    return maxEntriesPerBlock;
  }
}
