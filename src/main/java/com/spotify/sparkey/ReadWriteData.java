package com.spotify.sparkey;

import java.io.IOException;

public interface ReadWriteData extends RandomAccessData {

  void writeLittleEndianLong(long value) throws IOException;

  void writeLittleEndianInt(int value) throws IOException;

  void close() throws IOException;

  void writeUnsignedByte(int value) throws IOException;
}
