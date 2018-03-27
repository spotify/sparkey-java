package com.spotify.sparkey;

import java.io.IOException;

public interface ReadWriteData extends RandomAccessData {

  void close() throws IOException;

  void writeUnsignedByte(int value) throws IOException;
}
