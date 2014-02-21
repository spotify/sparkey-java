package com.spotify.sparkey;

import java.io.IOException;

public class SparkeyReaderClosedException extends IOException {
  public SparkeyReaderClosedException(String message) {
    super(message);
  }
}
