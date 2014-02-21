package com.spotify.sparkey;

import java.io.IOException;

public class CorruptedIndexException extends IOException {
  public CorruptedIndexException(String message) {
    super(message);
  }
}
