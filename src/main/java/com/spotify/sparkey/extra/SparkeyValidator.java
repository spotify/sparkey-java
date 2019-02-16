package com.spotify.sparkey.extra;

import com.spotify.sparkey.LogHeader;
import com.spotify.sparkey.Sparkey;
import com.spotify.sparkey.SparkeyLogIterator;
import com.spotify.sparkey.SparkeyReader;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class SparkeyValidator {
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Usage: sparkey-validator <input file>");
      System.exit(1);
      return;
    }

    File input = new File(args[0]);
    final File logFile = Sparkey.getLogFile(input);
    if (!logFile.exists()) {
      System.err.println(logFile.getAbsolutePath() + " does not exist");
      System.exit(1);
      return;
    }
    if (!logFile.isFile()) {
      System.err.println(logFile.getAbsolutePath() + " is not a file");
      System.exit(1);
      return;
    }

    final File indexFile = Sparkey.getIndexFile(input);
    if (!indexFile.exists()) {
      System.err.println(indexFile.getAbsolutePath() + " does not exist");
      System.exit(1);
      return;
    }
    if (!indexFile.isFile()) {
      System.err.println(indexFile.getAbsolutePath() + " is not a file");
      System.exit(1);
      return;
    }

    LogHeader logHeader = Sparkey.getLogHeader(logFile);

    final SparkeyReader reader = Sparkey.open(indexFile);
    final SparkeyReader reader2 = Sparkey.open(indexFile);

    boolean inconsistent = false;

    System.out.println("Validating log iterator.");

    final SparkeyLogIterator iterator = new SparkeyLogIterator(logFile);
    for (SparkeyReader.Entry entry : iterator) {
      switch (entry.getType()) {
        case PUT:
          validateKey(logHeader, entry);
          validateValue(logHeader, entry);

          // Just make sure this doesn't crash
          reader.getAsByteArray(entry.getKey());

          break;
        case DELETE:
          validateKey(logHeader, entry);
          checkState(0 == entry.getValueLength());

          // Just make sure this doesn't crash
          reader.getAsByteArray(entry.getKey());

          break;
        default:
          throw new RuntimeException("Unknown type: " + entry.getType());
      }
    }

    System.out.println("Validating hash iterator and random lookups.");
    for (SparkeyReader.Entry entry : reader) {
      final byte[] value = entry.getValue();

      validateKey(logHeader, entry);
      validateValue(logHeader, entry, value);


      final byte[] value2 = reader2.getAsByteArray(entry.getKey());
      if (!Arrays.equals(value, value2)) {
        System.err.println("Inconsistency for key: " + entry.getKeyAsString() + " when iterating and doing a lookup");
        inconsistent = true;
      }

    }

    System.out.println("Done!");

    if (inconsistent) {
      System.exit(1);
    }
  }

  private static void validateKey(LogHeader logHeader, SparkeyReader.Entry entry) {
    checkState(entry.getKeyLength() <= logHeader.getMaxKeyLen());
    checkState(entry.getKeyLength() == entry.getKey().length);
  }

  private static void validateValue(LogHeader logHeader, SparkeyReader.Entry entry) throws IOException {
    final byte[] value = entry.getValue();
    validateValue(logHeader, entry, value);
  }

  private static void validateValue(LogHeader logHeader, SparkeyReader.Entry entry, byte[] value) {
    checkState(entry.getValueLength() <= logHeader.getMaxValueLen());
    checkState(entry.getValueLength() == value.length);
  }

  private static void checkState(boolean b) {
    if (!b) {
      throw new IllegalStateException();
    }
  }


}
