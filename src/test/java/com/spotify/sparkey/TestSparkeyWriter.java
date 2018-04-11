package com.spotify.sparkey;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;

public class TestSparkeyWriter {

  public static void writeHashAndCompare(final SparkeyWriter writer2) throws IOException {
    final SingleThreadedSparkeyWriter writer = (SingleThreadedSparkeyWriter) writer2;

    final File indexFile = writer.indexFile;
    final File memFile = Sparkey.setEnding(indexFile, ".mem.spi");

    try {
      writer.setConstructionMethod(SparkeyWriter.ConstructionMethod.IN_MEMORY);
      writer.writeHash();
      indexFile.renameTo(memFile);
      final IndexHeader memHeader = IndexHeader.read(memFile);

      writer.setHashSeed(memHeader.getHashSeed());

      writer.setConstructionMethod(SparkeyWriter.ConstructionMethod.SORTING);
      writer.writeHash();
      final IndexHeader sortHeader = IndexHeader.read(indexFile);

      if (!Files.equal(indexFile, memFile)) {
        throw new RuntimeException(
            "Files are not equal: " + indexFile + ", " + memFile + "\n" +
            sortHeader.toString() + "\n" + memHeader.toString());
      }
    } finally {
      writer.setConstructionMethod(SparkeyWriter.ConstructionMethod.AUTO);
      memFile.delete();
    }
  }
}
