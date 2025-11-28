package com.spotify.sparkey;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * This code was taken from
 * https://stackoverflow.com/questions/2972986/how-to-unmap-a-file-from-memory-mapped-using-filechannel-in-java/19447758#19447758
 */
class ByteBufferCleaner {

  private static final Cleaner CLEANER = findCleaner();

  private static Cleaner findCleaner() {
    // JavaSpecVer: 1.6, 1.7, 1.8, 9-18, 19+
    try {
      String javaVersion = System.getProperty("java.specification.version", "99");
      if (javaVersion.startsWith("1.")) {
        // Java 8 and earlier
        return new ByteBufferCleaner.OldCleaner();
      } else {
        int version = Integer.parseInt(javaVersion);
        if (version >= 19) {
          // Java 19+: sun.misc.Unsafe.invokeCleaner is deprecated
          // Try jdk.internal.misc.Unsafe.invokeCleaner or fall back to no-op
          return new Java19Cleaner();
        } else {
          // Java 9-18: Use sun.misc.Unsafe.invokeCleaner
          return new NewCleaner();
        }
      }
    } catch(Exception e) {
      throw new Error(e);
    }
  }

  public static void cleanMapping(final MappedByteBuffer buffer) {
    CLEANER.clean(buffer);
  }

  /**
   * Clean an array of MappedByteBuffers, with optional sleep for multi-threaded scenarios.
   * On Java 19+, this is a no-op (no sleep, no clean).
   * On Java 8-18, sleeps if needed to allow other threads to see null assignments, then cleans.
   *
   * @param chunks the array of buffers to clean
   * @param otherRefsExist true if other threads might still hold references to the buffers
   */
  public static void cleanChunks(MappedByteBuffer[] chunks, boolean otherRefsExist) {
    if (!CLEANER.needsClean()) {
      // Java 19+: No cleaning needed, so no sleep needed either
      return;
    }

    // Java 8-18: Need to clean, so sleep first if other references exist
    if (otherRefsExist) {
      try {
        // Wait for other threads to see that chunks are null before cleaning
        // If we clean too early, the JVM can crash
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    // Clean all buffers
    for (MappedByteBuffer chunk : chunks) {
      CLEANER.clean(chunk);
    }
  }

  private interface Cleaner {
    void clean(MappedByteBuffer byteBuffer);
    boolean needsClean();
  }

  private static class OldCleaner implements Cleaner {

    @Override
    public void clean(final MappedByteBuffer byteBuffer) {
      try {
        final Class<? extends MappedByteBuffer> clazz = byteBuffer.getClass();
        final Method getCleanerMethod = clazz.getMethod("cleaner");
        getCleanerMethod.setAccessible(true);
        final Object cleaner = getCleanerMethod.invoke(byteBuffer);
        if (cleaner != null) {
          cleaner.getClass().getMethod("clean").invoke(cleaner);
        }
      } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean needsClean() {
      return true;
    }
  }

  private static class NewCleaner implements Cleaner {

    private final Method clean;
    private final Object theUnsafe;

    private NewCleaner() throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException, NoSuchMethodException {
      Class<?> unsafeClass = getUnsafeClass();
      clean = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
      clean.setAccessible(true);
      Field theUnsafeField = unsafeClass.getDeclaredField("theUnsafe");
      theUnsafeField.setAccessible(true);
      theUnsafe = theUnsafeField.get(null);
    }

    private static Class<?> getUnsafeClass() throws ClassNotFoundException {
      try {
        return Class.forName("sun.misc.Unsafe");
      } catch(Exception ex) {
        // jdk.internal.misc.Unsafe doesn't yet have an invokeCleaner() method,
        // but that method should be added if sun.misc.Unsafe is removed.
        return Class.forName("jdk.internal.misc.Unsafe");
      }
    }

    @Override
    public void clean(MappedByteBuffer byteBuffer) {
      try {
        clean.invoke(theUnsafe, byteBuffer);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean needsClean() {
      return true;
    }
  }

  /**
   * Java 19+ cleaner that avoids deprecated sun.misc.Unsafe.invokeCleaner.
   * Since Java 9+ automatically unmaps buffers when GC'd via the Cleaner API,
   * manual cleaning is no longer necessary. This is a no-op implementation.
   *
   * Note: jdk.internal.misc.Unsafe exists but is not exported from java.base,
   * so it cannot be accessed via reflection without --add-exports JVM flag.
   */
  private static class Java19Cleaner implements Cleaner {

    private Java19Cleaner() {
      // No initialization needed for no-op cleaner
    }

    @Override
    public void clean(MappedByteBuffer byteBuffer) {
      // No-op: Java 9+ automatically unmaps buffers when GC'd
      // The buffer will be cleaned up by the JVM's internal Cleaner mechanism
      // This avoids the deprecated sun.misc.Unsafe.invokeCleaner warning
    }

    @Override
    public boolean needsClean() {
      // Java 19+ doesn't need manual cleanup - it happens automatically
      return false;
    }
  }
}
