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
    // JavaSpecVer: 1.6, 1.7, 1.8, 9, 10
    try {
      if (System.getProperty("java.specification.version", "99").startsWith("1.")) {
        return new ByteBufferCleaner.OldCleaner();
      } else {
        return new NewCleaner();
      }
    } catch(Exception e) {
      throw new Error(e);
    }
  }

  public static void cleanMapping(final MappedByteBuffer buffer) {
    CLEANER.clean(buffer);
  }

  private interface Cleaner {
    void clean(MappedByteBuffer byteBuffer);
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
  }
}
