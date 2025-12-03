/*
 * Copyright (c) 2025 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.spotify.sparkey.system;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;

/**
 * Wrapper for native mlock() syscall using Foreign Function & Memory API.
 * Allows locking memory-mapped pages in RAM to prevent page faults during benchmarking.
 */
public class MemoryLock {

  private static final Linker LINKER = Linker.nativeLinker();
  private static final MethodHandle MLOCK;
  private static final MethodHandle MUNLOCK;

  static {
    // Look up native mlock and munlock functions
    // int mlock(const void *addr, size_t len);
    // int munlock(const void *addr, size_t len);

    SymbolLookup stdlib = LINKER.defaultLookup();

    FunctionDescriptor mlockDesc = FunctionDescriptor.of(
        ValueLayout.JAVA_INT,           // return type: int
        ValueLayout.ADDRESS,            // addr: void*
        ValueLayout.JAVA_LONG           // len: size_t
    );

    MemorySegment mlockAddr = stdlib.find("mlock")
        .orElseThrow(() -> new UnsupportedOperationException("mlock not available"));
    MemorySegment munlockAddr = stdlib.find("munlock")
        .orElseThrow(() -> new UnsupportedOperationException("munlock not available"));

    MLOCK = LINKER.downcallHandle(mlockAddr, mlockDesc);
    MUNLOCK = LINKER.downcallHandle(munlockAddr, mlockDesc);
  }

  /**
   * Lock a MemorySegment in RAM, preventing it from being paged out.
   * Per mlock(2) man page: "All pages that contain a part of the specified address
   * range are guaranteed to be resident in RAM when the call returns successfully."
   *
   * @param segment The memory segment to lock
   * @return true if successful, false if mlock failed (e.g., insufficient privileges)
   */
  public static boolean lock(MemorySegment segment) {
    try {
      int result = (int) MLOCK.invoke(segment, segment.byteSize());
      return result == 0;
    } catch (Throwable e) {
      return false;
    }
  }

  /**
   * Unlock a previously locked MemorySegment, allowing it to be paged out.
   *
   * @param segment The memory segment to unlock
   * @return true if successful, false if munlock failed
   */
  public static boolean unlock(MemorySegment segment) {
    try {
      int result = (int) MUNLOCK.invoke(segment, segment.byteSize());
      return result == 0;
    } catch (Throwable e) {
      return false;
    }
  }

  /**
   * Check if mlock is likely to work by testing current ulimit -l.
   * Returns the maximum lockable memory in bytes, or -1 if unlimited.
   */
  public static long getMaxLockedMemory() {
    try {
      // This would require another FFI call to getrlimit(RLIMIT_MEMLOCK)
      // For now, just suggest checking manually
      ProcessBuilder pb = new ProcessBuilder("sh", "-c", "ulimit -l");
      Process p = pb.start();
      byte[] output = p.getInputStream().readAllBytes();
      p.waitFor();

      String result = new String(output).trim();
      if (result.equals("unlimited")) {
        return -1;
      }

      // ulimit -l returns KB, convert to bytes
      return Long.parseLong(result) * 1024;
    } catch (Exception e) {
      System.err.println("Could not check ulimit -l: " + e.getMessage());
      return 0;
    }
  }
}
