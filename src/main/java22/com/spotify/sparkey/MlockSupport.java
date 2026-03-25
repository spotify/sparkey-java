/*
 * Copyright (c) 2026 Spotify AB
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
package com.spotify.sparkey;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * mlock support via FFM (Foreign Function & Memory API).
 * Only available on Java 22+ and platforms that provide mlock(2).
 */
final class MlockSupport {
  private static final MethodHandle MLOCK_HANDLE = initMlock();

  private static MethodHandle initMlock() {
    try {
      Linker linker = Linker.nativeLinker();
      SymbolLookup lookup = linker.defaultLookup();
      return lookup.find("mlock")
          .map(addr -> linker.downcallHandle(addr,
              FunctionDescriptor.of(ValueLayout.JAVA_INT,
                  ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)))
          .orElse(null);
    } catch (Throwable t) {
      // FFM not available or platform doesn't support mlock
      return null;
    }
  }

  /**
   * Try to mlock the given memory segment.
   * Returns true if mlock succeeded, false if unavailable or failed.
   */
  static boolean mlock(MemorySegment segment) {
    if (MLOCK_HANDLE == null) {
      return false;
    }
    try {
      int result = (int) MLOCK_HANDLE.invokeExact(segment, segment.byteSize());
      return result == 0;
    } catch (Throwable t) {
      return false;
    }
  }
}
