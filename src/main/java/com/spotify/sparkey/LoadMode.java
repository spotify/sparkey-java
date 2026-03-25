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

/**
 * Controls which parts of a sparkey file to prefetch into the page cache,
 * and whether to pin pages with mlock.
 *
 * <p>The basic modes ({@link #INDEX}, {@link #LOG}, {@link #ALL}) perform advisory
 * prefetching — the OS may evict pages under memory pressure.
 *
 * <p>The MLOCK modes ({@link #INDEX_MLOCK}, {@link #LOG_MLOCK}, {@link #ALL_MLOCK})
 * additionally attempt to lock pages in RAM using {@code mlock(2)}, preventing
 * eviction. This requires Java 22+ and sufficient OS privileges
 * ({@code CAP_IPC_LOCK} or adequate {@code ulimit -l}). If mlock is unavailable
 * or fails, these modes fall back to advisory prefetching silently.
 * Use {@link LoadResult#locked()} to check whether mlock succeeded.
 *
 * @see SparkeyReader#load(LoadMode)
 */
public enum LoadMode {
  /** Do not load anything. Useful to keep call sites clean without conditional logic. */
  NONE(Action.NONE, Action.NONE),
  /** Load the index file into the page cache. */
  INDEX(Action.LOAD, Action.NONE),
  /** Load the log file into the page cache. */
  LOG(Action.NONE, Action.LOAD),
  /** Load both the index and log files into the page cache. */
  ALL(Action.LOAD, Action.LOAD),
  /** Load and mlock the index file. Falls back to advisory load if mlock is unavailable. */
  INDEX_MLOCK(Action.MLOCK, Action.NONE),
  /** Load and mlock the log file. Falls back to advisory load if mlock is unavailable. */
  LOG_MLOCK(Action.NONE, Action.MLOCK),
  /** Load and mlock both files. Falls back to advisory load if mlock is unavailable. */
  ALL_MLOCK(Action.MLOCK, Action.MLOCK);

  enum Action { NONE, LOAD, MLOCK }

  private final Action indexAction;
  private final Action logAction;

  LoadMode(Action indexAction, Action logAction) {
    this.indexAction = indexAction;
    this.logAction = logAction;
  }

  /** The action to perform on the index file. */
  Action indexAction() {
    return indexAction;
  }

  /** The action to perform on the log file. */
  Action logAction() {
    return logAction;
  }
}
