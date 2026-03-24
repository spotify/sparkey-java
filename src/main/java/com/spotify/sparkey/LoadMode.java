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
 * Controls which parts of a sparkey file to prefetch into the page cache.
 *
 * @see SparkeyReader#load(LoadMode)
 */
public enum LoadMode {
  /** Do not load anything. Useful to keep call sites clean without conditional logic. */
  NONE,
  /** Load the index file into the page cache. */
  INDEX,
  /** Load the log file into the page cache. */
  LOG,
  /** Load both the index and log files into the page cache. */
  ALL
}
