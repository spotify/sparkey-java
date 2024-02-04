/*
 * Copyright (c) 2020 Spotify AB
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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.concurrent.CompletionStage;

public interface AsyncSparkeyReader extends Closeable {
  /**
   * @param key the key to search for, interpreted as an UTF-8 string.
   * @return null if the key/value pair was not found, otherwise the value interpreted as an UTF-8 string.
   * @throws java.util.concurrent.RejectedExecutionException if the reader is closed
   */
  CompletionStage<String> getAsString(String key);

  /**
   * @param key the key to search for
   * @return null if the key/value pair was not found, otherwise the raw byte array value
   * @throws java.util.concurrent.RejectedExecutionException if the reader is closed
   */
  CompletionStage<byte[]> getAsByteArray(byte[] key);

  // Override to remove exception
  @Override
  void close();

}
