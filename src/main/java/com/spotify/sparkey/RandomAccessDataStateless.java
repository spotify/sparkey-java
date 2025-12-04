/*
 * Copyright (c) 2011-2013 Spotify AB
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

import java.io.IOException;

/**
 * Stateless random access data interface with position-based reads.
 *
 * Unlike {@link RandomAccessData} which uses seek() followed by reads,
 * this interface passes position directly to each read method, enabling
 * truly immutable implementations without mutable position state.
 *
 * This design is inherently thread-safe and easier to reason about since
 * there's no shared mutable state.
 */
interface RandomAccessDataStateless {

  int readUnsignedByte(long pos) throws IOException;

  int readLittleEndianInt(long pos) throws IOException;

  long readLittleEndianLong(long pos) throws IOException;
}
