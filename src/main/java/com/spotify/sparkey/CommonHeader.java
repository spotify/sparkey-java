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

abstract class CommonHeader {
  final int majorVersion;
  final int minorVersion;
  final int fileIdentifier;

  long dataEnd;
  long maxKeyLen;
  long maxValueLen;
  long numPuts;

  CommonHeader(int majorVersion, int minorVersion, int fileIdentifier, long dataEnd, long maxKeyLen, long maxValueLen, long numPuts) throws IOException {
    this.majorVersion = majorVersion;
    this.minorVersion = minorVersion;
    this.fileIdentifier = fileIdentifier;
    this.dataEnd = dataEnd;
    this.maxKeyLen = maxKeyLen;
    this.maxValueLen = maxValueLen;
    this.numPuts = numPuts;
    if (this.maxKeyLen > Integer.MAX_VALUE || this.maxKeyLen < 0) {
      throw new IOException("Too large max key len: " + this.maxKeyLen);
    }
    if (this.maxValueLen < 0) {
      throw new IOException("Too large max value len: " + this.maxValueLen);
    }
  }

}
