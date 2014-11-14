/**
 * Copyright 2014 BlackBerry, Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.blackberry.logdriver.klogger;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;

public class UTF8Validator {
  private Charset UTF8;
  private CharsetDecoder decoder;
  private CharsetEncoder encoder;
  private CoderResult coderResult;

  private byte[] bytes = new byte[0];
  private ByteBuffer bb = ByteBuffer.wrap(bytes);
  private char[] outChars = new char[1];
  private CharBuffer out = CharBuffer.wrap(outChars);
  private byte[] resultBytes = new byte[1];
  private ByteBuffer resultBuffer = ByteBuffer.wrap(resultBytes);

  public UTF8Validator() {
    UTF8 = Charset.forName("UTF-8");
    decoder = UTF8.newDecoder();
    encoder = UTF8.newEncoder();
    decoder.onMalformedInput(CodingErrorAction.REPLACE);
    decoder.replaceWith("\uFFFD");
  }

  public void validate(byte[] bytesIn, int offset, int length) {
    if (bytes.length < length) {
      bytes = new byte[length];
      bb = ByteBuffer.wrap(bytes);
    }
    System.arraycopy(bytesIn, offset, bytes, 0, length);
    bb.clear();
    bb.limit(length);

    out.clear();
    coderResult = decoder.decode(bb, out, true);
    while (coderResult == CoderResult.OVERFLOW) {
      bb.rewind();
      outChars = new char[2 * outChars.length];
      out = CharBuffer.wrap(outChars);
      out.clear();
      coderResult = decoder.decode(bb, out, true);
    }
    out.flip();

    resultBuffer.clear();
    coderResult = encoder.encode(out, resultBuffer, true);
    while (coderResult == CoderResult.OVERFLOW) {
      out.rewind();
      resultBytes = new byte[2 * resultBytes.length];
      resultBuffer = ByteBuffer.wrap(resultBytes);
      resultBuffer.clear();
      coderResult = encoder.encode(out, resultBuffer, true);
    }

    resultBuffer.flip();
  }

  public byte[] getResultBytes() {
    return resultBytes;
  }

  public ByteBuffer getResultBuffer() {
    return resultBuffer;
  }

}
