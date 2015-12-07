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
package com.blackberry.bdp.klogger;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;

public class UTF8Validator {

	private final Charset UTF8;
	private final CharsetDecoder decoder;
	private final CharsetEncoder encoder;
	private CoderResult coderResult;

	private byte[] bytes = new byte[0];
	private ByteBuffer bb = ByteBuffer.wrap(bytes);

	private char[] outChars = new char[1];
	private CharBuffer out = CharBuffer.wrap(outChars);

	private byte[] resultBytes = new byte[1];
	private ByteBuffer resultBuffer = ByteBuffer.wrap(resultBytes);

	public UTF8Validator() {
		UTF8 = Charset.forName("UTF-8");

		encoder = UTF8.newEncoder();

		decoder = UTF8.newDecoder();
		decoder.onMalformedInput(CodingErrorAction.REPLACE);
		decoder.replaceWith("\uFFFD");
	}

	/**
	 * Validates a byte[] as UTF8, starting at an offset and for a given length
	 * @param bytesIn The byte array to validate
	 * @param offset The starting offset within the byte array
	 * @param length The length from the offset to validate for
	 */
	public void validate(byte[] bytesIn, int offset, int length) {
		// Make bytes the size of length if smaller, and adjust he ByteBuffer's wrap

		if (bytes.length < length) {
			bytes = new byte[length];
			bb = ByteBuffer.wrap(bytes);
		}

		// Copy the portion to be validated from bytesIn into our bytes
		System.arraycopy(bytesIn, offset, bytes, 0, length);

		// Clear the buffer and set the length
		bb.clear();
		bb.limit(length);

		// Clear the out CharBuffer and decode the bb ByteBuffer into it
		out.clear();
		coderResult = decoder.decode(bb, out, true);

		// The out CharBuffer starts with being wrapped to char[1], increase until it's large enough (if needed)
		while (coderResult == CoderResult.OVERFLOW) {
			bb.rewind();
			outChars = new char[2 * outChars.length];
			out = CharBuffer.wrap(outChars);
			out.clear();
			coderResult = decoder.decode(bb, out, true);
		}

		// Flip the out CharBuffer we just wrote to so we can read it out again from the begginning
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

		// Flip the resultBuffer so it can be accessed from the begging the next time it's read from
		resultBuffer.flip();
	}

	public byte[] getResultBytes() {
		return resultBytes;
	}

	public ByteBuffer getResultBuffer() {
		return resultBuffer;
	}

}
