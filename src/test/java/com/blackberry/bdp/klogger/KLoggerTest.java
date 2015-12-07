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

import com.blackberry.bdp.klogger.KLogger;
import java.io.OutputStream;
import java.net.Socket;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.blackberry.bdp.test.utils.LocalKafkaServer;
import com.blackberry.bdp.test.utils.LocalZkServer;

public class KLoggerTest {

	private static LocalZkServer zk;
	private static LocalKafkaServer kafka;

	@BeforeClass
	public static void setup() throws Exception {
		zk = new LocalZkServer();
		kafka = new LocalKafkaServer();
		kafka.createTopic("wt-abc");

		Thread t = new Thread(new Runnable() {
			@Override
			public void run() {
				KLogger.main(new String[]{});
			}

		});
		t.start();

		Thread.sleep(1000);
	}

	@Test
	public void testGoodData() throws Exception {
		String testline = "2014-03-20T00:00:00Z myhostname This is a test.  Wheeeeeeee....... Something... Blah.\n";
		byte[] outBytes = testline.getBytes("UTF8");
		Socket s = new Socket("localhost", 10000);
		OutputStream out = s.getOutputStream();
		for (int i = 0; i < 100000; i++) {
			out.write(outBytes);
		}
	}

	@Test
	public void testNoNewLines() throws Exception {
		String testline = "2014-03-20T00:00:00Z myhostname This is a test.  Wheeeeeeee....... Something... Blah.";
		byte[] outBytes = testline.getBytes("UTF8");
		Socket s = new Socket("localhost", 10000);
		OutputStream out = s.getOutputStream();
		for (int i = 0; i < 1000; i++) {
			out.write(outBytes);
		}
	}

	@Test
	public void testPartialLines() throws Exception {
		String testline = "2014-03-20T00:00:00Z myhostname This is a test.  Wheeeeeeee....... Something... Blah.\n";
		byte[] outBytes = testline.getBytes("UTF8");
		Socket s = new Socket("localhost", 10000);
		OutputStream out = s.getOutputStream();
		for (int i = 0; i < 1000; i++) {
			out.write(outBytes, 0, 20);
			Thread.sleep(10);
			out.write(outBytes, 20, outBytes.length - 20);
		}
	}

	@AfterClass
	public static void cleanup() throws Exception {
		kafka.shutdown();
		zk.shutdown();
	}

}
