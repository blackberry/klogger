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

import java.io.IOException;
import java.io.InputStream;

import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerSocketLogReader extends LogReader {

	private static final Logger LOG = LoggerFactory.getLogger(ServerSocketLogReader.class);

	private InputStream in;
	private final Socket socket;

	public ServerSocketLogReader(PortSource source, Socket s) throws Exception {
		super(source.getConf());
		LOG.info("Created new {} for connection {}", this.getClass().getName(), s.getRemoteSocketAddress());
		socket = s;
	}

	@Override
	protected void prepareSource() throws Exception {
		in = socket.getInputStream();
	}

	@Override
	protected int readSource() throws Exception {
		bytesRead = in.read(bytes, start, maxLine - start);

		return bytesRead;
	}

	@Override
	protected void finished() {
		try {
			socket.close();
			// Producers never close. The number of producers is fairly small, so this shouldn't be an issue.  We may need to fix that at some point.
			// producer.close();
		} catch (IOException e) {
			LOG.error("Error trying to close socket: ", e);
		}
	}

}
