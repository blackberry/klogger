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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PortSource extends Source {

	private static final Logger LOG = LoggerFactory.getLogger(PortSource.class);

	private int port;
	private InetAddress bindAddr;
	private int listenBacklog;
	private int tcpReceiveBufferBytes = 1048576;

	public PortSource(String port, String topic) {
		super(topic);
		this.port = Integer.parseInt(port);
	}

	@Override
	public Runnable getListener() {
		return new TcpListener(this);
	}

	@Override
	public void configure(Properties props) throws ConfigurationException, Exception {
		super.configure(props);
		try {
			tcpReceiveBufferBytes = Integer.parseInt(props.getProperty(
				 "tcp.receive.buffer.bytes", Integer.toString(tcpReceiveBufferBytes)));
			String bindProperty = String.format("server.listen.address.%s", this.getTopic());
			if (props.containsKey(bindProperty)) {
				bindAddr = InetAddress.getByName(props.getProperty(bindProperty).trim());
			} else {
				bindAddr = InetAddress.getByName(props.getProperty(
					 "server.listen.address", "127.0.0.1").trim());
			}
			String backlogProperty = String.format("server.listen.backlog.%s", this.getTopic());
			if (props.containsKey(backlogProperty)) {
				listenBacklog = Integer.parseInt(props.getProperty(backlogProperty).trim());
			} else {
				listenBacklog = Integer.parseInt(props.getProperty(
					 "server.listen.backlog", "50").trim());
			}
			LOG.info("Configured source port for {}:{} (with backlog of {}",
				 bindAddr.getHostAddress(),
				 port,
				 listenBacklog);
		} catch (NumberFormatException | UnknownHostException e) {
			LOG.error("Failed to configure source port for topic {}", e);
			throw new ConfigurationException("failed to configure source port", e);
		}
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public String toString() {
		return "PortSource: topic=" + this.getTopic() + ", port=" + this.getPort();
	}

	/**
	 * @return the tcpReceiveBufferBytes
	 */
	public int getTcpReceiveBufferBytes() {
		return tcpReceiveBufferBytes;
	}

	/**
	 * @param tcpReceiveBufferBytes the tcpReceiveBufferBytes to set
	 */
	public void setTcpReceiveBufferBytes(int tcpReceiveBufferBytes) {
		this.tcpReceiveBufferBytes = tcpReceiveBufferBytes;
	}

	/**
	 * @return the bindAddr
	 */
	public InetAddress getBindAddr() {
		return bindAddr;
	}

	/**
	 * @return the listenBacklog
	 */
	public int getListenBacklog() {
		return listenBacklog;
	}

}
