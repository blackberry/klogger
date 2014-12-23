/**
 * Copyright 2014 BlackBerry, Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

package com.blackberry.logdriver.klogger;

import java.util.Properties;

public class PortSource extends Source
{
	private int port;
	private  int tcpReceiveBufferBytes = 1048576;
	
	public PortSource(String port, String topic)
	{
		super(topic);		
		this.port = Integer.parseInt(port);
	}
	
	@Override
	public Runnable getListener()
	{
		return new TcpListener(this);
	}
	
	@Override
	public void configure(Properties props) throws ConfigurationException, Exception
	{
		super.configure(props);
		
		tcpReceiveBufferBytes = Integer.parseInt(props.getProperty("tcp.receive.buffer.bytes", Integer.toString(tcpReceiveBufferBytes)));		
	}

	public int getPort()
	{
		return port;
	}

	public void setPort(int port)
	{
		this.port = port;
	}
	
	@Override
	public String toString()
	{
		return "PortSource: topic=" + this.getTopic() + ", port=" + this.getPort();
	}

	/**
	 * @return the tcpReceiveBufferBytes
	 */
	public int getTcpReceiveBufferBytes()
	{
		return tcpReceiveBufferBytes;
	}

	/**
	 * @param tcpReceiveBufferBytes the tcpReceiveBufferBytes to set
	 */
	public void setTcpReceiveBufferBytes(int tcpReceiveBufferBytes)
	{
		this.tcpReceiveBufferBytes = tcpReceiveBufferBytes;
	}

}


























