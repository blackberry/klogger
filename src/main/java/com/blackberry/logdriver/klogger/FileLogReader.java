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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blackberry.krackle.MetricRegistrySingleton;
import com.blackberry.krackle.producer.Producer;
import com.codahale.metrics.Meter;

import java.io.FileInputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;


public class FileLogReader implements Runnable
{
	private static final Logger LOG = LoggerFactory.getLogger(ServerSocketLogReader.class);
	private static final Object producersLock = new Object();
	private static final Map<String, Producer> producers = new HashMap<>();
	private final int maxLine;
	private final Producer producer;	
	private final FileSource source;
	private final boolean encodeTimestamp;
	private final boolean validateUTF8;
	private final Meter mBytesReceived;
	private final Meter mBytesReceivedTotal;
	private final Meter mLinesReceived;
	private final Meter mLinesReceivedTotal;
	
	private Boolean finished = false;
	
	public void setFinished(Boolean state) 
	{
		this.finished = state;
	}
	
	public FileLogReader(FileSource source) throws Exception
	{
		this.source = source;		
				
		LOG.info("Created new {} for connection {}", this.getClass().getName(), source);
		
		maxLine = source.getConf().getMaxLineLength();
		encodeTimestamp = source.getConf().isEncodeTimestamp();
		validateUTF8 = source.getConf().isValidateUtf8();

		String clientId = source.getConf().getClientId();
		String key = source.getConf().getKafkaKey();		
		String topic = source.getTopic();		
		
		MetricRegistrySingleton.getInstance().enableJmx();

		synchronized (producersLock)
		{
			String mapKey = clientId + "::" + topic + "::" + key;
			
			if (producers.containsKey(mapKey))
			{
				producer = producers.get(mapKey);
			} 
			else
			{
				producer = new Producer(source.getConf(), clientId, source.getTopic(), key, MetricRegistrySingleton.getInstance().getMetricsRegistry());				
				producers.put(mapKey, producer);
			}
		}

		mBytesReceived = MetricRegistrySingleton.getInstance().getMetricsRegistry().meter("klogger:topics:" + topic + ":bytes received");
		mBytesReceivedTotal = MetricRegistrySingleton.getInstance().getMetricsRegistry().meter("klogger:total:bytes received");
		mLinesReceived = MetricRegistrySingleton.getInstance().getMetricsRegistry().meter("klogger:topics:" + topic + ":lines received");
		mLinesReceivedTotal = MetricRegistrySingleton.getInstance().getMetricsRegistry().meter("klogger:total:lines received");
	}
		
	@Override
	public void run()
	{
		UTF8Validator utf8Validator = null;
		
		if (validateUTF8)
		{
			utf8Validator = new UTF8Validator();
		}

		byte[] bytes = new byte[maxLine];
		
		ByteBuffer buffer = ByteBuffer.wrap(bytes);

		// Calculate send buffer size. If we're validating UTF-8, then theoretically, each byte could be
		// replaced by the three byte replacement character. So the send buffer needs to be triple 
		// the max line length.  If we're encoding the timestamp, then that adds 10 bytes.
		
		byte[] sendBytes;
		{
			int sendBufferSize = maxLine;
			
			if (validateUTF8)
			{
				sendBufferSize *= 3;
			}
			if (encodeTimestamp)
			{
				sendBufferSize += 10;
			}
			
			sendBytes = new byte[sendBufferSize];
		}
		
		ByteBuffer sendBuffer = ByteBuffer.wrap(sendBytes);

		int start = 0;
		int limit;
		int newline;
		int bytesRead;

		try
		{
			LOG.info("Instantiating InputStream for {}", source.getFile());
			
			FileInputStream in = new FileInputStream(source.getFile());			
			FileChannel channel = in.getChannel();
			Path p = Paths.get(source.getFile().toURI());
			BasicFileAttributes bfa = Files.readAttributes(p, BasicFileAttributes.class);
			
			if (bfa.isRegularFile())
			{
				LOG.info("Setting intitial positon of regular file {} to {}", source.getFile(), source.getPosition());
				channel.position(source.getPosition());
			}
			else
			{
				LOG.info("Not setting initial positon of non-regular file {}", source.getFile());
			}
			
			while (!finished)
			{
				buffer.position(start);
				bytesRead = channel.read(buffer);
				
				if (bfa.isRegularFile() && channel.size() < source.getPosition())
				{
					LOG.warn("Truncated regular file {} detected, size is {} last position was {} -- resetting to positon zero",  source.getFile(), channel.size(), source.getPosition());
					channel.position(0);
					source.setPosition(0);
				}								

				if (bytesRead == -1)
				{
					continue;
				}

				LOG.trace("Read {} bytes", bytesRead);
				
				mBytesReceived.mark(bytesRead);
				mBytesReceivedTotal.mark(bytesRead);

				if (bfa.isRegularFile())
				{
					LOG.trace("Position in file is now: {}", channel.position());
					source.setPosition(channel.position());
				}
				
				LOG.trace("Position in buffer is now: {}", buffer.position());								

				limit = start + bytesRead;
				start = 0;

				while (true)
				{
					newline = -1;					
					for (int i = start; i < limit; i++)
					{						
						if (bytes[i] == '\n')
						{
							newline = i;							
							break;
						}
					}
					
					LOG.trace("Newline at {}", newline);
					
					if (newline >= 0)
					{
						mLinesReceived.mark();
						mLinesReceivedTotal.mark();

						LOG.trace("Sending (pos {}, len {}):{}", start, newline - start, new String(bytes, start, newline - start, "UTF-8"));
						
						sendBuffer.clear();

						if (encodeTimestamp)
						{
							sendBuffer.put(new byte[]
							{
								(byte) 0xFE, 0x00
							});
							
							sendBuffer.putLong(System.currentTimeMillis());
						}

						if (validateUTF8)
						{
							utf8Validator.validate(bytes, start, newline - start);
							sendBuffer.put(utf8Validator.getResultBytes(), 0, utf8Validator.getResultBuffer().limit());
						} 
						else
						{
							sendBuffer.put(bytes, start, newline - start);
						}

						producer.send(sendBytes, 0, sendBuffer.position());

						start = newline + 1;
						continue;
						
					} 
					else
					{
						LOG.trace("No newline.  start={}, limit={}", start, limit);
						
						// if the buffer is full, send it all. Otherwise, do nothing.
						
						if (start == 0 && limit == maxLine)
						{
							mLinesReceived.mark();
							mLinesReceivedTotal.mark();

							LOG.trace("Sending log with no new-line:{}", new String(bytes, 0, maxLine, "UTF-8"));
							
							sendBuffer.clear();

							if (encodeTimestamp)
							{
								sendBuffer.put(new byte[]
								{
									(byte) 0xFE, 0x00
								});
								sendBuffer.putLong(System.currentTimeMillis());
							}

							if (validateUTF8)
							{
								utf8Validator.validate(bytes, 0, maxLine);
								sendBuffer.put(utf8Validator.getResultBytes(), 0, utf8Validator.getResultBuffer().limit());
							} 
							else
							{
								sendBuffer.put(bytes, 0, maxLine);
							}

							producer.send(sendBytes, 0, sendBuffer.position());

							start = 0;
							break;
							
						} // if there is still data, then shift it to the start
						else
						{
							if (start > 0 && start < limit)
							{
								int toMove = limit - start;
								int moveSize;
								int done = 0;
								
								while (done < toMove)
								{
									moveSize = Math.min(start - done, limit - start);

									System.arraycopy(bytes, start, bytes, done, moveSize);
									
									done += moveSize;
									start += moveSize;
								}

								start = toMove;
								break;								
							}
							else
							{
								if (start >= limit)
								{
									LOG.trace("All the data has been read");
									start = 0;
									break;
								} 
								else
								{
									// start == 0, so move it to the limit for the next pass.
									start = limit;
									break;
								}
							}
						}
					}
				}

			}
		} 		
		catch (Throwable t)
		{
			LOG.error("An error has occured: {}", t);
			
			t.printStackTrace();
		} 
	
		LOG.info("And we're done here: {}", source.getFile());
	}

}