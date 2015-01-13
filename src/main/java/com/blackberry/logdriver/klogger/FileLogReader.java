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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;

public class FileLogReader extends  LogReader
{
	private static final Logger LOG = LoggerFactory.getLogger(ServerSocketLogReader.class);
	private final FileSource source;	
	private FileInputStream in;
	private FileChannel channel = in.getChannel();	
	private BasicFileAttributes bfa;	
	private final ByteBuffer buffer;

	public FileLogReader(FileSource source) throws Exception
	{		
		super(source.getConf());
		
		this.source = source;
		
		buffer = ByteBuffer.wrap(bytes);
	}

	@Override
	protected void prepareSource() throws FileNotFoundException, IOException
	{
		LOG.info("Instantiating InputStream for {}", source.getFile());

		in = new FileInputStream(source.getFile());			
		channel = in.getChannel();
		Path p = Paths.get(source.getFile().toURI());
		bfa = Files.readAttributes(p, BasicFileAttributes.class);

		if (bfa.isRegularFile())
		{
			LOG.info("Setting intitial positon of regular file {} to {}", source.getFile(), source.getPosition());
			channel.position(source.getPosition());
		}
		else
		{
			LOG.info("Not setting initial positon of non-regular file {}", source.getFile());
		}
	}
	
	@Override
	protected int readSource() throws IOException
	{
		buffer.position(start);
		int bytesRead = channel.read(buffer);

		if (bfa.isRegularFile() && channel.size() < source.getPosition())
		{
			LOG.warn("Truncated regular file {} detected, size is {} last position was {} -- resetting to positon zero",  source.getFile(), channel.size(), source.getPosition());
			channel.position(0);
			source.setPosition(0);
		}								

		if (bfa.isRegularFile())
		{
			LOG.trace("Position in file is now: {}", channel.position());
			source.setPosition(channel.position());
		}

		LOG.trace("Position in buffer is now: {}", buffer.position());										
		
		return bytesRead;
	}
	
	@Override 
	protected void finished()
	{
		LOG.info("Finished reading source {}", source);
	}
}