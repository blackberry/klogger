/**
 * Copyright 2014 BlackBerry, Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

package com.blackberry.bdp.klogger;


import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;

public class FileLogReader extends  LogReader
{
	private static final Logger LOG = LoggerFactory.getLogger(ServerSocketLogReader.class);
	private final FileSource source;	
	private FileInputStream in;
	private FileChannel channel;
	private BasicFileAttributes bfa;	
	private final ByteBuffer buffer;

	private long persistLinesCounter = 0;
	private long persisMsTimestamp = 0;

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
			setFileChannelPosition(readPositionFromPersistCacheFile());
			persisMsTimestamp = System.currentTimeMillis();
		}
		else
		{
			LOG.info("Not setting initial positon of non-regular file {}", source.getFile());
		}
	}
	
	/**
	 * Reads from the file source.  
	 * Returns the number  of bytes read when there were bytes to have read
	 * Return 0 (zero) if the FileChannel has reached the end of the stream
	 * @return
	 * @throws IOException
	 */
	@Override
	protected int readSource() throws IOException
	{
		buffer.position(start);
		
		bytesRead = channel.read(buffer);
		
		if (bytesRead == -1)
		{
			try
			{
				Thread.sleep(source.getFileEndReadDelayMs());
			}
			catch (InterruptedException ie)
			{
				LOG.warn("{} Interrupted when waiting for end of file read delay", source);
			}
			
			return 0;
		}
		
		LOG.trace("Position in buffer is now: {}", buffer.position());										

		if (bfa.isRegularFile())
		{
			LOG.trace("Position in file is now: {}", channel.position());
			
			source.setPosition(channel.position());
			
			// Did our file get smaller?
			
			if (channel.size() < source.getPosition())
			{
				LOG.warn("Truncated regular file {} detected, size is {} last position was {} -- resetting to positon zero",  source.getFile(), channel.size(), source.getPosition());
				channel.position(0);
				source.setPosition(channel.size());
				persistPosition();
			}
			
			// Do we need to persist our position in the cache?
			
			if (System.currentTimeMillis()- persisMsTimestamp > source.getPositionPersistMs()
				 || totalLinesRead - persistLinesCounter > source.getPositionPersistLines())
			{
				persistLinesCounter = 0;
				persisMsTimestamp = System.currentTimeMillis();
				persistPosition();
			}
			
		}		

		return bytesRead;
	}
	
	@Override 
	protected void finished()
	{
		LOG.info("Finished reading source {}", source);
		
		if (bfa.isRegularFile())
		{
			persistPosition();
		}			
	}
	
	public File getPositionPersistCacheFile()
	{
		return new File(source.getPositonPersistCacheDir() + "/" + source.getFile().toString().replaceAll("/", "_"));
	}		 
	
	private void persistPosition()
	{
		File persistFile = getPositionPersistCacheFile();
		
		try 
		{			
			FileWriter writer = new FileWriter(persistFile, false);		
			writer.write(String.valueOf(source.getPosition()));
			writer.close();
			LOG.info("Wrote position {} to file {} for source {}", source.getPosition(), persistFile, source);
		}
		catch(IOException ioe)
		{
			LOG.error("Unable to write position {} to file {} for source {}, error: ", source.getPosition(), persistFile, source, ioe);
			
		}		
	}
	
	private void setFileChannelPosition(long position) throws IOException
	{
		LOG.info("Settting position to {} for {} ", position, source);
		channel.position(position);
	}
	
	/**
	 * Reads the position for source from the cache directory
	 * Returns the position when the file exists, otherwise 0 (zero)
	 * @return
	 * @throws IOException
	 */
	private long readPositionFromPersistCacheFile() throws IOException
	{				
		File persistFile = getPositionPersistCacheFile();
		
		if (!persistFile.exists())
		{
			return 0;
		}
		
		byte[] encoded = Files.readAllBytes(persistFile.toPath());
		Long position = Long.parseLong(new String(encoded, Charset.forName("UTF-8")));
		LOG.info("Read position {} from file {} for source {}", position, persistFile, source);
		return position;
	}
}