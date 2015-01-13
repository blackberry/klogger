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

import java.io.File;

import java.util.Properties;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSource extends Source
{
	private static final Logger LOG = LoggerFactory.getLogger(FileSource.class);
	private File file;
	private long positionPersistMs = 1000;
	private long positionPersistLines = 1000;
	public long position;
	
	public FileSource(String path, String topic)
	{
		super(topic);
		this.file = new File(path);
	}
	
	@Override
	public Runnable getListener()
	{
		return new FileListener(getConf(), this);
	}
	
	public Path getParentDirectoryPath()
	{
		return Paths.get(getFile().getAbsoluteFile().getParentFile().getAbsolutePath());
	}

	public Boolean pathMatches(Path otherPath)
	{
		String myFile = getFile().getName();
		String otherFile = otherPath.getFileName().toString();
			 
		if (myFile.equals(otherFile))
		{
			return true;
		}
		
		return false;
	}
	
	@Override
	public void configure(Properties props) throws ConfigurationException, Exception
	{
		super.configure(props);
		
		String propPositionPersistLines = this.getConf().getTopicAwarePropName("file.position.persist.lines");
		String propPositionPersistMs = this.getConf().getTopicAwarePropName("file.position.persist.ms");
		
		positionPersistLines = Long.parseLong(props.getProperty(propPositionPersistLines, Long.toString(positionPersistLines)));		
		positionPersistMs = Long.parseLong(props.getProperty(propPositionPersistMs, Long.toString(positionPersistMs)));		
	}
	
	public File getFile()
	{
		return file;
	}

	public void setFile(File file)
	{
		this.file = file;
	}
	
	@Override
	public String toString()
	{
		return "FileSource: topic=" + this.getTopic() + ", path=" + this.getFile();
	}

	/**
	 * @return the positionPersistMs
	 */
	public long getPositionPersistMs()
	{
		return positionPersistMs;
	}

	/**
	 * @param positionPersistMs the positionPersistMs to set
	 */
	public void setPositionPersistMs(long positionPersistMs)
	{
		this.positionPersistMs = positionPersistMs;
	}

	/**
	 * @return the positionPersistLines
	 */
	public long getPositionPersistLines()
	{
		return positionPersistLines;
	}

	/**
	 * @param positionPersistLines the positionPersistLines to set
	 */
	public void setPositionPersistLines(long positionPersistLines)
	{
		this.positionPersistLines = positionPersistLines;
	}

	/**
	 * @return the position
	 */
	public long getPosition()
	{
		return position;
	}

	/**
	 * @param position the position to set
	 */
	public void setPosition(long position)
	{
		this.position = position;
	}
	
}