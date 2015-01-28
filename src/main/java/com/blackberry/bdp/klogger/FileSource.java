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

import java.util.Properties;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuring File Sources.
 *
 * The same properties object for Klogger is used to configure file sources.
 * 
 * <p><b>NOTE:</b> Every single one of these properties can be overwritten for a specific topic by using the following property patten:</p>
 * 
 * <p>source.&lt;<i>topic</i>&gt.&lt;<i>property</i>&gt</p>
 * 
 * Valid properties are:
 * <br />
 * <table border="1">
 *
 * <tr>
 * <th>property</th>
 * <th>default</th>
 * <th>description</th>
 * </tr>
 *
 * <tr>
 * <td>file.position.persist.lines</td>
 * <td>1000</td>
 * <td>How many lines to read from before persisting the file position in the cache</td>
 * </tr>
 * 
 * <tr>
 * <td>file.position.persist.ms</td>
 * <td>1000()</td>
 * <td>How long to wait between calls  to cache the file position (milliseconds)</td>
 * </tr>
 *
 * <tr>
 * <td>file.stream.end.read.delay.ms</td>
 * <td>500</td>
 * <td>How long to wait after reaching the end of a file before another read is attempted (milliseconds)</td>
 * </tr>
 * 
 * <tr>
 * <td>file.positions.persist.cache.dir</td>
 * <td>/opt/klogger/file_positions_cache</td>
 * <td>The directory to persist the positions of files in</td>
 * </tr>
 *
 * </table>
 *
 */
public class FileSource extends Source
{
	private static final Logger LOG = LoggerFactory.getLogger(FileSource.class);
	private File file;
	private long positionPersistMs = 1000;
	private long positionPersistLines = 1000;
	private long position;
	private String positonPersistCacheDir = "/opt/klogger/file_positions_cache";
	private long fileEndReadDelayMs = 500;
	
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
		String propFileEndReadDelayMs = this.getConf().getTopicAwarePropName("file.stream.end.read.delay.ms");
		String propPositonPersistCacheDir = "file.positions.persist.cache.dir";
		
		positionPersistLines = Long.parseLong(props.getProperty(propPositionPersistLines, Long.toString(positionPersistLines)));		
		positionPersistMs = Long.parseLong(props.getProperty(propPositionPersistMs, Long.toString(positionPersistMs)));
		positonPersistCacheDir = props.getProperty(propPositonPersistCacheDir, positonPersistCacheDir);
		fileEndReadDelayMs = Long.parseLong(props.getProperty(propFileEndReadDelayMs, Long.toString(fileEndReadDelayMs)));
		
		LOG.info("{} configured {} = {}", this, propPositionPersistLines, positionPersistLines);
		LOG.info("{} configured {} = {}", this, propPositionPersistMs, positionPersistMs);
		LOG.info("{} configured {} = {}", this, propPositonPersistCacheDir, positonPersistCacheDir);
		LOG.info("{} configured {} = {}", this, propFileEndReadDelayMs, fileEndReadDelayMs);
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

	/**
	 * @return the positonPersistCacheDir
	 */
	public String getPositonPersistCacheDir()
	{
		return positonPersistCacheDir;
	}

	/**
	 * @param positonPersistCacheDir the positonPersistCacheDir to set
	 */
	public void setPositonPersistCacheDir(String positonPersistCacheDir)
	{
		this.positonPersistCacheDir = positonPersistCacheDir;
	}

	/**
	 * @return the fileEndReadDelayMs
	 */
	public long getFileEndReadDelayMs()
	{
		return fileEndReadDelayMs;
	}

	/**
	 * @param fileEndReadDelayMs the fileEndReadDelayMs to set
	 */
	public void setFileEndReadDelayMs(long fileEndReadDelayMs)
	{
		this.fileEndReadDelayMs = fileEndReadDelayMs;
	}
	
}