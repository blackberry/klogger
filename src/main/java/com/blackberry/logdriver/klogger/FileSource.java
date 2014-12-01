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
import java.nio.file.Path;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSource extends Source
{
	private File file;
	private static final Logger LOG = LoggerFactory.getLogger(FileSource.class);
	
	public FileSource(String path, String topic, Boolean quickRotate, long quickRotateMessageBlocks)
	{
		super(topic, quickRotate, quickRotateMessageBlocks);
		this.file = new File(path);
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
	
}