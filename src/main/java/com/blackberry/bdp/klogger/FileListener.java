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

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;

import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileListener implements Runnable
{
	private static final Logger LOG = LoggerFactory.getLogger(TcpListener.class);

	private final Configuration conf;
	private final FileSource source;

	public FileListener(Configuration conf, FileSource source)
	{
		this.conf = conf;
		this.source = source;
	}

	private void chill(long ms)
	{
		try
		{
			Thread.sleep(ms);
		} 
		catch (InterruptedException ie)
		{
			LOG.warn("interuped while sleeping: {}", ie);
		}
	}
	
	private Thread startNewThread(FileLogReader logReader) throws Exception
	{		
		logReader.setFinished(false);
		Thread thread = new Thread(logReader);
		thread.start();
		return thread;
	}
	
	@Override
	public void run()
	{
		try
		{
			FileSystem fs = source.getParentDirectoryPath().getFileSystem();
			LOG.info("File system object instantiated for parent dir path: {}", source.getParentDirectoryPath());
			WatchService service = fs.newWatchService();
			WatchKey key;

			source.getParentDirectoryPath().register(service, ENTRY_CREATE, ENTRY_DELETE);

			LOG.info("Registered the watch service against the path {}", source.getParentDirectoryPath());
			
			FileLogReader logReader = new FileLogReader(source);
			
			Thread thread = null;
			
			if (source.getFile().exists())
			{
				LOG.info("Starting FileLogReader thread for file {}", source.getFile());				
				thread = startNewThread(logReader);
			}
			
			while(true) 
			{
				key = service.take();

				Kind<?> kind;

				for(WatchEvent<?> watchEvent : key.pollEvents()) 
				{
					kind = watchEvent.kind();

					if (OVERFLOW == kind) 
					{
						continue;
					} 
					else if (ENTRY_CREATE == kind) 
					{
						Path path = ((WatchEvent<Path>) watchEvent).context();
						
						if (!source.pathMatches(path))
						{
							continue;
						}
						
						LOG.info("The path that we're watching has been created: {}", path);						
						
						chill(100);
						
						if (thread != null && thread.isAlive())
						{
							LOG.error("After watching {} file was created but the FileLogReader thread is still running", source.getFile());
							logReader.setFinished(true);
							while(thread.isAlive()) { }							
							chill(100);
						}						
						
						source.setPosition(0);
						thread = startNewThread(logReader);
						
						LOG.info("File {} was created and the LogReaderThread has been started", source.getFile());
					}
					else if (ENTRY_DELETE == kind) 
					{
						Path path = ((WatchEvent<Path>) watchEvent).context();
						
						if (!source.pathMatches(path))
						{							
							continue;
						}
						
						LOG.info("Our path was deleted and we need to stop our thread: {}", path);
						
						logReader.setFinished(true);
						
						while(thread.isAlive()) { }
						
						logReader.getSource().setPosition(0);
						logReader.persistPosition();
						
						LOG.info("Resetting the position of file {} to zero", logReader.getSource());
					}
				}

				if(!key.reset()) 
				{
					break; //loop
				}
			}
		}
		catch(Exception e) 
		{
			LOG.error("Error in listening for file {}, error: {}", source.getFile(), e.getStackTrace());
			System.exit(1);
		} 
	}
}