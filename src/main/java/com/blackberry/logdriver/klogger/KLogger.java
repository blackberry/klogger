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
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.blackberry.krackle.MetricRegistrySingleton;
import com.codahale.metrics.CsvReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KLogger
{
	private static final Logger LOG = LoggerFactory.getLogger(KLogger.class);
	public static void main(String[] args)
	{
		// Check to see if we want to use CSV metric logging
		
		if (System.getProperty("metrics.log.dir") != null)
		{
			String metricsLogDir = System.getProperty("metrics.log.dir");

			CsvReporter reporter = CsvReporter
				.forRegistry(
					MetricRegistrySingleton.getInstance().getMetricsRegistry())
				.formatFor(Locale.US).convertRatesTo(TimeUnit.SECONDS)
				.convertDurationsTo(TimeUnit.MILLISECONDS)
				.build(new File(metricsLogDir));
			reporter.start(60, TimeUnit.SECONDS);
		}

		// Check to see if we need to enable console reporting
		
		if (Boolean.parseBoolean(System.getProperty("metrics.to.console", "false")))
		{
			MetricRegistrySingleton.getInstance().enableConsole();
		}

		// Read in properties file and configure ports and Kafka appender
		
		Configuration conf = null;
		try
		{
			InputStream propsIn;
			Properties props = new Properties();
			
			if (System.getProperty("klogger.configuration") != null)
			{
				propsIn = new FileInputStream(System.getProperty("klogger.configuration"));
				props.load(propsIn);
			}
			else
			{
				propsIn = KLogger.class.getClassLoader().getResourceAsStream("klogger.properties");
				props.load(propsIn);
			}

			conf = new Configuration(props);
		} 
		catch (Throwable t)
		{
			System.err.println("Error while configuring.");
			t.printStackTrace();
			System.exit(1);
		}

		/**
		 * Does this not handle recovering from any exceptions that may  
		 * be throw in either TcpListener or ServerSocketLogReader?
		 */
		
		List<Thread> threads = new ArrayList<>();
		
		/**
		 * Instantiate TcpListener's for each of the ports we've been configured for
		 * These will then instantiate ServerSocketLogReader threads themselves 
		 * that will listen on the TCP port and produce for their configured topic.
		 */
		
		if (conf.getPortSources().size() > 0)
		{
			for (PortSource s : conf.getPortSources())
			{
				TcpListener listener = new TcpListener(conf, s);
				Thread t = new Thread(listener);
				t.start();
				threads.add(t);
			}
		}
		else
		{
			System.err.println("There are no configured port based sources");
		}
		
		/**
		 * Instantiate FileListener's for each of the paths we've been configured for
		 * These will instantiate file listeners that will monitor the parent directories
		 * for files that are modified (deleted, re-created) while we're reading them
		 */
		
		for (FileSource s : conf.getFileSources())
		{
			try
			{
				FileListener fileListener = new FileListener(conf, s);
				Thread t = new Thread(fileListener);
				t.start();
				threads.add(t);
			}
			catch (Exception e)
			{
				LOG.error("Error creating file listener thread for source file {}, error: {}", s.getFile(), e);
			}
		}

		for (Thread t : threads)
		{
			try
			{
				t.join();
			} 
			catch (InterruptedException e)
			{
				LOG.error("Thread interuped exception: {}", e);
			}
		}
	}

}
