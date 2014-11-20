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
import com.blackberry.logdriver.klogger.Configuration.Source;
import com.codahale.metrics.CsvReporter;

public class KLogger
{

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
			InputStream propsIn = null;
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

		// Listen on each port
		
		List<Thread> threads = new ArrayList<Thread>();
		for (Source s : conf.getSources())
		{
			TcpListener listener = new TcpListener(conf, s);
			Thread t = new Thread(listener);
			t.start();
			threads.add(t);
		}

		for (Thread t : threads)
		{
			try
			{
				t.join();
			} 
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}
	}

}
