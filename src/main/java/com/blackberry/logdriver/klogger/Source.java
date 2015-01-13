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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Source
{
	private static final Logger LOG = LoggerFactory.getLogger(Source.class);
	private String topic;
	public Configuration conf;
	
	public Source(String topic)
	{
		this.topic = topic;
	}
	
	public abstract Runnable getListener();
	
	public void configure(Properties props) throws ConfigurationException,  Exception
	{
		setConf(new Configuration(props, topic));
	}
	
	public String getTopic()
	{
		return topic;
	}

	public void setTopic(String topic)
	{
		this.topic = topic;
	}

	/**
	 * @return the configuration
	 */
	public Configuration getConf()
	{
		return conf;
	}

	/**
	 * @param conf the configuration to set
	 */
	public void setConf(Configuration conf)
	{
		this.conf = conf;
	}

}