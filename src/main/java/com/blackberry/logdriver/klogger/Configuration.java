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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.blackberry.common.props.Parser;
import com.blackberry.krackle.producer.ProducerConfiguration;


/**
 * Class for configuring KLogger.
 *
 * It extends ProducerConfiguration from Krackle, so any property that is valid there is also valid here.
 *
 * Previously existing HTML Javadoc describing the additional properties has been removed.  Will be added back in
 * after a review of the delta between these and the default Krackle properties
 *
 */

public class Configuration extends ProducerConfiguration
{
	// These are the original configuration attributes from verson 0.6.4
	private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);	
	private String clientId = InetAddress.getLocalHost().getHostName();
	private String kafkaKey = InetAddress.getLocalHost().getHostName();	
	private int maxLineLength = 64 * 1024;
	private boolean encodeTimestamp = true;
	private boolean validateUtf8 = true;
	
	public Configuration(Properties props, String topicName) throws Exception
	{
		super(props, topicName);
		
		Parser parser = new Parser(props);

		clientId = parser.parseString("client.id", clientId);
		kafkaKey = parser.parseString("kafka.key", kafkaKey);
		maxLineLength = Integer.parseInt(props.getProperty("max.line.length", Integer.toString(maxLineLength)).trim());
		encodeTimestamp = Boolean.parseBoolean(props.getProperty("encode.timestamp", Boolean.toString(encodeTimestamp)).trim());
		validateUtf8 = Boolean.parseBoolean(props.getProperty("validate.utf8", Boolean.toString(validateUtf8)).trim());
				
		// So this topic is not configured to use the shared buffers, so require explicit buffer properties
		
		LOG.info("client.id = {}", clientId);
		LOG.info("kafka.key = {}", kafkaKey);		
		LOG.info("max.line.length = {}", maxLineLength);
		LOG.info("encode.timestamp = {}", encodeTimestamp);
		LOG.info("validate.utf8 = {}", validateUtf8);				
	}	
	
	/**
	 * Returns the sources defined in the properties
	 * @param props
	 * @return
	 * @throws ConfigurationException
	 */
	public static ArrayList<Source> getSources(Properties props) throws ConfigurationException, Exception
	{
		ArrayList<Source> sources = new ArrayList<>();
		Set<String> sourceList = props.stringPropertyNames();

		for (String curElement : sourceList)
		{
			Pattern pattern = Pattern.compile("^source\\.([^\\.]+)\\.(port|file)$", Pattern.CASE_INSENSITIVE);
			Matcher matcher = pattern.matcher(curElement);
			
			if (matcher.find())
			{				
				String topic = matcher.group(1);
				String sourceType = matcher.group(2);
				String sourceValue = props.getProperty("source." + topic + "." + sourceType);				
				
				if (sourceType.equals("port"))
				{					
					PortSource source = new PortSource(sourceValue, topic);
					sources.add(source);
					LOG.info("    Adding a new port source: {} ", source);
				}
				else if (sourceType.equals("file"))
				{
					File file = new File(sourceValue);
					
					if (file.isDirectory())
					{
						LOG.warn("Skippping file based source for {} because it is a directory", sourceValue);
						continue;
					}
					
					File parentDirectory = new File(file.getAbsoluteFile().getParentFile().getAbsolutePath());
					
					if (!parentDirectory.exists())
					{
						LOG.warn("Skippping file based source for {} because parent directory {} does not exist", sourceValue, parentDirectory);
						continue;
					}
					
					if (!file.exists())
					{
						LOG.warn("Specified file {} does not exist (although the parrent directory does) will be watched for creation", file);
					}
					
					FileSource source = new FileSource(sourceValue, topic);
					sources.add(source);
					LOG.info("    Adding a new file source: {} ", source);						
				}				
			}			
		}
		
		for  (Source source : sources)
		{
			LOG.info("Configuring {}", source);
			source.configure(props);
		}

		
		return sources;
	}
	
	public String getClientId()
	{
		return clientId;
	}

	public void setClientId(String clientId)
	{
		this.clientId = clientId;
	}

	public String getKafkaKey()
	{
		return kafkaKey;
	}

	public void setKafkaKey(String kafkaKey)
	{
		this.kafkaKey = kafkaKey;
	}

	public int getMaxLineLength()
	{
		return maxLineLength;
	}

	public void setMaxLineLength(int maxLineLength)
	{
		this.maxLineLength = maxLineLength;
	}

	public boolean isEncodeTimestamp()
	{
		return encodeTimestamp;
	}

	public void setEncodeTimestamp(boolean encodeTimestamp)
	{
		this.encodeTimestamp = encodeTimestamp;
	}

	public boolean isValidateUtf8()
	{
		return validateUtf8;
	}

	public void setValidateUtf8(boolean validateUtf8)
	{
		this.validateUtf8 = validateUtf8;
	}	
}

