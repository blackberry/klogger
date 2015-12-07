/**
 * Copyright 2014 BlackBerry, Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.blackberry.bdp.klogger;

import com.blackberry.bdp.common.props.Parser;
import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.blackberry.bdp.krackle.producer.ProducerConfiguration;

/**
 * Class for configuring KLogger.
 *
 * It extends ProducerConfiguration from Krackle, so any property that is valid there is also valid here.
 *
 * <p><b>NOTE:</b> These configuration properties can not be overwritten to be topic specific they are global for all sources</p>
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
 * <td>client.id</td>
 * <td>InetAddress.getLocalHost().getHostName()</td>
 * <td>The client ID to send with requests to the broker</td>
 * </tr>
 *
 * <tr>
 * <td>kafka.key</td>
 * <td>InetAddress.getLocalHost().getHostName()</td>
 * <td>The key to use when partitioning all data topics sent from this instance</td>
 * </tr>
 *
 * <tr>
 * <td>max.line.length</td>
 * <td>64 * 1024</td>
 * <td>The maximum length of a log line to send to Kafka</td>
 * </tr>
 *
 * <tr>
 * <td>encode.timestamp</td>
 * <td>true</td>
 * <td>Whether or not to encode the timestamp in front of the log line</td>
 * </tr>
 *
 * <tr>
 * <td>validate.utf8</td>
 * <td>true</td>
 * <td>If this is set to true, then all incoming log lines will be validated to ensure that they are correctly encoded in UTF-8. Invalid bytes will be replaced by the replacement character (U+FFFD)</td>
 * </tr>
 *
 * </table>
 *
 */
public class Configuration extends ProducerConfiguration {

	// These are the original configuration attributes from verson 0.6.4

	private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);
	private String clientId = InetAddress.getLocalHost().getHostName();
	private String kafkaKey = InetAddress.getLocalHost().getHostName();
	private int maxLineLength = 64 * 1024;
	private boolean encodeTimestamp = true;
	private boolean validateUtf8 = true;

	public Configuration(Properties props, String topicName) throws Exception {
		super(props, topicName);

		Parser parser = new Parser(props);

		clientId = parser.parseString("client.id", clientId);
		kafkaKey = parser.parseString("kafka.key", kafkaKey);
		maxLineLength = Integer.parseInt(props.getProperty("max.line.length", Integer.toString(maxLineLength)).trim());
		encodeTimestamp = Boolean.parseBoolean(props.getProperty("encode.timestamp", Boolean.toString(encodeTimestamp)).trim());
		validateUtf8 = Boolean.parseBoolean(props.getProperty("validate.utf8", Boolean.toString(validateUtf8)).trim());

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
	public static ArrayList<Source> getSources(Properties props) throws ConfigurationException, Exception {
		ArrayList<Source> sources = new ArrayList<>();
		Set<String> sourceList = props.stringPropertyNames();

		for (String curElement : sourceList) {
			Pattern pattern = Pattern.compile("^source\\.([^\\.]+)\\.(port|file)$", Pattern.CASE_INSENSITIVE);
			Matcher matcher = pattern.matcher(curElement);

			if (matcher.find()) {
				String topic = matcher.group(1);
				String sourceType = matcher.group(2);
				String sourceValue = props.getProperty("source." + topic + "." + sourceType);

				if (sourceType.equals("port")) {
					PortSource source = new PortSource(sourceValue, topic);
					sources.add(source);
					LOG.info("    Adding a new port source: {} ", source);
				} else {
					if (sourceType.equals("file")) {
						File file = new File(sourceValue);

						if (file.isDirectory()) {
							LOG.warn("Skippping file based source for {} because it is a directory", sourceValue);
							continue;
						}

						File parentDirectory = new File(file.getAbsoluteFile().getParentFile().getAbsolutePath());

						if (!parentDirectory.exists()) {
							LOG.warn("Skippping file based source for {} because parent directory {} does not exist", sourceValue, parentDirectory);
							continue;
						}

						if (!file.exists()) {
							LOG.warn("Specified file {} does not exist (although the parrent directory does) will be watched for creation", file);
						}

						FileSource source = new FileSource(sourceValue, topic);
						sources.add(source);
						LOG.info("    Adding a new file source: {} ", source);
					}
				}
			}
		}

		// Now configure each of the sources
		for (Source source : sources) {
			LOG.info("Configuring {}", source);
			source.configure(props);
		}

		return sources;
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public String getKafkaKey() {
		return kafkaKey;
	}

	public void setKafkaKey(String kafkaKey) {
		this.kafkaKey = kafkaKey;
	}

	public int getMaxLineLength() {
		return maxLineLength;
	}

	public void setMaxLineLength(int maxLineLength) {
		this.maxLineLength = maxLineLength;
	}

	public boolean isEncodeTimestamp() {
		return encodeTimestamp;
	}

	public void setEncodeTimestamp(boolean encodeTimestamp) {
		this.encodeTimestamp = encodeTimestamp;
	}

	public boolean isValidateUtf8() {
		return validateUtf8;
	}

	public void setValidateUtf8(boolean validateUtf8) {
		this.validateUtf8 = validateUtf8;
	}

}
