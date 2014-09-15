package com.blackberry.logdriver.klogger;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blackberry.krackle.producer.ProducerConfiguration;

/**
 * Class for configuring KLogger.
 * 
 * It extends ProducerConfiguration from Krackle, so any property that is valid
 * there is also valid here.
 * 
 * In addition, the following are possible.
 * 
 * <table>
 * <tr>
 * <th>property</th>
 * <th>default</th>
 * <th>description</th>
 * </tr>
 * 
 * <tr>
 * <td>client.id</td>
 * <td>the local hostname</td>
 * <td>The client ID to send with requests to the broker.</td>
 * </tr>
 * 
 * 
 * 
 * <tr>
 * <td>kafka.key</td>
 * <td>the local hostname</td>
 * <td>The key to use when partitioning all data topics sent from this instance.
 * </td>
 * </tr>
 * 
 * <tr>
 * <td>kafka.rotate</td>
 * <td>whether to RR through partitions</td>
 * <td>Whether or not we switch to the next available partitions on each Meta-Data Refresh or not (default=false)
 * </td>
 * </tr>
  * 
 * <tr>
 * <td>tcp.receive.buffer.bytes</td>
 * <td>1024 * 1024</td>
 * <td>Suggested size of the TCP receive buffer.</td>
 * </tr>
 * 
 * <tr>
 * <td>max.line.length</td>
 * <td>64 * 1024</td>
 * <td>Maximum line length accepted. Longer lines will be split into multiple
 * lines of at most this size.</td>
 * </tr>
 * 
 * <tr>
 * <td>encode.timestamp</td>
 * <td>true</td>
 * <td>Whether or not to encode the timestamp in front of the logline, as
 * described in the <a
 * href="https://wikis.rim.net/display/bdp/Kafka+Logging+Architecture"
 * >Architecture</a></td>
 * </tr>
 * 
 * <tr>
 * <td>validate.utf8</td>
 * <td>true</td>
 * <td>If this is set to true, then all incoming log lines will be validated to
 * ensure that they are correctly encoded in UTF-8. Invalid bytes will be
 * replaced by the replacement character (U+FFFD).</td>
 * </tr>
 * 
 * <tr>
 * <td>sources</td>
 * <td></td>
 * <td>(required) A comma separated list of source names which should be active.
 * </td>
 * </tr>
 * 
 * <tr>
 * <td>source.<em>sourceName</em>.port</td>
 * <td></td>
 * <td>(required) One entry is required for each source listed in
 * <code>sources</code>.
 * <p>
 * The port to listen on for messages send via this source.</td>
 * </tr>
 * 
 * <tr>
 * <td>source.<em>sourceName</em>.topic</td>
 * <td></td>
 * <td>(required) One entry is required for each source listed in
 * <code>sources</code>.
 * <p>
 * The Kafka topic that messages sent via this <code>source</code> should be
 * sent to.</td>
 * </tr>
 * 
 * </table>
 */
public class Configuration extends ProducerConfiguration {
  private static final Logger LOG = LoggerFactory
      .getLogger(Configuration.class);

  private List<Source> sources;
  private String clientId;
  private String kafkaKey;
  private boolean rotatePartitions;
  private int tcpReceiveBufferBytes;
  private int maxLineLength;
  private boolean encodeTimestamp;
  private boolean validateUtf8;

  public Configuration(Properties props) throws Exception {
    super(props);

    if (props.containsKey("client.id")) {
      clientId = props.getProperty("client.id");
    } else {
      // Try to figure out my local hostname.
      clientId = InetAddress.getLocalHost().getHostName();
    }
    LOG.info("client.id = {}", clientId);

    if (props.containsKey("kafka.key")) {
      kafkaKey = props.getProperty("kafka.key");
    } else {
      // Try to figure out my local hostname.
      kafkaKey = InetAddress.getLocalHost().getHostName();
    }
    LOG.info("kafka.key = {}", kafkaKey);
    
    rotatePartitions = Boolean.parseBoolean(props.getProperty("kafka.rotate", "false").trim());
    LOG.info("kafka.rotate = {}", rotatePartitions);

    tcpReceiveBufferBytes = Integer.parseInt(props.getProperty(
        "tcp.receive.buffer.bytes", "" + ONE_MB));
    if (tcpReceiveBufferBytes < 1) {
      throw new Exception("tcp.receive.buffer.bytes must be positive.  Got "
          + tcpReceiveBufferBytes);
    }
    LOG.info("tcp.receive.buffer.bytes = {}", tcpReceiveBufferBytes);

    maxLineLength = Integer.parseInt(props.getProperty("max.line.length", ""
        + (64 * 1024)));
    if (maxLineLength < 1) {
      throw new Exception("max.line.length must be positive.  Got "
          + maxLineLength);
    }
    LOG.info("max.line.length = {}", maxLineLength);

    encodeTimestamp = Boolean.parseBoolean(props.getProperty(
        "encode.timestamp", "true").trim());
    LOG.info("encode.timestamp = {}", encodeTimestamp);

    validateUtf8 = Boolean.parseBoolean(props.getProperty("validate.utf8",
        "true").trim());
    LOG.info("validate.utf8 = {}", encodeTimestamp);

    LOG.info("Port to topic mappings:");
    sources = new ArrayList<Source>();
    String sourceString = props.getProperty("sources");
    if (sourceString == null || sourceString.isEmpty()) {
      throw new Exception("sources cannot be empty.");
    }
    for (String s : sourceString.split(",")) {
      Source source = new Source();
      source.setPort(Integer.parseInt(props
          .getProperty("source." + s + ".port")));
      source.setTopic(props.getProperty("source." + s + ".topic"));
      sources.add(source);
      LOG.info("    {} ==> {}", source.getPort(), source.getTopic());
    }
  }

  public List<Source> getSources() {
    return sources;
  }

  public void setSources(List<Source> sources) {
    this.sources = sources;
  }

  public String getClientId() {
    return clientId;
  }

  public void setClientId(String clientId) {
    this.clientId = clientId;
  }

  public boolean getKafkaRotatePartitions() {
  		return rotatePartitions;
  }
  
  public void setKafkaRotatePartitions(boolean rotatePartitions) {
  	this.rotatePartitions = rotatePartitions;
  }
  
  public String getKafkaKey() {
    return kafkaKey;
  }

  public void setKafkaKey(String kafkaKey) {
    this.kafkaKey = kafkaKey;
  }

  public int getTcpReceiveBufferBytes() {
    return tcpReceiveBufferBytes;
  }

  public void setTcpReceiveBufferBytes(int tcpReceiveBufferBytes) {
    this.tcpReceiveBufferBytes = tcpReceiveBufferBytes;
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

  public static class Source {
    private int port;
    private String topic;

    public int getPort() {
      return port;
    }

    public void setPort(int port) {
      this.port = port;
    }

    public String getTopic() {
      return topic;
    }

    public void setTopic(String topic) {
      this.topic = topic;
    }
  }
}
