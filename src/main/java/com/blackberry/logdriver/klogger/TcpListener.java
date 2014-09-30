package com.blackberry.logdriver.klogger;

import java.net.ServerSocket;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TcpListener implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(TcpListener.class);

  private Configuration conf;
  private int port;
  private String topic;
  private boolean quickRotate;
  private long quickRotateMessageBlocks;

  public TcpListener(Configuration conf, int port, String topic) {
	  this(conf, port, topic, false, 0);
  }
  
  public TcpListener(Configuration conf, int port, String topic, boolean quickRotate, long quickRotateMessageBlocks) {
    this.conf = conf;
    this.port = port;
    this.topic = topic;
    this.quickRotate = quickRotate;
    this.quickRotateMessageBlocks = quickRotateMessageBlocks;
  }

  @Override
  public void run() {
    try {
      ServerSocket ss = new ServerSocket(port);
      ss.setReceiveBufferSize(conf.getTcpReceiveBufferBytes());
      LOG.info("Listening on port {}", port);

      while (true) {
        Socket s = ss.accept();
        LogReader r = new LogReader(conf, s, topic, quickRotate, quickRotateMessageBlocks);
        Thread t = new Thread(r);
        t.start();
      }
    } catch (Throwable t) {
      t.printStackTrace();
      System.exit(1);
    }

  }
}
