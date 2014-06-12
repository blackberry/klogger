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

  public TcpListener(Configuration conf, int port, String topic) {
    this.conf = conf;
    this.port = port;
    this.topic = topic;
  }

  @Override
  public void run() {
    try {
      ServerSocket ss = new ServerSocket(port);
      ss.setReceiveBufferSize(conf.getTcpReceiveBufferBytes());
      LOG.info("Listening on port {}", port);

      while (true) {
        Socket s = ss.accept();
        LogReader r = new LogReader(conf, s, topic);
        Thread t = new Thread(r);
        t.start();
      }
    } catch (Throwable t) {
      t.printStackTrace();
      System.exit(1);
    }

  }
}
