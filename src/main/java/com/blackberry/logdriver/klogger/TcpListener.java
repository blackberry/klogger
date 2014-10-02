package com.blackberry.logdriver.klogger;

import java.net.ServerSocket;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blackberry.logdriver.klogger.Configuration.Source;

public class TcpListener implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(TcpListener.class);

  private Configuration conf;
  private Source source;
  
  public TcpListener(Configuration conf, Source s) {
    this.conf = conf;
    this.source = s;
  }

  @Override
  public void run() {
    try {
      ServerSocket ss = new ServerSocket(source.getPort());
      ss.setReceiveBufferSize(conf.getTcpReceiveBufferBytes());
      LOG.info("Listening on port {}", source.getPort());

      while (true) {
        Socket s = ss.accept();
        LogReader r = new LogReader(conf, source, s);
        Thread t = new Thread(r);
        t.start();
      }
    } catch (Throwable t) {
      t.printStackTrace();
      System.exit(1);
    }

  }
}
