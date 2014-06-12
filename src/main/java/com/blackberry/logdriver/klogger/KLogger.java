package com.blackberry.logdriver.klogger;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.blackberry.logdriver.klogger.Configuration.Source;

public class KLogger {

  public static void main(String[] args) {
    // Read in properties file and configure ports and Kafka appender
    Configuration conf = null;
    try {
      InputStream propsIn = null;
      Properties props = new Properties();
      if (System.getProperty("klogger.configuration") != null) {
        propsIn = new FileInputStream(
            System.getProperty("klogger.configuration"));
        props.load(propsIn);
      } else {
        propsIn = KLogger.class.getClassLoader().getResourceAsStream(
            "klogger.properties");
        props.load(propsIn);
      }

      conf = new Configuration(props);
    } catch (Throwable t) {
      System.err.println("Error while configuring.");
      t.printStackTrace();
      System.exit(1);
    }

    // Listen on each port
    List<Thread> threads = new ArrayList<Thread>();
    for (Source s : conf.getSources()) {
      TcpListener listener = new TcpListener(conf, s.getPort(), s.getTopic());
      Thread t = new Thread(listener);
      t.start();

      threads.add(t);
    }

    for (Thread t : threads) {
      try {
        t.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

}
