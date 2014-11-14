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
