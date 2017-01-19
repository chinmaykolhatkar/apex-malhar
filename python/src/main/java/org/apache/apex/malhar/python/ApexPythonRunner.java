package org.apache.apex.malhar.python;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.client.api.YarnClient;
import py4j.GatewayServer;

import java.util.LinkedList;
import java.util.List;

public class ApexPythonRunner
{
  private List<StreamingApp> contexts = new LinkedList<>();
  private Configuration conf = new Configuration(true);
  private static GatewayServer server;

  public StreamingApp newStreamingApp(String name)
  {
    StreamingApp streamingApp = new StreamingApp(name, conf);
    contexts.add(streamingApp);
    return streamingApp;
  }

  public static void main(String[] args) throws Exception
  {
    ApexPythonRunner apexPythonRunner = new ApexPythonRunner();
    server = new GatewayServer(apexPythonRunner);
    server.start();
    System.out.println("Gateway Server started");
  }
}
