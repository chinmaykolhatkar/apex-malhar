package org.apache.apex.malhar.python;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.client.StramAppLauncher;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

public class StreamingApp
{
  private ApexStream apexStream;
  private String name = "testName";
  private ApplicationId appId;
  private Configuration conf;

  public StreamingApp(String name, Configuration conf)
  {
    this.name = name;
    this.conf = conf;
  }

  public StreamingApp fromFolder(String folderName)
  {
    apexStream = StreamFactory.fromFolder(folderName);
    return this;
  }

  public StreamingApp printStream()
  {
    apexStream.print();
    return this;
  }

  public String launchDAG() throws Exception
  {
    ApexPythonApplication app = new ApexPythonApplication(apexStream);

    StramAppLauncher appLauncher = new StramAppLauncher(name, conf);
    appLauncher.loadDependencies();
    StreamingAppFactory appFactory = new StreamingAppFactory(app, name);
    this.appId = appLauncher.launchApp(appFactory);

    return appId.toString();
  }

  public void kill() throws IOException, YarnException
  {
    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(this.conf);
    yarnClient.start();
    yarnClient.killApplication(appId);
    yarnClient.stop();
  }

  public static class ApexPythonApplication implements StreamingApplication
  {
    private final ApexStream apexStream;

    public ApexPythonApplication(ApexStream stream)
    {
      this.apexStream = stream;
    }

    @Override
    public void populateDAG(DAG dag, Configuration configuration)
    {
      apexStream.populateDag(dag);
    }
  }
}
