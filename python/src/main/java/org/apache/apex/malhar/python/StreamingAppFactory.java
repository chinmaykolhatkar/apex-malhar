package org.apache.apex.malhar.python;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.client.StramAppLauncher;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;

public class StreamingAppFactory implements StramAppLauncher.AppFactory
{

  private final StreamingApplication app;
  private final String name;

  public StreamingAppFactory(StreamingApplication app, String name)
  {
    this.app = app;
    this.name = name;
  }

  @Override
  public LogicalPlan createApp(LogicalPlanConfiguration planConfig)
  {
    LogicalPlan dag = new LogicalPlan();
    planConfig.prepareDAG(dag, app, getName());
    return dag;
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public String getDisplayName()
  {
    return name;
  }
}
