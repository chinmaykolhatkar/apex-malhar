/**
 * Put your copyright and license info here.
 */
package com.datatorrent.schema;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.schema.RelativeFileSchemaRegistry;
import com.datatorrent.lib.schema.SchemaUtils;
import com.datatorrent.lib.transform.TransformOperator;

@ApplicationAnnotation(name="MyFirstApplication")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    /**
     * Setting registry schema is optional. Default schema registry is com.datatorrent.lib.schema.FileSchemaRegistry.
     * This means one could set a property "dt.stream.<streamname>.schema" to absolute path of json schema file.
     */
    RelativeFileSchemaRegistry registry = new RelativeFileSchemaRegistry();
    registry.setBasePath(Application.class.getResource(".").getPath());
    SchemaUtils.useSchemaRegistry(registry);

    RandomInputGenerator inputGenerator = dag.addOperator("input", RandomInputGenerator.class);
    TransformOperator transform = dag.addOperator("transform", TransformOperator.class);
    ConsoleOutputOperator console = dag.addOperator("console", ConsoleOutputOperator.class);

    dag.setInputPortAttribute(transform.input, Context.PortContext.TUPLE_CLASS, TestPojo.class);

    dag.addStream("S1", inputGenerator.output, transform.input);
    SchemaUtils.addStream(dag, conf, "S2", transform.output, console.input);
  }
}
