package com.datatorrent.lib.schema;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.codehaus.jettison.json.JSONObject;

@InterfaceStability.Evolving
public class RelativeFileSchemaRegistry extends FileSchemaRegistry
{
  public static String DEFAULT_SCHEMA_DIR = "datatorrent/classSchemas";

  private String basePath = null;

  @Override
  public JSONObject getJSONSchema(String schemaIdentifier) throws Exception
  {
    if (basePath == null) {
      FileSystem fs = FileSystem.newInstance(new Configuration());
      Path home = fs.getHomeDirectory();
      Path schemaDir = new Path(home, DEFAULT_SCHEMA_DIR);
      schemaIdentifier = new Path(schemaDir, schemaIdentifier).toUri().getPath();
    }
    return super.getJSONSchema(schemaIdentifier);
  }

  public String getBasePath()
  {
    return basePath;
  }

  public void setBasePath(String basePath)
  {
    this.basePath = basePath;
  }
}
