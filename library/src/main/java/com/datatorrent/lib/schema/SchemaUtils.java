/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.lib.schema;

import java.io.File;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.*;

import org.codehaus.jettison.json.JSONObject;

/**
 * Schema utility that can be used in populateDAG to set schema
 */
public class SchemaUtils
{
  /**
   * tmplibjars is actually coming from StramAppLauncher.LIBJARS_CONF_KEY_NAME but this needs apex-engine to be a
   * dependency on malhar library.
   */
  public static String EXTRA_JARS = "tmplibjars";

  private static SchemaRegistry registry;

  public static void useSchemaRegistry(SchemaRegistry registry)
  {
    SchemaUtils.registry = registry;
  }

  private static SchemaRegistry getSchemaRegistry(Configuration conf)
  {
    String schemaRegistry = conf.get("dt.schema.registry");
    SchemaRegistry registry;

    if (schemaRegistry == null) {
      if (SchemaUtils.registry == null) {
        registry = new FileSchemaRegistry();
      }
      else {
        registry = SchemaUtils.registry;
      }
    }
    else {
      try {
        Class<?> schemaClazz = Thread.currentThread().getContextClassLoader().loadClass(schemaRegistry);
        registry = (SchemaRegistry)schemaClazz.newInstance();
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        throw new RuntimeException("Failed to initialize schema registry.", e);
      }
    }

    return registry;
  }

  /**
   * This method is supposed to be used instead of DAG.addStream in case a configurable schema needs to be set to the DAG.
   * For eg.
   * <p>
   * Instead of doing:
   *     dag.addStream("s1", op1.output, op2.output);
   * Do:
   *     SchemaUtils.addStream(dag, conf, "s1", op1.output, op2.input);
   * </p>
   *
   * This will ensure that schemaId set in conf property dt.schema.stream.S1 will be generated and
   * set to all the ports of S1 stream
   *
   * @param dag     {@link DAG} object under which application is getting built
   * @param conf    Hadoop configuration object.
   * @param id      ID is the stream is under creation
   * @param source  Source port in stream of upstream operator.
   * @param sinks   Destination ports in the stream of downstream operator(s).
   *
   * @return StreamMeta object representing Stream.
   */
  public static <T> DAG.StreamMeta addStream(DAG dag, Configuration conf, String id,
      Operator.OutputPort<? extends T> source, Operator.InputPort<? super T>... sinks)
  {
    String schemaId = conf.get("dt.stream." + id + ".schema", null);

    if (schemaId != null) {
      Class schemaClass = generateSchemaClass(conf, schemaId);
      if (schemaClass != null) {
        dag.setOutputPortAttribute(source, Context.PortContext.TUPLE_CLASS, schemaClass);
        for (Operator.InputPort sink : sinks) {
          dag.setInputPortAttribute(sink, Context.PortContext.TUPLE_CLASS, schemaClass);
        }
      }
    }

    return dag.addStream(id, source, sinks);
  }

  public static <T> DAG.StreamMeta addStream(DAG dag, Configuration conf, String id,
      Operator.OutputPort<? extends T> source, Operator.InputPort<? super T> sink1)
  {
    Operator.InputPort[] ports = new Operator.InputPort[] { sink1 };
    return addStream(dag, conf, id, source, ports);
  }

  public static <T> DAG.StreamMeta addStream(DAG dag, Configuration conf, String id,
      Operator.OutputPort<? extends T> source, Operator.InputPort<? super T> sink1, Operator.InputPort<? super T> sink2)
  {
    Operator.InputPort[] ports = new Operator.InputPort[] { sink1, sink2 };
    return addStream(dag, conf, id, source, ports);
  }

  private static Class generateSchemaClass(Configuration conf, String schemaId)
  {
    SchemaRegistry registry = getSchemaRegistry(conf);

    try {
      JSONObject jsonSchema = registry.getJSONSchema(schemaId);
      String fqcn = null;
      byte[] beanClass = null;
      Class<?> aClass = null;

      fqcn = jsonSchema.getString("fqcn");
      beanClass = BeanClassGenerator.createAndWriteBeanClass(fqcn, jsonSchema);
      aClass = BeanClassGenerator.readBeanClass(fqcn, beanClass);

      File tempFile = File.createTempFile("/tmp", ".jar", new File("/tmp"));
      Path path = new Path("file:///tmp/");
      FileSystem fs = FileSystem.newInstance(path.toUri(), new Configuration());
      FSDataOutputStream out = fs.create(new Path(path, tempFile.getName()));
      JarOutputStream jout = new JarOutputStream(out);
      jout.putNextEntry(new ZipEntry(fqcn.replace(".", "/") + ".class"));
      jout.write(beanClass);
      jout.closeEntry();
      jout.close();
      out.close();

      conf.set(EXTRA_JARS, conf.get(EXTRA_JARS) + "," + tempFile.getAbsolutePath());

      return aClass;
    } catch (Exception e) {
      e.printStackTrace();
    }

    return null;
  }
}
