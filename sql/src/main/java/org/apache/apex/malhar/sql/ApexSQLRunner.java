/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.sql;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.apex.malhar.sql.parser.ApexSQLParser;
import org.apache.apex.malhar.sql.parser.SqlCreateFunction;
import org.apache.apex.malhar.sql.parser.SqlCreateTable;
import org.apache.apex.malhar.sql.table.Endpoint;
import org.apache.calcite.sql.SqlNode;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.client.StramAppLauncher;

public class ApexSQLRunner
{
  public static void main(String[] args) throws Exception
  {
    if (args.length != 1) {
      System.err.println("apex-sql <sql-file>");
      return;
    }

    List<String> stmts = Files.readAllLines(Paths.get(args[0]), StandardCharsets.UTF_8);
    run(null, stmts);
  }

  public static void run(DAG dag, List<String> stmts) throws Exception
  {
    SQLExecEnvironment env = SQLExecEnvironment.getEnvironment();
    for (String stmt : stmts) {
      ApexSQLParser parser = new ApexSQLParser(stmt);
      SqlNode sqlNode = parser.impl().parseSqlStmtEof();
      if (sqlNode instanceof SqlCreateTable) {
        handleCreateTable(env, (SqlCreateTable)sqlNode);
      } else if (sqlNode instanceof SqlCreateFunction) {
        handleCreateFunction(env, (SqlCreateFunction)sqlNode);
      } else {
        if (dag != null) {
          env.executeSQL(dag, stmt);
          // break here because only one DAG execution statement is possible here.
          break;
        }
        else {
          startLaunch(env, stmt);
        }
      }
    }
  }

  private static void handleCreateFunction(SQLExecEnvironment env, SqlCreateFunction sqlCreateFunction)
    throws ClassNotFoundException
  {
    if (sqlCreateFunction.jarName() != null) {
      // TODO: Load the jar using URLClassLoader and add the jar to libjars to make it available during execution
      throw new UnsupportedOperationException("UDF 'USING JAR' not implemented");
    }

    String className = sqlCreateFunction.className();
    String classFnName = sqlCreateFunction.classFnName();
    String fnName = sqlCreateFunction.functionName();

    Class<?> fnClass = Thread.currentThread().getContextClassLoader().loadClass(className);
    env.registerFunction(fnName, fnClass, classFnName);
  }

  private static void handleCreateTable(SQLExecEnvironment env, SqlCreateTable sqlCreateTable)
  {
    String tableName = sqlCreateTable.tableName();
    Endpoint endpoint = null;

    env.registerTable(tableName, endpoint);
  }

  private static void startLaunch(SQLExecEnvironment env, String sql) throws Exception
  {
    ApexSQLApplication streamingApp = new ApexSQLApplication(env, sql);
    launch(streamingApp, "Apex SQL App", null);
  }

  private static void launch(StreamingApplication app, String name, String libjars) throws Exception
  {
    Configuration conf = new Configuration(true);
//    conf.set("dt.loggers.level", "org.apache.*:DEBUG, com.datatorrent.*:DEBUG");
    conf.set("dt.dfsRootDirectory", System.getProperty("dt.dfsRootDirectory"));
    conf.set("fs.defaultFS", System.getProperty("fs.defaultFS"));
    conf.set("yarn.resourcemanager.address", System.getProperty("yarn.resourcemanager.address"));
    conf.addResource(new File(System.getProperty("dt.site.path")).toURI().toURL());

    if (libjars != null) {
      conf.set(StramAppLauncher.LIBJARS_CONF_KEY_NAME, libjars);
    }
    StramAppLauncher appLauncher = new StramAppLauncher(name, conf);
    appLauncher.loadDependencies();
    StreamingAppFactory appFactory = new StreamingAppFactory(app, name);
    appLauncher.launchApp(appFactory);
  }

  public static class ApexSQLApplication implements StreamingApplication
  {
    private SQLExecEnvironment env;
    private String sql;

    public ApexSQLApplication(SQLExecEnvironment env, String sql)
    {
      this.env = env;
      this.sql = sql;
    }

    @Override
    public void populateDAG(DAG dag, Configuration configuration)
    {
      this.env.executeSQL(dag, sql);
    }
  }
}
