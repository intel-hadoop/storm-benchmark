/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package storm.benchmark.metrics;


/**
 * DRPCMetricsCollector is meant to collect end-to-end latency for DRPC benchmarks
 * the latency is the time between DRPCClient submitting a query and
 * receiving the result
 */

import backtype.storm.Config;
import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.DRPCClient;
import org.apache.log4j.Logger;
import org.apache.thrift7.TException;
import storm.benchmark.util.FileUtils;

import java.io.PrintWriter;
import java.util.HashSet;
import java.util.List;

public class DRPCMetricsCollector implements IMetricsCollector {
  private static final Logger LOG = Logger.getLogger(DRPCMetricsCollector.class);

  final MetricsCollectorConfig config;
  final String function;
  final List<String> args;
  final int port;
  int index = 0;

  public DRPCMetricsCollector(Config stormConfig,
                              String function, List<String> args) {
    this.config = new MetricsCollectorConfig(stormConfig);
    this.function = function;
    this.args = args;
    this.port = (Integer) stormConfig.get(Config.DRPC_PORT);
  }

  @Override
  public void run() {
    long now = System.currentTimeMillis();

    final long endTime = now + config.totalTime;
    long totalLat = 0L;
    int count = 0;
    try {
      final String name = config.name;
      final String path = config.path;
      final String confFile = String.format(
              MetricsCollectorConfig.CONF_FILE_FORMAT, path, name, now);
      final String dataFile = String.format(
              MetricsCollectorConfig.DATA_FILE_FORMAT, path, name, now);
      PrintWriter confWriter = FileUtils.createFileWriter(path, confFile);
      PrintWriter dataWriter = FileUtils.createFileWriter(path, dataFile);
      config.writeStormConfig(confWriter);
      while (now < endTime) {
        Thread.sleep(config.pollInterval);
        long lat = execute(nextArg(), dataWriter);
        totalLat += lat;
        count++;
        now = System.currentTimeMillis();
      }
      double avgLat = 0 == count ? 0.0 : (double) totalLat / count;
      dataWriter.println(String.format("average latency = %f ms", avgLat));
      dataWriter.close();
    } catch (DRPCExecutionException e) {
      LOG.error("fail to execute drpc function", e);
    } catch (TException e) {
      LOG.error("thrift error", e);
    } catch (InterruptedException e) {
      LOG.error("interrupted", e);
    }
  }

  private String nextArg() {
    if (args.size() == index) {
      index = 0;
    }
    String ret = args.get(index);
    index++;
    return ret;
  }

  private long execute(String arg, PrintWriter writer) throws TException, DRPCExecutionException {
    LOG.debug(String.format("executing %s('%s')", function, arg));
    DRPCClient client = new DRPCClient("127.0.0.1", port);
    long start = System.currentTimeMillis();
    String result = client.execute(function, arg);
    long end = System.currentTimeMillis();
    long latency = end - start;
    writer.println(String.format("%s('%s') = %s, latency = %d ms", function, arg, result, latency));
    writer.flush();
    return latency;
  }
}
