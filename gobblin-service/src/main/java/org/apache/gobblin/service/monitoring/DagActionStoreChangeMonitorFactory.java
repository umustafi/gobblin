/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.service.monitoring;

import java.util.Objects;

import com.typesafe.config.Config;

import javax.inject.Inject;
import javax.inject.Provider;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.kafka.HighLevelConsumer;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.util.ConfigUtils;


/**
 * A factory implementation that returns a {@link DagActionStoreChangeMonitor} instance.
 */
@Slf4j
public class DagActionStoreChangeMonitorFactory implements Provider<DagActionStoreChangeMonitor> {
  static final String DAG_ACTION_STORE_CHANGE_MONITOR_NUM_THREADS_KEY = "numThreads";

  private final Config config;
  private DagActionStore dagActionStore;
  private DagManager dagManager;

  @Inject
  public DagActionStoreChangeMonitorFactory(Config config, DagActionStore dagActionStore, DagManager dagManager) {
    this.config = Objects.requireNonNull(config);
    this.dagActionStore = dagActionStore;
    this.dagManager = dagManager;
  }

  private DagActionStoreChangeMonitor createDagActionStoreMonitor()
    throws ReflectiveOperationException {
    // TODO: remove after e2e test
    Config fallback = ConfigBuilder.create()
        .addPrimitive(DagActionStoreChangeMonitor.DAG_ACTION_CHANGE_MONITOR_PREFIX + "." + DAG_ACTION_STORE_CHANGE_MONITOR_NUM_THREADS_KEY, 2)
        .addPrimitive(DagActionStoreChangeMonitor.DAG_ACTION_CHANGE_MONITOR_PREFIX + ".ligobblin.shaded." + ConfigurationKeys.KAFKA_BROKERS, "fakeBroker2")
        .addPrimitive(DagActionStoreChangeMonitor.DAG_ACTION_CHANGE_MONITOR_PREFIX + ".kafka.consumer.groupId", "shared-gobblin-service")
        .addPrimitive(DagActionStoreChangeMonitor.DAG_ACTION_CHANGE_MONITOR_PREFIX + ".kafka.consumer.clientId", "shared-gobblin-service")
        .addPrimitive(DagActionStoreChangeMonitor.DAG_ACTION_CHANGE_MONITOR_PREFIX + ".kafka.schema.registry.url", "http://brooklin.mysql.tag.ei-ltx1.atd.disco.linkedin.com:2428/brooklin-service/")
        .addPrimitive(DagActionStoreChangeMonitor.DAG_ACTION_CHANGE_MONITOR_PREFIX + "." + "singleKafkaDatastreamConsumerClient.brooklinUri", "http://brooklin.mysql.tag.ei-ltx1.atd.disco.linkedin.com:2428/brooklin-service/")
        .addPrimitive(DagActionStoreChangeMonitor.DAG_ACTION_CHANGE_MONITOR_PREFIX + "." + "singleKafkaDatastreamConsumerClient.name", "gobblin-dag-action-updates")
        .addPrimitive(DagActionStoreChangeMonitor.DAG_ACTION_CHANGE_MONITOR_PREFIX + "." + "singleKafkaDatastreamConsumerClient.completenessEnabled", "true")
        .addPrimitive(DagActionStoreChangeMonitor.DAG_ACTION_CHANGE_MONITOR_PREFIX + ".ligobblin.shaded." + HighLevelConsumer.CONSUMER_CLIENT_FACTORY_CLASS_KEY, "com.linkedin.gobblinkafka.client.DagActionChangeDataStreamConsumerClient$Factory")
        .build();
    Config configWithFallBack = config.withFallback(fallback);

    if (configWithFallBack.hasPath(DagActionStoreChangeMonitor.DAG_ACTION_CHANGE_MONITOR_PREFIX)) {
      log.info("DagActionStoreChangeMonitorFactory contains prefix after giving fall back");
    } else {
      log.info("DagActionStoreChangeMonitorFactory Please provide fallback");
    }

    Config dagActionStoreChangeConfig = configWithFallBack.getConfig(DagActionStoreChangeMonitor.DAG_ACTION_CHANGE_MONITOR_PREFIX);

    log.info("Extracted the sub config {}", dagActionStoreChangeConfig);
    String topic = ""; // Pass empty string because we expect underlying client to dynamically determine the Kafka topic
    int numThreads = ConfigUtils.getInt(dagActionStoreChangeConfig, DAG_ACTION_STORE_CHANGE_MONITOR_NUM_THREADS_KEY, 5);

    return new DagActionStoreChangeMonitor(topic, dagActionStoreChangeConfig, this.dagActionStore, this.dagManager, numThreads);
  }

  @Override
  public DagActionStoreChangeMonitor get() {
    try {
      return createDagActionStoreMonitor();
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Failed to initialize DagActionStoreMonitor due to ", e);
    }
  }
}
