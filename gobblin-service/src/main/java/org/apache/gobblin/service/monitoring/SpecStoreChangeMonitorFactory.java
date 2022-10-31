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
import org.apache.gobblin.runtime.kafka.HighLevelConsumer;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * A factory implementation that returns a {@link SpecStoreChangeMonitor} instance.
 */
@Slf4j
public class SpecStoreChangeMonitorFactory implements Provider<SpecStoreChangeMonitor> {
//  static final String SPEC_STORE_CHANGE_MONITOR_CLASS_NAME = "org.apache.gobblin.service.monitoring.SpecStoreChangeMonitor";
  static final String SPEC_STORE_CHANGE_MONITOR_NUM_THREADS_KEY = "numThreads";

  private final Config config;

  @Inject
  public SpecStoreChangeMonitorFactory(Config config) {
    this.config = Objects.requireNonNull(config);
  }

  private SpecStoreChangeMonitor createSpecStoreChangeMonitor()
      throws ReflectiveOperationException {
    // TODO: remove after e2e test
    Config fallback = ConfigBuilder.create()
        .addPrimitive(SpecStoreChangeMonitor.SPEC_STORE_CHANGE_MONITOR_PREFIX + "." + SPEC_STORE_CHANGE_MONITOR_NUM_THREADS_KEY, 2)
        .addPrimitive(SpecStoreChangeMonitor.SPEC_STORE_CHANGE_MONITOR_PREFIX + "." + ConfigurationKeys.KAFKA_BROKERS, "fakeBroker")
        .addPrimitive(SpecStoreChangeMonitor.SPEC_STORE_CHANGE_MONITOR_PREFIX + "." + "singleKafkaDatastreamConsumerClient.brooklinUri", "http://brooklin.mysql.tag.ei-ltx1.atd.disco.linkedin.com:2428/brooklin-service/")
        .addPrimitive(SpecStoreChangeMonitor.SPEC_STORE_CHANGE_MONITOR_PREFIX + "." + "singleKafkaDatastreamConsumerClient.name", "gobblin-flow-spec-updates")
        .addPrimitive(SpecStoreChangeMonitor.SPEC_STORE_CHANGE_MONITOR_PREFIX + ".ligobblin.shaded." + HighLevelConsumer.CONSUMER_CLIENT_FACTORY_CLASS_KEY, "com.linkedin.gobblinkafka.client.SpecChangeDataStreamConsumerClient$Factory")
        .build();
    Config configWithFallBack = config.withFallback(fallback);

    log.info("SpecStoreChangeMonitorFactory fallback config is {}", fallback);

    if (configWithFallBack.hasPath(SpecStoreChangeMonitor.SPEC_STORE_CHANGE_MONITOR_PREFIX)) {
      log.info("SpecStoreChangeMonitorFactory contains prefix after giving fallback");
    } else {
        log.info("SpecStoreChangeMonitorFactory Please provide fallback");
    }
    Config specStoreChangeConfig = configWithFallBack.getConfig(SpecStoreChangeMonitor.SPEC_STORE_CHANGE_MONITOR_PREFIX);

    log.info("Extracted the sub config {}", specStoreChangeConfig);

    String topic = ""; // Pass empty string because we expect underlying client to dynamically determine the Kafka topic
    int numThreads = ConfigUtils.getInt(specStoreChangeConfig, SPEC_STORE_CHANGE_MONITOR_NUM_THREADS_KEY, 5);

    return new SpecStoreChangeMonitor(topic, specStoreChangeConfig, numThreads);
  }

  @Override
  public SpecStoreChangeMonitor get() {
    try {
      return createSpecStoreChangeMonitor();
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Failed to initialize SpecStoreChangeMonitor due to ", e);
    }
  }
}
