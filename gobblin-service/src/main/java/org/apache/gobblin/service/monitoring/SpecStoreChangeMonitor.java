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

import java.util.HashMap;
import java.util.HashSet;

import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecStore;
import org.apache.gobblin.runtime.kafka.HighLevelConsumer;
import org.apache.gobblin.runtime.spec_catalog.AddSpecResponse;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.runtime.spec_store.MysqlSpecStore;
import org.apache.gobblin.service.modules.scheduler.GobblinServiceJobScheduler;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SpecStoreChangeMonitor extends HighLevelConsumer {
  protected FlowCatalog _flowCatalog;
  protected HashSet<String> timestampsSeenBefore;


  public SpecStoreChangeMonitor(String topic, Config config, int numThreads, FlowCatalog flowCatalog) {
    super(topic, config, numThreads);
    // TODO: change this to receive the SpecStore instance from Guice
    this._flowCatalog = flowCatalog;
    this.timestampsSeenBefore = new HashSet();
  }

  @Override
  protected void processMessage(DecodeableKafkaRecord message) {
    // the message value contains a SpecStoreChangeEvent with all this info necessary
    // expects operation, specUri, timestamp for de-dup
    String specUri = (String) message.getKey();
    HashMap<String, String> value = (HashMap<String, String>) message.getValue();

    String timestamp = value.get("timestamp");
    String operation = value.get("operation");
    log.info("specUri is %s timestamp is %s operation is %s", specUri, timestamp, operation);

    // If we've already processed a message with this timestamp before then skip duplicate message
    if (timestampsSeenBefore.contains(timestamp)) {
      return;
    }

    Spec spec = this._flowCatalog.getSpecFromStore(specUri);

    // Call respective action for the type of change received
    if (operation == "CREATE") {
      GobblinServiceJobScheduler.onAddSpec(spec);
    } else if (operation == "INSERT") {
      GobblinServiceJobScheduler.onUpdateSpec(spec);
    } else if (operation == "DELETE") {
      GobblinServiceJobScheduler.onDeleteSpec(spec.getUri(), spec.getVersion());
    } else {
      log.error("Received unsupported change type of operation");
      return;
    }

    timestampsSeenBefore.add(timestamp);
  }
}