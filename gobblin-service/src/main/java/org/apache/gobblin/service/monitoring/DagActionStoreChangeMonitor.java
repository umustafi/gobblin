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

import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.kafka.HighLevelConsumer;
import org.apache.gobblin.runtime.metrics.RuntimeMetrics;
import org.apache.gobblin.service.modules.orchestration.DagManager;


/**
 * A DagActionStore change monitor that uses {@link DagActionStoreChangeEvent} schema to process Kafka messages received
 * from its corresponding consumer client. This monitor responds to requests to resume or delete a flow and acts as a
 * connector between the API and execution layers of GaaS.
 */
@Slf4j
public class DagActionStoreChangeMonitor extends HighLevelConsumer {
  public static final String DAG_ACTION_CHANGE_MONITOR_PREFIX = "dagActionChangeStore";

  // Metrics
  ContextAwareMeter killsInvoked;
  ContextAwareMeter resumesInvoked;
  ContextAwareMeter unexpectedErrors;

  protected CacheLoader<String, String> cacheLoader = new CacheLoader<String, String>() {
    @Override
    public String load(String key) throws Exception {
      return key;
    }
  };

  protected LoadingCache<String, String>
      dagActionsSeenCache = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build(cacheLoader);

  @Inject
  protected DagActionStore dagActionStore;

  @Inject
  protected DagManager dagManager;

  public DagActionStoreChangeMonitor(String topic, Config config, int numThreads) {
    // Differentiate group id for each host
    super(topic, config.withValue(GROUP_ID_KEY,
        ConfigValueFactory.fromAnyRef(DAG_ACTION_CHANGE_MONITOR_PREFIX + UUID.randomUUID().toString())),
        numThreads);
  }

  @Override
  /*
  This class is multi-threaded and this message will be called by multiple threads, however any given message will be
  partitioned and processed by only one thread (and corresponding queue).
   */
  protected void processMessage(DecodeableKafkaRecord message) {
    String key = (String) message.getKey();
    DagActionStoreChangeEvent value = (DagActionStoreChangeEvent) message.getValue();

    Long timestamp = value.getTimestamp();
    String operation = value.getOperationType().name();
    String flowGroup = value.getFlowGroup();
    String flowName = value.getFlowName();
    String flowExecutionId = value.getFlowExecutionId();

    log.info("Processing Dag Action message for flow group: {} name: {} executionId: {}", flowGroup, flowName,
        flowExecutionId);

    // If we've already processed a message with this timestamp and flow before then skip duplicate message
    String changeIdentifier = timestamp.toString() + key;
    if (dagActionsSeenCache.getIfPresent(changeIdentifier) != null) {
      return;
    }

    // If event is a heartbeat type then log it and skip processing
    if (operation == "HEARTBEAT") {
      log.debug("Received heartbeat message from time {}", timestamp);
      return;
    }

    // retrieve operation type from MySQL table OR from the event itself
    DagActionStore.DagActionValue dagAction = null;
    try {
      dagAction = dagActionStore.getDagAction(flowGroup, flowName, flowExecutionId).getDagActionValue();
    } catch (IOException e) {
      log.warn("Encountered IOException trying to retrieve dagAction for flow group: {} name: {} executionId: {}. "
          + "Exception: {}", flowGroup, flowName, flowExecutionId, e);
      this.unexpectedErrors.mark();
    } catch (SpecNotFoundException e) {
      log.warn("DagAction not found for flow group: {} name: {} executionId: {} Exception: {}", flowGroup, flowName,
          flowExecutionId, e);
      this.unexpectedErrors.mark();
    } catch (SQLException throwables) {
      log.warn("Encountered SQLException trying to retrieve dagAction for flow group: {} name: {} executionId: {}. "
          + "Exception: {}", flowGroup, flowName, flowExecutionId, throwables);
      throwables.printStackTrace();
    }

    try {
      if (operation == "INSERT") {
        if (dagAction.equals(DagActionStore.DagActionValue.RESUME)) {
          dagManager.handleResumeFlowRequest(flowGroup, flowName,Long.parseLong(flowExecutionId));
          this.resumesInvoked.mark();
        } else if (dagAction.equals(DagActionStore.DagActionValue.KILL)) {
          dagManager.handleKillFlowRequest(flowGroup, flowName, Long.parseLong(flowExecutionId));
          this.killsInvoked.mark();
        } else {
          log.warn("Received unsupported dagAction {}. Expected to be a KILL or RESUME", dagAction);
          this.unexpectedErrors.mark();
          return;
        }
      } else if (operation == "UPDATE") {
        log.warn("Received an UPDATE action to the DagActionStore when values in this store are never supposed to be "
            + "updated. Flow group: {} name {} executionId {} were updated to action {}", flowGroup, flowName,
            flowExecutionId, dagAction);
        this.unexpectedErrors.mark();
      } else if (operation == "DELETE") {
        log.debug("Deleted flow group: {} name: {} executionId {} from DagActionStore", flowGroup, flowName, flowExecutionId);
      } else {
        log.warn("Received unsupported change type of operation {}. Expected values to be in [INSERT, UPDATE, DELETE]",
            operation);
        this.unexpectedErrors.mark();
        return;
      }
    } catch (Exception e) {
      log.warn("Ran into unexpected error processing DagActionStore changes: {}", e);
      this.unexpectedErrors.mark();
    }

    dagActionsSeenCache.put(changeIdentifier, changeIdentifier);
  }

  @Override
  protected void createMetrics() {
    super.createMetrics();
    this.killsInvoked = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_KILLS_INVOKED);
    this.resumesInvoked = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_RESUMES_INVOKED);
    this.unexpectedErrors = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_UNEXPECTED_ERRORS);
  }

}
