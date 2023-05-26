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
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.reflect.ConstructorUtils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.kafka.HighLevelConsumer;
import org.apache.gobblin.runtime.metrics.RuntimeMetrics;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flow.SpecCompiler;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.TimingEventUtils;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.util.ClassAliasResolver;


/**
 * A DagActionStore change monitor that uses {@link DagActionStoreChangeEvent} schema to process Kafka messages received
 * from its corresponding consumer client. This monitor responds to requests to resume or delete a flow and acts as a
 * connector between the API and execution layers of GaaS.
 */
@Slf4j
public class DagActionStoreChangeMonitor extends HighLevelConsumer {
  public static final String DAG_ACTION_CHANGE_MONITOR_PREFIX = "dagActionChangeStore";

  // Metrics
  private ContextAwareMeter killsInvoked;
  private ContextAwareMeter resumesInvoked;
  private ContextAwareMeter flowsLaunched;
  private ContextAwareMeter unexpectedErrors;
  private ContextAwareMeter messageProcessedMeter;
  private ContextAwareGauge produceToConsumeDelayMillis; // Reports delay from all partitions in one gauge

  private volatile Long produceToConsumeDelayValue = -1L;

  protected CacheLoader<String, String> cacheLoader = new CacheLoader<String, String>() {
    @Override
    public String load(String key) throws Exception {
      return key;
    }
  };

  protected LoadingCache<String, String>
      dagActionsSeenCache = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build(cacheLoader);

  protected DagActionStore dagActionStore;

  protected DagManager dagManager;
  protected SpecCompiler specCompiler;
  protected FlowCatalog flowCatalog;
  protected EventSubmitter eventSubmitter;

  // Note that the topic is an empty string (rather than null to avoid NPE) because this monitor relies on the consumer
  // client itself to determine all Kafka related information dynamically rather than through the config.
  public DagActionStoreChangeMonitor(String topic, Config config, DagActionStore dagActionStore, DagManager dagManager,
      int numThreads, FlowCatalog flowCatalog) {
    // Differentiate group id for each host
    super(topic, config.withValue(GROUP_ID_KEY,
        ConfigValueFactory.fromAnyRef(DAG_ACTION_CHANGE_MONITOR_PREFIX + UUID.randomUUID().toString())),
        numThreads);
    this.dagActionStore = dagActionStore;
    this.dagManager = dagManager;
    ClassAliasResolver aliasResolver = new ClassAliasResolver(SpecCompiler.class);
    try {
      String specCompilerClassName = ServiceConfigKeys.DEFAULT_GOBBLIN_SERVICE_FLOWCOMPILER_CLASS;
      if (config.hasPath(ServiceConfigKeys.GOBBLIN_SERVICE_FLOWCOMPILER_CLASS_KEY)) {
        specCompilerClassName = config.getString(ServiceConfigKeys.GOBBLIN_SERVICE_FLOWCOMPILER_CLASS_KEY);
      }
      log.info("Using specCompiler class name/alias " + specCompilerClassName);

      this.specCompiler = (SpecCompiler) ConstructorUtils.invokeConstructor(Class.forName(aliasResolver.resolve(
          specCompilerClassName)), config);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
             | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    this.flowCatalog = flowCatalog;
    this.eventSubmitter = new EventSubmitter.Builder(this.getMetricContext(), "org.apache.gobblin.service").build();
  }

  @Override
  protected void assignTopicPartitions() {
    // Expects underlying consumer to handle initializing partitions and offset for the topic -
    // subscribe to all partitions from latest offset
    return;
  }

  @Override
  /*
  This class is multi-threaded and this message will be called by multiple threads, however any given message will be
  partitioned and processed by only one thread (and corresponding queue).
   */
  protected void processMessage(DecodeableKafkaRecord message) {
    // This will also include the heathCheck message so that we can rely on this to monitor the health of this Monitor
    messageProcessedMeter.mark();
    String key = (String) message.getKey();
    DagActionStoreChangeEvent value = (DagActionStoreChangeEvent) message.getValue();

    String tid = value.getChangeEventIdentifier().getTxId();
    Long produceTimestamp = value.getChangeEventIdentifier().getProduceTimestampMillis();
    String operation = value.getChangeEventIdentifier().getOperationType().name();
    String flowGroup = value.getFlowGroup();
    String flowName = value.getFlowName();
    String flowExecutionId = value.getFlowExecutionId();
    DagActionStore.DagActionValue dagAction = DagActionStore.DagActionValue.valueOf(value.getDagAction());

    produceToConsumeDelayValue = calcMillisSince(produceTimestamp);
    log.debug("Processing Dag Action message for flow group: {} name: {} executionId: {} tid: {} operation: {} lag: {}",
        flowGroup, flowName, flowExecutionId, tid, operation, produceToConsumeDelayValue);

    String changeIdentifier = tid + key;
    if (!ChangeMonitorUtils.shouldProcessMessage(changeIdentifier, dagActionsSeenCache, operation,
        produceTimestamp.toString())) {
      return;
    }

    // We only expect INSERT and DELETE operations done to this table. INSERTs correspond to any type of
    // {@link DagActionStore.DagACtionValue} flow requests that have to be processed. DELETEs require no action.
    try {
      if (operation.equals("INSERT")) {
        if (dagAction.equals(DagActionStore.DagActionValue.RESUME)) {
          log.info("Received insert dag action and about to send resume flow request");
          dagManager.handleResumeFlowRequest(flowGroup, flowName,Long.parseLong(flowExecutionId));
          this.resumesInvoked.mark();
        } else if (dagAction.equals(DagActionStore.DagActionValue.KILL)) {
          log.info("Received insert dag action and about to send kill flow request");
          dagManager.handleKillFlowRequest(flowGroup, flowName, Long.parseLong(flowExecutionId));
          this.killsInvoked.mark();
        } else if (dagAction.equals(DagActionStore.DagActionValue.LAUNCH)) {
          log.info("Received insert dag action and about to forward launch request to DagManager");
          submitFlowToDagManager(flowGroup, flowName);
        }else {
          log.warn("Received unsupported dagAction {}. Expected to be a KILL or RESUME", dagAction);
          this.unexpectedErrors.mark();
          return;
        }
      } else if (operation.equals("UPDATE")) {
        log.warn("Received an UPDATE action to the DagActionStore when values in this store are never supposed to be "
            + "updated. Flow group: {} name {} executionId {} were updated to action {}", flowGroup, flowName,
            flowExecutionId, dagAction);
        this.unexpectedErrors.mark();
      } else if (operation.equals("DELETE")) {
        log.debug("Deleted flow group: {} name: {} executionId {} from DagActionStore", flowGroup, flowName, flowExecutionId);
      } // TODO: multiActiveScheduler change here to add a case for a new launch flow action. We want to check if it is
      // an execution that has been "won" by checking pursuant timestamp = null then pass to dag managers. the right one will
      // actually launch it. if the config is NOT turned on we should do any of this handling or recieve these type of events
      else {
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

  protected void submitFlowToDagManager(String flowGroup, String flowName) {
    // Retrieve job execution plan by recompiling the flow spec to send to the DagManager
    FlowId flowId = new FlowId().setFlowGroup(flowGroup).setFlowName(flowName);
    FlowSpec spec = null;
    try {
      URI flowUri = FlowSpec.Utils.createFlowSpecUri(flowId);
      spec = (FlowSpec) flowCatalog.getSpecs(flowUri);
      Dag<JobExecutionPlan> jobExecutionPlanDag = specCompiler.compileFlow(spec);
      //Send the dag to the DagManager.
      dagManager.addDag(jobExecutionPlanDag, true, true);
    } catch (URISyntaxException e) {
      log.warn("Could not create URI object for flowId {} due to error {}", flowId, e.getMessage());
      this.unexpectedErrors.mark();
      return;
    } catch (SpecNotFoundException e) {
      log.warn("Spec not found for flow group: {} name: {} Exception: {}", flowGroup, flowName, e);
      this.unexpectedErrors.mark();
      return;
    } catch (IOException e) {
      Map<String, String> flowMetadata = TimingEventUtils.getFlowMetadata(spec);
      String failureMessage = "Failed to add Job Execution Plan due to: " + e.getMessage();
      flowMetadata.put(TimingEvent.METADATA_MESSAGE, failureMessage);
      new TimingEvent(this.eventSubmitter, TimingEvent.FlowTimings.FLOW_FAILED).stop(flowMetadata);
      log.warn("Failed to add Job Execution Plan for flow group: {} name: {} due to error {}", flowGroup, flowName, e);
      this.unexpectedErrors.mark();
      return;
    }
    // Only mark this if the dag was successfully added
    this.flowsLaunched.mark();
  }

  @Override
  protected void createMetrics() {
    super.createMetrics();
    this.killsInvoked = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_KILLS_INVOKED);
    this.resumesInvoked = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_RESUMES_INVOKED);
    this.flowsLaunched = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_FLOWS_LAUNCHED);
    this.unexpectedErrors = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_UNEXPECTED_ERRORS);
    this.messageProcessedMeter = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_MESSAGE_PROCESSED);
    this.produceToConsumeDelayMillis = this.getMetricContext().newContextAwareGauge(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_PRODUCE_TO_CONSUME_DELAY_MILLIS, () -> produceToConsumeDelayValue);
    this.getMetricContext().register(this.produceToConsumeDelayMillis);
  }
}
