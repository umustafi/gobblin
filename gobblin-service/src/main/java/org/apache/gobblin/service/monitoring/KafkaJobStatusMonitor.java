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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.RetryListener;
import com.github.rholder.retry.Retryer;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import avro.shaded.com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.metastore.FileContextBasedFsStateStore;
import org.apache.gobblin.metastore.FileContextBasedFsStateStoreFactory;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.metrics.event.CountEventBuilder;
import org.apache.gobblin.metrics.event.JobEvent;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.kafka.HighLevelConsumer;
import org.apache.gobblin.runtime.retention.DatasetCleanerTask;
import org.apache.gobblin.runtime.troubleshooter.IssueEventBuilder;
import org.apache.gobblin.runtime.troubleshooter.JobIssueEventHandler;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.retry.RetryerFactory;

import static org.apache.gobblin.util.retry.RetryerFactory.RETRY_INTERVAL_MS;
import static org.apache.gobblin.util.retry.RetryerFactory.RETRY_TIME_OUT_MS;
import static org.apache.gobblin.util.retry.RetryerFactory.RETRY_TYPE;
import static org.apache.gobblin.util.retry.RetryerFactory.RetryType;


/**
 * A Kafka monitor that tracks {@link org.apache.gobblin.metrics.GobblinTrackingEvent}s reporting statuses of
 * running jobs. The job statuses are stored as {@link org.apache.gobblin.configuration.State} objects in
 * a {@link FileContextBasedFsStateStore}.
 */
@Slf4j
public abstract class KafkaJobStatusMonitor extends HighLevelConsumer<byte[], byte[]> {
  static final String JOB_STATUS_MONITOR_PREFIX = "jobStatusMonitor";
  //We use table suffix that is different from the Gobblin job state store suffix of jst to avoid confusion.
  //gst refers to the state store suffix for GaaS-orchestrated Gobblin jobs.
  public static final String GET_AND_SET_JOB_STATUS = MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,
      JOB_STATUS_MONITOR_PREFIX,  "getAndSetJobStatus");

  private static final String PROCESS_JOB_ISSUE = MetricRegistry
      .name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX, JOB_STATUS_MONITOR_PREFIX, "jobIssueProcessingTime");

  static final String JOB_STATUS_MONITOR_TOPIC_KEY = "topic";
  static final String JOB_STATUS_MONITOR_NUM_THREADS_KEY = "numThreads";
  static final String JOB_STATUS_MONITOR_CLASS_KEY = "class";
  static final String DEFAULT_JOB_STATUS_MONITOR_CLASS = KafkaAvroJobStatusMonitor.class.getName();

  private static final String KAFKA_AUTO_OFFSET_RESET_KEY = "auto.offset.reset";
  private static final String KAFKA_AUTO_OFFSET_RESET_SMALLEST = "smallest";

  private static final int EXPECTED_TIME_FOR_METRICS_REPORTING = 90;

  @Getter
  private final StateStore<org.apache.gobblin.configuration.State> stateStore;
  private final ScheduledExecutorService scheduledExecutorService;
  private static final Config RETRYER_FALLBACK_CONFIG = ConfigFactory.parseMap(ImmutableMap.of(
      RETRY_TIME_OUT_MS, TimeUnit.HOURS.toMillis(24L), // after a day, presume non-transient and give up
      RETRY_INTERVAL_MS, TimeUnit.MINUTES.toMillis(1L), // back-off to once/minute
      RETRY_TYPE, RetryType.EXPONENTIAL.name()));
  private static final Config DEFAULTS = ConfigFactory.parseMap(ImmutableMap.of(
      KAFKA_AUTO_OFFSET_RESET_KEY, KAFKA_AUTO_OFFSET_RESET_SMALLEST));

  private static final List<ExecutionStatus> ORDERED_EXECUTION_STATUSES = ImmutableList
      .of(ExecutionStatus.COMPILED, ExecutionStatus.PENDING, ExecutionStatus.PENDING_RESUME, ExecutionStatus.PENDING_RETRY,
          ExecutionStatus.ORCHESTRATED, ExecutionStatus.RUNNING, ExecutionStatus.COMPLETE,
          ExecutionStatus.FAILED, ExecutionStatus.CANCELLED);

  private final JobIssueEventHandler jobIssueEventHandler;

  private final ConcurrentHashMap<String, Long> flowNameGroupToWorkUnitCount;

  private final Retryer<Void> persistJobStatusRetryer;

  private final ScheduledExecutorService scheduler;

  private final ArrayList<ScheduledFuture> futuresList;

  public KafkaJobStatusMonitor(String topic, Config config, int numThreads, JobIssueEventHandler jobIssueEventHandler)
      throws ReflectiveOperationException {
    super(topic, config.withFallback(DEFAULTS), numThreads);
    String stateStoreFactoryClass = ConfigUtils.getString(config, ConfigurationKeys.STATE_STORE_FACTORY_CLASS_KEY, FileContextBasedFsStateStoreFactory.class.getName());

    this.stateStore =
        ((StateStore.Factory) Class.forName(stateStoreFactoryClass).newInstance()).createStateStore(config, org.apache.gobblin.configuration.State.class);
    this.scheduledExecutorService = Executors.newScheduledThreadPool(1);

    this.jobIssueEventHandler = jobIssueEventHandler;

    this.flowNameGroupToWorkUnitCount = new ConcurrentHashMap<>();

    Config retryerOverridesConfig = config.hasPath(ConfigurationKeys.KAFKA_JOB_STATUS_MONITOR_RETRY_TIME_OUT_MINUTES)
        ? ConfigFactory.parseMap(ImmutableMap.of(
                RETRY_TIME_OUT_MS,
                config.getDuration(ConfigurationKeys.KAFKA_JOB_STATUS_MONITOR_RETRY_TIME_OUT_MINUTES, TimeUnit.MINUTES)))
        : ConfigFactory.empty();
    // log exceptions to expose errors we suffer under and/or guide intervention when resolution not readily forthcoming
    this.persistJobStatusRetryer =
        RetryerFactory.newInstance(retryerOverridesConfig.withFallback(RETRYER_FALLBACK_CONFIG), Optional.of(new RetryListener() {
          @Override
          public <V> void onRetry(Attempt<V> attempt) {
            if (attempt.hasException()) {
              String msg = String.format("(Likely retryable) failure adding job status to state store [attempt: %d; %s after start]",
                  attempt.getAttemptNumber(), Duration.ofMillis(attempt.getDelaySinceFirstAttempt()).toString());
              log.warn(msg, attempt.getExceptionCause());
            }
          }
        }));

    // Used to remove gauges in the background
    this.scheduler = Executors.newScheduledThreadPool(1);
    this.futuresList = new ArrayList<>();
  }

  @Override
  protected void startUp() {
    super.startUp();
    log.info("Scheduling state store cleaner..");
    org.apache.gobblin.configuration.State state = new org.apache.gobblin.configuration.State(ConfigUtils.configToProperties(this.config));
    state.setProp(ConfigurationKeys.JOB_ID_KEY, "GobblinServiceJobStatusCleanerJob");
    state.setProp(ConfigurationKeys.TASK_ID_KEY, "GobblinServiceJobStatusCleanerTask");

    TaskContext taskContext = new TaskContext(new WorkUnitState(WorkUnit.createEmpty(), state));
    DatasetCleanerTask cleanerTask = new DatasetCleanerTask(taskContext);
    scheduledExecutorService.scheduleAtFixedRate(cleanerTask, 300L, 86400L, TimeUnit.SECONDS);
  }

  @Override
  public void shutDown() {
     super.shutDown();
     this.scheduledExecutorService.shutdown();
     try {
       this.scheduledExecutorService.awaitTermination(30, TimeUnit.SECONDS);

       // Upon shutdown, cancel any pending removals of gauges
       for (ScheduledFuture futureToTerminate : this.futuresList) {
         futureToTerminate.cancel(true);
       }
     } catch (InterruptedException e) {
       log.error("Exception encountered when shutting down state store cleaner", e);
     }
  }

  @Override
  protected void createMetrics() {
    super.createMetrics();
  }

  @Override
  protected void processMessage(DecodeableKafkaRecord<byte[],byte[]> message) {
    GobblinTrackingEvent gobblinTrackingEvent = deserializeEvent(message);

    if (gobblinTrackingEvent == null) {
      return;
    }

    if (IssueEventBuilder.isIssueEvent(gobblinTrackingEvent)) {
      try (Timer.Context context = getMetricContext().timer(PROCESS_JOB_ISSUE).time()) {
        jobIssueEventHandler.processEvent(gobblinTrackingEvent);
      }
    }

    if (gobblinTrackingEvent.getName().equals(JobEvent.WORK_UNITS_CREATED)) {
      emitWorkUnitCountMetric(gobblinTrackingEvent);
      return;
    }

    try {
      persistJobStatusRetryer.call(() -> {
        // re-create `jobStatus` on each attempt, since mutated within `addJobStatusToStateStore`
        org.apache.gobblin.configuration.State jobStatus = parseJobStatus(gobblinTrackingEvent);
        if (jobStatus != null) {
          try (Timer.Context context = getMetricContext().timer(GET_AND_SET_JOB_STATUS).time()) {
            addJobStatusToStateStore(jobStatus, this.stateStore);
          }
        }
        return null;
      });
    } catch (ExecutionException ee) {
      String msg = String.format("Failed to add job status to state store for kafka offset %d", message.getOffset());
      log.warn(msg, ee.getCause());
      // Throw RuntimeException to avoid advancing kafka offsets without updating state store
      throw new RuntimeException(msg, ee.getCause());
    } catch (RetryException re) {
      String interruptedNote = Thread.currentThread().isInterrupted() ? "... then interrupted" : "";
      String msg = String.format("Failed to add job status to state store for kafka offset %d (retried %d times%s)",
          message.getOffset(), re.getNumberOfFailedAttempts(), interruptedNote);
      Throwable informativeException = re.getLastFailedAttempt().hasException()
          ? re.getLastFailedAttempt().getExceptionCause()
          : re;
      log.warn(msg, informativeException);
      // Throw RuntimeException to avoid advancing kafka offsets without updating state store
      throw new RuntimeException(msg, informativeException);
    }
  }

  /**
   * Persist job status to the underlying {@link StateStore}.
   * It fills missing fields in job status and also merge the fields with the
   * existing job status in the state store. Merging is required because we
   * do not want to lose the information sent by other GobblinTrackingEvents.
   * @param jobStatus
   * @throws IOException
   */
  @VisibleForTesting
  static void addJobStatusToStateStore(org.apache.gobblin.configuration.State jobStatus, StateStore stateStore)
      throws IOException {
    if (!jobStatus.contains(TimingEvent.FlowEventConstants.JOB_NAME_FIELD)) {
      jobStatus.setProp(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, JobStatusRetriever.NA_KEY);
    }
    if (!jobStatus.contains(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD)) {
      jobStatus.setProp(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD, JobStatusRetriever.NA_KEY);
    }

    String flowName = jobStatus.getProp(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD);
    String flowGroup = jobStatus.getProp(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD);
    String flowExecutionId = jobStatus.getProp(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD);
    String jobName = jobStatus.getProp(TimingEvent.FlowEventConstants.JOB_NAME_FIELD);
    String jobGroup = jobStatus.getProp(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD);
    String storeName = jobStatusStoreName(flowGroup, flowName);
    String tableName = jobStatusTableName(flowExecutionId, jobGroup, jobName);

    List<org.apache.gobblin.configuration.State> states = stateStore.getAll(storeName, tableName);
    if (states.size() > 0) {
      String previousStatus = states.get(states.size() - 1).getProp(JobStatusRetriever.EVENT_NAME_FIELD);
      String currentStatus = jobStatus.getProp(JobStatusRetriever.EVENT_NAME_FIELD);

      // PENDING_RESUME is allowed to override, because it happens when a flow is being resumed from previously being failed
      if (previousStatus != null && currentStatus != null && !currentStatus.equals(ExecutionStatus.PENDING_RESUME.name())
        && ORDERED_EXECUTION_STATUSES.indexOf(ExecutionStatus.valueOf(currentStatus)) < ORDERED_EXECUTION_STATUSES.indexOf(ExecutionStatus.valueOf(previousStatus))) {
        log.warn(String.format("Received status %s when status is already %s for flow (%s, %s, %s), job (%s, %s)",
            currentStatus, previousStatus, flowGroup, flowName, flowExecutionId, jobGroup, jobName));
        jobStatus = mergeState(states.get(states.size() - 1), jobStatus);
      } else {
        jobStatus = mergeState(jobStatus, states.get(states.size() - 1));
      }
    }

    modifyStateIfRetryRequired(jobStatus);

    stateStore.put(storeName, tableName, jobStatus);
  }

  private static void modifyStateIfRetryRequired(org.apache.gobblin.configuration.State state) {
    int maxAttempts = state.getPropAsInt(TimingEvent.FlowEventConstants.MAX_ATTEMPTS_FIELD, 1);
    int currentAttempts = state.getPropAsInt(TimingEvent.FlowEventConstants.CURRENT_ATTEMPTS_FIELD, 1);
    if (state.getProp(JobStatusRetriever.EVENT_NAME_FIELD).equals(ExecutionStatus.FAILED.name()) && currentAttempts < maxAttempts) {
      state.setProp(TimingEvent.FlowEventConstants.SHOULD_RETRY_FIELD, true);
      state.setProp(JobStatusRetriever.EVENT_NAME_FIELD, ExecutionStatus.PENDING_RETRY.name());
      state.removeProp(TimingEvent.JOB_END_TIME);
    }
  }

  /**
   * Merge states based on precedence defined by {@link #ORDERED_EXECUTION_STATUSES}.
   * The state instance in the 1st argument reflects the more recent state of a job
   * (and is thus, given higher priority) compared to the 2nd argument.
   * @param state higher priority state
   * @param fallbackState lower priority state
   * @return merged state
   */
  private static org.apache.gobblin.configuration.State mergeState(org.apache.gobblin.configuration.State state,
      org.apache.gobblin.configuration.State fallbackState) {
    Properties mergedState = new Properties();

    mergedState.putAll(fallbackState.getProperties());
    mergedState.putAll(state.getProperties());

    return new org.apache.gobblin.configuration.State(mergedState);
  }

  private void emitWorkUnitCountMetric(GobblinTrackingEvent event) {
    Properties properties = new Properties();
    properties.putAll(event.getMetadata());

    Long numWorkUnits = Long.parseLong(properties.getProperty(CountEventBuilder.COUNT_KEY));
    String workUnitCountName = MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,
        properties.getProperty(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD),
        properties.getProperty(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD),
        JobEvent.WORK_UNITS_CREATED);

    SortedMap<String, Gauge> existingGauges = this.getMetricContext().getGauges(
        (name, metric) -> name.equals(workUnitCountName));

    // If gauge for this flow name and group exists, then value will be updated by reference. Otherwise create
    // a new gauge and save a reference to the value in the HashMap
    this.flowNameGroupToWorkUnitCount.put(workUnitCountName, numWorkUnits);
    if (existingGauges.isEmpty()) {
      ContextAwareGauge gauge = this.getMetricContext().newContextAwareGauge(workUnitCountName,
          () -> this.flowNameGroupToWorkUnitCount.get(workUnitCountName));
      this.getMetricContext().register(workUnitCountName, gauge);
    }

   // Schedule this gauge to be removed after emission
    Runnable task = new RunnableWithMetricContext(this.getMetricContext(), workUnitCountName);
    ScheduledFuture<?> future = this.scheduler.schedule(task, EXPECTED_TIME_FOR_METRICS_REPORTING, TimeUnit.SECONDS);
    this.futuresList.add(future);;
  }

  public static String jobStatusTableName(String flowExecutionId, String jobGroup, String jobName) {
    return Joiner.on(ServiceConfigKeys.STATE_STORE_KEY_SEPARATION_CHARACTER).join(flowExecutionId, jobGroup, jobName, ServiceConfigKeys.STATE_STORE_TABLE_SUFFIX);
  }

  public static String jobStatusTableName(long flowExecutionId, String jobGroup, String jobName) {
    return jobStatusTableName(String.valueOf(flowExecutionId), jobGroup, jobName);
  }

  public static String jobStatusStoreName(String flowGroup, String flowName) {
    return Joiner.on(ServiceConfigKeys.STATE_STORE_KEY_SEPARATION_CHARACTER).join(flowGroup, flowName);
  }

  public static long getExecutionIdFromTableName(String tableName) {
    return Long.parseLong(Splitter.on(ServiceConfigKeys.STATE_STORE_KEY_SEPARATION_CHARACTER).splitToList(tableName).get(0));
  }

  protected abstract GobblinTrackingEvent deserializeEvent(DecodeableKafkaRecord<byte[],byte[]> message);

  protected abstract org.apache.gobblin.configuration.State parseJobStatus(GobblinTrackingEvent event);

}

@Slf4j
class RunnableWithMetricContext implements Runnable {
  private MetricContext _metricContext;
  private String gaugeToRemove;

  public RunnableWithMetricContext(MetricContext providedContext, String gaugeName) {
    this._metricContext = providedContext;
    this.gaugeToRemove = gaugeName;
  }

  public void run() {
    this._metricContext.remove(this.gaugeToRemove);
  }
}
