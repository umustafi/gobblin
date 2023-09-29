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

package org.apache.gobblin.runtime.api;

import com.typesafe.config.Config;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.gobblin.runtime.api.MysqlMultiActiveLeaseArbiter.*;

@Slf4j
public class MysqlMultiActiveLeaseArbiterTest {
  private static final int EPSILON = 10000;
  private static final int LINGER = 50000;
  private static final int TOLERABLE_DELTA = (int) (LINGER * 0.25);
  private static final String USER = "testUser";
  private static final String PASSWORD = "testPassword";
  private static final String TABLE = "mysql_multi_active_lease_arbiter_store";
  private static final String flowGroup = "testFlowGroup";
  private static final String flowName = "testFlowName";
  private static final String flowExecutionId = "12345677";
  // The following are considered unique because they correspond to different flow action types
  private static DagActionStore.DagAction launchDagAction =
      new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, DagActionStore.FlowActionType.LAUNCH);
  private static DagActionStore.DagAction resumeDagAction =
      new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, DagActionStore.FlowActionType.RESUME);
  private static final long eventTimeMillis = System.currentTimeMillis();
  private static final Timestamp eventTimestamp = new Timestamp(eventTimeMillis);
  private static final long secondEventTimeMillis = eventTimeMillis + (int) (EPSILON * 1.1);
  private static final long thirdEventTimeMillis = secondEventTimeMillis + (int) (EPSILON * 1.1);
  private static final Optional<Long> dummyTimeValue = Optional.of(9999L);
  private MysqlMultiActiveLeaseArbiter mysqlMultiActiveLeaseArbiter;
  private String formattedAcquireLeaseIfMatchingAllStatement =
      String.format(CONDITIONALLY_ACQUIRE_LEASE_IF_MATCHING_ALL_COLS_STATEMENT, TABLE);
  private String formattedAcquireLeaseIfFinishedStatement =
      String.format(CONDITIONALLY_ACQUIRE_LEASE_IF_FINISHED_LEASING_STATEMENT, TABLE);

  // The setup functionality verifies that the initialization of the tables is done correctly and verifies any SQL
  // syntax errors.
  @BeforeClass
  public void setUp() throws Exception {
    ITestMetastoreDatabase testDb = TestMetastoreDatabaseFactory.get();

    Config config = ConfigBuilder.create()
        .addPrimitive(ConfigurationKeys.SCHEDULER_EVENT_EPSILON_MILLIS_KEY, EPSILON)
        .addPrimitive(ConfigurationKeys.SCHEDULER_EVENT_LINGER_MILLIS_KEY, LINGER)
        .addPrimitive(ConfigurationKeys.MYSQL_LEASE_ARBITER_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_URL_KEY, testDb.getJdbcUrl())
        .addPrimitive(ConfigurationKeys.MYSQL_LEASE_ARBITER_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_USER_KEY, USER)
        .addPrimitive(ConfigurationKeys.MYSQL_LEASE_ARBITER_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, PASSWORD)
        .addPrimitive(ConfigurationKeys.SCHEDULER_LEASE_DETERMINATION_STORE_DB_TABLE_KEY, TABLE)
        .build();

    this.mysqlMultiActiveLeaseArbiter = new MysqlMultiActiveLeaseArbiter(config);
  }

  /*
     Tests all cases of trying to acquire a lease (CASES 1-6 detailed below) for a flow action event with one
     participant involved.
  */
  // TODO: refactor this to break it into separate test cases as much is possible
  @Test
  public void testAcquireLeaseSingleParticipant() throws Exception {
    Assert.assertEquals(eventTimeMillis, eventTimestamp.getTime());
    // Tests CASE 1 of acquire lease for a flow action event not present in DB
    MultiActiveLeaseArbiter.LeaseAttemptStatus firstLaunchStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchDagAction, eventTimeMillis, false);
    Assert.assertTrue(firstLaunchStatus instanceof MultiActiveLeaseArbiter.LeaseObtainedStatus);
    MultiActiveLeaseArbiter.LeaseObtainedStatus firstObtainedStatus =
        (MultiActiveLeaseArbiter.LeaseObtainedStatus) firstLaunchStatus;
    Assert.assertEquals(firstObtainedStatus.getEventTimestamp(), eventTimeMillis);
    Assert.assertTrue(firstObtainedStatus.getEventTimestamp() <=
        firstObtainedStatus.getLeaseAcquisitionTimestamp());
    Assert.assertTrue(firstObtainedStatus.getFlowAction().equals(
        new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, DagActionStore.FlowActionType.LAUNCH)));

    // Verify that different DagAction types for the same flow can have leases at the same time
    DagActionStore.DagAction killDagAction = new
        DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, DagActionStore.FlowActionType.KILL);
    MultiActiveLeaseArbiter.LeaseAttemptStatus killStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(killDagAction, eventTimeMillis, false);
    Assert.assertTrue(killStatus instanceof MultiActiveLeaseArbiter.LeaseObtainedStatus);
    MultiActiveLeaseArbiter.LeaseObtainedStatus killObtainedStatus =
        (MultiActiveLeaseArbiter.LeaseObtainedStatus) killStatus;
    Assert.assertTrue(
        killObtainedStatus.getLeaseAcquisitionTimestamp() >= killObtainedStatus.getEventTimestamp());

    // Tests CASE 2 of acquire lease for a flow action event that already has a valid lease for the same event in db
    // Utilize the same event timestamp as the launch above so this call will be considered a duplicate of the previous
    MultiActiveLeaseArbiter.LeaseAttemptStatus secondLaunchStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchDagAction, eventTimeMillis, false);
    Assert.assertTrue(secondLaunchStatus instanceof MultiActiveLeaseArbiter.LeasedToAnotherStatus);
    MultiActiveLeaseArbiter.LeasedToAnotherStatus secondLeasedToAnotherStatus =
        (MultiActiveLeaseArbiter.LeasedToAnotherStatus) secondLaunchStatus;
    Assert.assertTrue(secondLeasedToAnotherStatus.getEventTimeMillis() == firstObtainedStatus.getEventTimestamp());
    Assert.assertTrue(secondLeasedToAnotherStatus.getMinimumLingerDurationMillis() > 0);

    // Tests CASE 3 of trying to acquire a lease for a distinct flow action event, while the previous event's lease is
    // valid (since not much time has passed since the previous launch)
    MultiActiveLeaseArbiter.LeaseAttemptStatus thirdLaunchStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchDagAction, secondEventTimeMillis, false);
    Assert.assertTrue(thirdLaunchStatus instanceof MultiActiveLeaseArbiter.LeasedToAnotherStatus);
    MultiActiveLeaseArbiter.LeasedToAnotherStatus thirdLeasedToAnotherStatus =
        (MultiActiveLeaseArbiter.LeasedToAnotherStatus) thirdLaunchStatus;
    Assert.assertEquals(eventTimeMillis, thirdLeasedToAnotherStatus.getEventTimeMillis());
    Assert.assertEquals(thirdLeasedToAnotherStatus.getMinimumLingerDurationMillis(), LINGER, TOLERABLE_DELTA);

    // Tests CASE 4 of lease out of date
    // by trying to acquire a lease for a previously used event time
    Thread.sleep(LINGER);
    MultiActiveLeaseArbiter.LeaseAttemptStatus fourthLaunchStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchDagAction, eventTimeMillis, false);
    Assert.assertTrue(fourthLaunchStatus instanceof MultiActiveLeaseArbiter.LeaseObtainedStatus);
    MultiActiveLeaseArbiter.LeaseObtainedStatus fourthObtainedStatus =
        (MultiActiveLeaseArbiter.LeaseObtainedStatus) fourthLaunchStatus;
    Assert.assertEquals(eventTimeMillis, fourthObtainedStatus.getEventTimestamp());
    Assert.assertTrue(fourthObtainedStatus.getEventTimestamp()
        <= fourthObtainedStatus.getLeaseAcquisitionTimestamp());

    // Tests CASE 5 of no longer leasing the same event in DB
    // by trying to acquire a lease on an event timestamp that has been completed
    Assert.assertTrue(mysqlMultiActiveLeaseArbiter.recordLeaseSuccess(fourthObtainedStatus));
    MultiActiveLeaseArbiter.LeaseAttemptStatus fifthLaunchStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchDagAction, eventTimeMillis, false);
    Assert.assertTrue(fifthLaunchStatus instanceof MultiActiveLeaseArbiter.NoLongerLeasingStatus);

    // Tests CASE 6 of no longer leasing a distinct event in DB
    // acquire a lease on a third timestamp (at least epsilon away from the second)
    MultiActiveLeaseArbiter.LeaseAttemptStatus sixthLaunchStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchDagAction, thirdEventTimeMillis, false);
    Assert.assertTrue(sixthLaunchStatus instanceof MultiActiveLeaseArbiter.LeaseObtainedStatus);
    MultiActiveLeaseArbiter.LeaseObtainedStatus sixthObtainedStatus =
        (MultiActiveLeaseArbiter.LeaseObtainedStatus) sixthLaunchStatus;
    Assert.assertEquals(sixthObtainedStatus.getEventTimestamp(), thirdEventTimeMillis);
    Assert.assertTrue(sixthObtainedStatus.getEventTimestamp()
        <= sixthObtainedStatus.getLeaseAcquisitionTimestamp());
  }

  /*
     Tests attemptLeaseIfNewRow() method to ensure a new row is inserted if no row matches the primary key in the table.
     If such a row does exist, the method should disregard the resulting SQL error and return 0 rows updated, indicating
     the lease was not acquired.
     Note: this isolates and tests CASE 1 in which another participant could have acquired the lease between the time
     the read was done and subsequent write was carried out
  */
  @Test (dependsOnMethods = "testAcquireLeaseSingleParticipant")
  public void testAcquireLeaseIfNewRow() throws IOException {
    // Inserting the first time should update 1 row
    Assert.assertEquals(this.mysqlMultiActiveLeaseArbiter.attemptLeaseIfNewRow(resumeDagAction, eventTimeMillis), 1);
    // Inserting the second time should not update any rows
    Assert.assertEquals(this.mysqlMultiActiveLeaseArbiter.attemptLeaseIfNewRow(resumeDagAction, eventTimeMillis), 0);
  }

    /*
    Tests CONDITIONALLY_ACQUIRE_LEASE_IF_MATCHING_ALL_COLS_STATEMENT to ensure insertion is not completed if another
    participant updated the table between the prior reed and attempted insertion.
    Note: this isolates and tests CASE 4 in which a flow action event has an out of date lease, so a participant
    attempts a new one given the table the eventTimestamp and leaseAcquisitionTimestamp values are unchanged.
   */
  @Test (dependsOnMethods = "testAcquireLeaseIfNewRow")
  public void testConditionallyAcquireLeaseIfFMatchingAllColsStatement() throws IOException {
    MysqlMultiActiveLeaseArbiter.SelectInfoResult selectInfoResult =
        this.mysqlMultiActiveLeaseArbiter.getRowInfo(resumeDagAction);

    // The following insert will fail since the eventTimestamp does not match
    int numRowsUpdated = this.mysqlMultiActiveLeaseArbiter.attemptLeaseIfExistingRow(
        formattedAcquireLeaseIfMatchingAllStatement, resumeDagAction, secondEventTimeMillis, true,
        true, dummyTimeValue, selectInfoResult.getLeaseAcquisitionTimeMillis());
    Assert.assertEquals(numRowsUpdated, 0);

    // The following insert will fail since the leaseAcquisitionTimestamp does not match
    numRowsUpdated = this.mysqlMultiActiveLeaseArbiter.attemptLeaseIfExistingRow(
        formattedAcquireLeaseIfMatchingAllStatement, resumeDagAction, secondEventTimeMillis, true,
        true, Optional.of(selectInfoResult.getEventTimeMillis()), dummyTimeValue);
    Assert.assertEquals(numRowsUpdated, 0);

    // This insert should work since the values match all the columns
    numRowsUpdated = this.mysqlMultiActiveLeaseArbiter.attemptLeaseIfExistingRow(
        formattedAcquireLeaseIfMatchingAllStatement, resumeDagAction, secondEventTimeMillis, true,
        true, Optional.of(selectInfoResult.getEventTimeMillis()),
        selectInfoResult.getLeaseAcquisitionTimeMillis());
    Assert.assertEquals(numRowsUpdated, 1);
  }

  /*
  Tests CONDITIONALLY_ACQUIRE_LEASE_IF_FINISHED_LEASING_STATEMENT to ensure the insertion will only succeed if another
  participant has not updated the eventTimestamp state since the prior read.
  Note: This isolates and tests CASE 6 during which current participant saw a distinct flow action event had completed
  its prior lease, encouraging the current participant to acquire a lease for its event.
   */
  @Test (dependsOnMethods = "testConditionallyAcquireLeaseIfFMatchingAllColsStatement")
  public void testConditionallyAcquireLeaseIfFinishedLeasingStatement()
      throws IOException, InterruptedException, SQLException {
    // Mark the resume action lease from above as completed by fabricating a LeaseObtainedStatus
    MysqlMultiActiveLeaseArbiter.SelectInfoResult selectInfoResult =
        this.mysqlMultiActiveLeaseArbiter.getRowInfo(resumeDagAction);
    boolean markedSuccess = this.mysqlMultiActiveLeaseArbiter.recordLeaseSuccess(new LeaseObtainedStatus(
        resumeDagAction, selectInfoResult.getEventTimeMillis(), selectInfoResult.getLeaseAcquisitionTimeMillis().get()));
    Assert.assertTrue(markedSuccess);
    // Ensure no NPE results from calling this after a lease has been completed and acquisition timestamp val is NULL
    mysqlMultiActiveLeaseArbiter.evaluateStatusAfterLeaseAttempt(1, resumeDagAction, Optional.empty());

    // Sleep enough time for event to be considered distinct
    Thread.sleep(LINGER);

    // The following insert will fail since eventTimestamp does not match the expected
    int numRowsUpdated = this.mysqlMultiActiveLeaseArbiter.attemptLeaseIfExistingRow(
        formattedAcquireLeaseIfFinishedStatement, resumeDagAction, thirdEventTimeMillis, true,
        false, dummyTimeValue, Optional.empty());
    Assert.assertEquals(numRowsUpdated, 0);

    // This insert does match since we utilize the right eventTimestamp
    numRowsUpdated = this.mysqlMultiActiveLeaseArbiter.attemptLeaseIfExistingRow(
        formattedAcquireLeaseIfFinishedStatement, resumeDagAction, thirdEventTimeMillis, true,
        false, Optional.of(selectInfoResult.getEventTimeMillis()), Optional.empty());
    Assert.assertEquals(numRowsUpdated, 1);
    // Complete this event as done above
    selectInfoResult = this.mysqlMultiActiveLeaseArbiter.getRowInfo(resumeDagAction);
    markedSuccess = this.mysqlMultiActiveLeaseArbiter.recordLeaseSuccess(new LeaseObtainedStatus(
        resumeDagAction, selectInfoResult.getEventTimeMillis(), selectInfoResult.getLeaseAcquisitionTimeMillis().get()));
    Assert.assertTrue(markedSuccess);
  }

  // Tests trying to acquire a lease with a reminder event time exactly matching a lease that's already completed
  @Test (dependsOnMethods = "testConditionallyAcquireLeaseIfFinishedLeasingStatement")
  public void testAcquireLeaseForCompletedReminderEvent() throws IOException {
    // Reminder event checks on the resume event with `thirdEventTimeMillis` completed in
    // `testConditionallyAcquireLeaseIfFMatchingAllColsStatement` above
    MultiActiveLeaseArbiter.LeaseAttemptStatus reminderStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(resumeDagAction, thirdEventTimeMillis, true);
    Assert.assertTrue(reminderStatus instanceof MultiActiveLeaseArbiter.NoLongerLeasingStatus);
  }

  // Tests trying to acquire a lease with a reminder event time within epsilon of a lease that's already completed but
  // not exactly matching the event time
  @Test (dependsOnMethods = "testConditionallyAcquireLeaseIfFinishedLeasingStatement")
  public void testAcquireLeaseForCompletedDuplicateReminderEvent() throws IOException {
    // Reminder event checks on the resume event with time slightly older than (within epsilon) `thirdEventTimeMillis`
    // completed in `testConditionallyAcquireLeaseIfFMatchingAllColsStatement` above
    long withinEpsilonThirdEventTimeMillis = thirdEventTimeMillis - EPSILON/3;
    MultiActiveLeaseArbiter.LeaseAttemptStatus reminderStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(resumeDagAction, withinEpsilonThirdEventTimeMillis, true);
    Assert.assertTrue(reminderStatus instanceof MultiActiveLeaseArbiter.NoLongerLeasingStatus);
  }

  // Tests trying to acquire a lease with a reminder event time way prior to the completed event in the table. It should
  // be ignored since it's way in the past
  @Test (dependsOnMethods = "testConditionallyAcquireLeaseIfFinishedLeasingStatement")
  public void testAcquireLeaseForExtremelyOldReminderEvent() throws IOException {
    // Reminder event checks on the resume event with time slightly older than (within epsilon) `thirdEventTimeMillis`
    // completed in `testConditionallyAcquireLeaseIfFMatchingAllColsStatement` above
    long withinEpsilonThirdEventTimeMillis = thirdEventTimeMillis - EPSILON/3;
    MultiActiveLeaseArbiter.LeaseAttemptStatus reminderStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(resumeDagAction, withinEpsilonThirdEventTimeMillis, true);
    Assert.assertTrue(reminderStatus instanceof MultiActiveLeaseArbiter.NoLongerLeasingStatus);
  }
}
