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

import java.io.IOException;

import lombok.Data;


/**
 * This interface defines a generic approach to a non-blocking, multiple active thread or host system, in which one or
 * more active participants compete to take responsibility for a particular flow's event. The type of flow event in
 * question does not impact the algorithm other than to uniquely identify the flow event. Each participant uses the
 * interface to initiate an attempt at ownership over the flow event and receives a response indicating the status of
 * the attempt.
 *
 * At a high level the lease arbiter works as follows:
 *  1. Multiple participants independently learn of a flow action event to act upon
 *  2. Each participant attempts to acquire rights or `a lease` to be the sole participant acting on the event by
 *     calling the tryAcquireLease method below and receives the resulting status. The status indicates whether this
 *     participant has
 *        a) LeaseObtainedStatus -> this participant will attempt to carry out the required action before the lease expires
 *        b) LeasedToAnotherStatus -> another will attempt to carry out the required action before the lease expires
 *        c) NoLongerLeasingStatus -> flow event no longer needs to be acted upon (terminal state)
 *  3. If another participant has acquired the lease before this one could, then the present participant must check back
 *    in at the time of lease expiry to see if it needs to attempt the lease again [status (b) above]. We refer to this
 *    check-in as a 'reminder event'.
 *  4. Once the participant which acquired the lease completes its work on the flow event, it calls recordLeaseSuccess
 *    to indicate to all other participants that the flow event no longer needs to be acted upon [status (c) above]
 */
public interface MultiActiveLeaseArbiter {
  /**
   * This method attempts to insert an entry into store for a particular flow action event if one does not already
   * exist in the store for the flow action or has expired. Regardless of the outcome it also reads the lease
   * acquisition timestamp of the entry for that flow action event (it could have pre-existed in the table or been newly
   * added by the previous write). Based on the transaction results, it will return {@link LeaseAttemptStatus} to
   * determine the next action.
   * @param flowAction uniquely identifies the flow and the present action upon it
   * @param eventTimeMillis is the time this flow action was triggered
   * @param isReminderEvent true if the flow action event we're checking on is a reminder event
   * @param adoptConsensusFlowExecutionId if true then replaces the flowAction flowExecutionId returned in
   *                                      LeaseAttemptStatuses with the consensus eventTime
   *
   * @return LeaseAttemptStatus
   * @throws IOException
   */
  LeaseAttemptStatus tryAcquireLease(DagActionStore.DagAction flowAction, long eventTimeMillis, boolean isReminderEvent,
      boolean adoptConsensusFlowExecutionId)
      throws IOException;

  /**
   * This method is used to indicate the owner of the lease has successfully completed required actions while holding
   * the lease of the flow action event. It marks the lease as "no longer leasing", if the eventTimeMillis and
   * leaseAcquisitionTimeMillis values have not changed since this owner acquired the lease (indicating the lease did
   * not expire).
   * @return true if successfully updated, indicating no further actions need to be taken regarding this event.
   *         false if failed to update the lease properly, the caller should continue seeking to acquire the lease as
   *         if any actions it did successfully accomplish, do not count
   */
  boolean recordLeaseSuccess(LeaseObtainedStatus status) throws IOException;

  /*
   Class used to encapsulate status of lease acquisition attempt and derivations should contain information specific to
   the status that results.
   */
  abstract class LeaseAttemptStatus {}

  class NoLongerLeasingStatus extends LeaseAttemptStatus {}


  /** `j.u.Function` variant for an operation that may @throw IOException */
  @FunctionalInterface
  interface FunctionMayThrowIO<T, R> {
    R apply(T t) throws IOException;
  }

  /*
  The participant calling this method acquired the lease for the event in question. `Flow action`'s flow execution id
  is the timestamp associated with the lease and the time the caller obtained the lease is stored within the
  `leaseAcquisitionTimestamp` field. `completeLeaseRunnable` is a reference to a method that will recordLeaseSuccess
  for the current LeaseObtainedStatus from a caller without access to the {@link MultiActiveLeaseArbiter}
  */
  @Data
  class LeaseObtainedStatus extends LeaseAttemptStatus {
    private final DagActionStore.DagAction flowAction;
    private final long leaseAcquisitionTimestamp;
    protected final FunctionMayThrowIO<LeaseObtainedStatus, Boolean> completeLeaseRunnable;

    /**
     * @return event time in millis since epoch for the event of this lease acquisition
     */
    public long getEventTimeMillis() {
      return Long.parseLong(flowAction.getFlowExecutionId());
    }
  }

  /*
  This flow action event already has a valid lease owned by another participant.
  `Flow action`'s flow execution id is the timestamp the lease is associated with, however the flow action event it
  corresponds to may be a different and distinct occurrence of the same event.
  `minimumLingerDurationMillis` is the minimum amount of time to wait before this participant should return to check if
  the lease has completed or expired
   */
  @Data
  class LeasedToAnotherStatus extends LeaseAttemptStatus {
    private final DagActionStore.DagAction flowAction;
    private final long minimumLingerDurationMillis;

    /**
     * Returns event time in millis since epoch for the event whose lease was obtained by another participant.
     * @return
     */
    public long getEventTimeMillis() {
      return Long.parseLong(flowAction.getFlowExecutionId());
    }
}
}
