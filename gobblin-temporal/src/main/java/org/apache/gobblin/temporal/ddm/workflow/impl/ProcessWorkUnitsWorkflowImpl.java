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
package org.apache.gobblin.temporal.ddm.workflow.impl;

import java.util.Optional;

import com.typesafe.config.ConfigFactory;

import io.temporal.api.enums.v1.ParentClosePolicy;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.Workflow;

import org.apache.gobblin.temporal.cluster.WorkerConfig;
import org.apache.gobblin.temporal.ddm.work.WUProcessingSpec;
import org.apache.gobblin.temporal.ddm.work.WorkUnitClaimCheck;
import org.apache.gobblin.temporal.ddm.work.assistance.Help;
import org.apache.gobblin.temporal.ddm.work.styles.FileSystemJobStateful;
import org.apache.gobblin.temporal.ddm.workflow.ProcessWorkUnitsWorkflow;
import org.apache.gobblin.temporal.ddm.work.EagerFsDirBackedWorkUnitClaimCheckWorkload;
import org.apache.gobblin.temporal.util.nesting.work.WorkflowAddr;
import org.apache.gobblin.temporal.util.nesting.work.Workload;
import org.apache.gobblin.temporal.util.nesting.workflow.NestingExecWorkflow;


public class ProcessWorkUnitsWorkflowImpl implements ProcessWorkUnitsWorkflow {
  public static final String CHILD_WORKFLOW_ID_BASE = "NestingExecWorkUnits";

  @Override
  public int process(WUProcessingSpec workSpec) {
    Workload<WorkUnitClaimCheck> workload = createWorkload(workSpec);
    NestingExecWorkflow<WorkUnitClaimCheck> processingWorkflow = createProcessingWorkflow(workSpec);
    return processingWorkflow.performWorkload(
        WorkflowAddr.ROOT, workload, 0,
        workSpec.getTuning().getMaxBranchesPerTree(), workSpec.getTuning().getMaxSubTreesPerTree(), Optional.empty()
    );
  }

  protected Workload<WorkUnitClaimCheck> createWorkload(WUProcessingSpec workSpec) {
    return new EagerFsDirBackedWorkUnitClaimCheckWorkload(workSpec.getFileSystemUri(), workSpec.getWorkUnitsDir());
  }

  protected NestingExecWorkflow<WorkUnitClaimCheck> createProcessingWorkflow(FileSystemJobStateful f) {
    ChildWorkflowOptions childOpts = ChildWorkflowOptions.newBuilder()
        .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON)
        .setWorkflowId(Help.qualifyNamePerExec(CHILD_WORKFLOW_ID_BASE, f, WorkerConfig.of(this).orElse(ConfigFactory.empty())))
        .build();
    // TODO: to incorporate multiple different concrete `NestingExecWorkflow` sub-workflows in the same super-workflow... shall we use queues?!?!?
    return Workflow.newChildWorkflowStub(NestingExecWorkflow.class, childOpts);
  }
}
