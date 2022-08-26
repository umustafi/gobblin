package org.apache.gobblin.service.monitoring;

import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.runtime.kafka.HighLevelConsumer;


public class SpecStoreChangeMonitor extends HighLevelConsumer {

  public SpecStoreChangeMonitor(String topic, Config config, int numThreads) {
    super(topic, config, numThreads);
  }

  @Override
  protected void processMessage(DecodeableKafkaRecord message) {
    // the message value contains a SpecStoreChangeEvent with all this info necessary
    // expects operation, key, timestamp for de-dup, what is operation for?
//
//    if (specStoreChangeEvent == null) {
//      return;
//    }

    // TODO: handle the API requests
    /** if CREATE
     look for onAddSpec listener and see what it does
     FlowConfigV2ResourceLocalHandler puts it in flowCatalog.put()
     GobblinServiceJobScheduler.addSpec notices new specs from flowCatalog then actually schedules it and stuff
     if UPDATE
     FlowConfigResourceLocalHandler updateFlowConfig also puts it in flowCatalog and follows same format to addSpec
     otherwise just update Schedule through Orchestrator
     if DELETE
     FlowConfigResourceLocalHandler deletes from flowCatalog with flowCatalog.remove() and triggers listeners
     **/
    // do we handle deletes and resume here? no bc different
  }
}