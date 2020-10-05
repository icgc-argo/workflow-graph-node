package org.icgc_argo.workflowgraphnode.components;

import com.pivotal.rabbitmq.stream.Transaction;
import java.util.List;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc_argo.workflow_graph_lib.exceptions.DeadLetterQueueableException;
import org.icgc_argo.workflow_graph_lib.workflow.client.RdpcClient;
import reactor.core.publisher.SynchronousSink;

@Slf4j
public class Workflows {

  /** Constants */
  private static final List<String> ROLLBACK =
      List.of("UNKNOWN", "QUEUED", "INITIALIZING", "RUNNING", "PAUSED");

  private static final List<String> NEXT = List.of("COMPLETE");

  private static final List<String> REJECT = List.of("EXECUTOR_ERROR", "SYSTEM_ERROR", "FAILED");

  private static final List<String> COMMIT = List.of("CANCELED", "CANCELING");

  public static BiConsumer<Transaction<String>, SynchronousSink<Transaction<String>>>
      handleRunStatus(RdpcClient rdpcClient) {
    return (tx, sink) -> {
      log.debug("Checking status for: {}", tx.get());
      val status = rdpcClient.getWorkflowStatus(tx.get());
      status.subscribe(
          s -> {
            if (ROLLBACK.contains(s)) {
              log.debug("Requeueing {} with status {}", tx.get(), s);
              tx.rollback(true);
            } else if (NEXT.contains(s)) {
              log.debug("Nexting {} with status {}", tx.get(), s);
              sink.next(tx);
            } else if (REJECT.contains(s)) {
              log.debug("Rejecting {} with status {}", tx.get(), s);
              tx.reject();
            } else if (COMMIT.contains(s)) {
              log.debug("Commiting {} with status {}", tx.get(), s);
              tx.commit();
            } else {
              log.error("Cannot map workflow status for run: {}.", tx.get());
              sink.error(new DeadLetterQueueableException(tx.get()));
            }
          });
    };
  }
}
