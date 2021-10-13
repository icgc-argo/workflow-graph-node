package org.icgc_argo.workflowgraphnode.components;

import com.pivotal.rabbitmq.stream.Transaction;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.icgc_argo.workflow_graph_lib.exceptions.CommittableException;
import org.icgc_argo.workflow_graph_lib.exceptions.DeadLetterQueueableException;
import org.icgc_argo.workflow_graph_lib.exceptions.RequeueableException;
import org.icgc_argo.workflow_graph_lib.schema.GraphEvent;
import org.icgc_argo.workflow_graph_lib.schema.GraphRun;
import org.icgc_argo.workflow_graph_lib.workflow.client.RdpcClient;
import org.icgc_argo.workflow_graph_lib.workflow.model.RunRequest;
import org.icgc_argo.workflowgraphnode.logging.GraphLogger;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

public class Workflows {

  /** Constants */
  private static final List<String> ROLLBACK =
      List.of("UNKNOWN", "QUEUED", "INITIALIZING", "RUNNING", "PAUSED");

  private static final List<String> NEXT = List.of("COMPLETE");

  private static final List<String> REJECT = List.of("EXECUTOR_ERROR", "SYSTEM_ERROR", "FAILED");

  private static final List<String> COMMIT = List.of("CANCELED", "CANCELING");

  public static Function<Transaction<RunRequest>, Publisher<? extends Transaction<GraphRun>>>
      startRuns(RdpcClient rdpcClient) {
    // flatMap needs a function that returns a Publisher that it then
    // resolves async by subscribing to it (ex. mono)
    return tx ->
        rdpcClient
            .startRun(tx.get())
            .flatMap(
                response ->
                    Mono.fromCallable(
                        () -> tx.map(new GraphRun(UUID.randomUUID().toString(), response))));
  }

  public static Function<Transaction<GraphRun>, Mono<Transaction<GraphRun>>> handleRunStatus(
      RdpcClient rdpcClient) {
    return (tx) -> {
      GraphLogger.debug(tx, "Checking status for: %s", tx.get().getRunId());
      return rdpcClient
          .getWorkflowStatus(tx.get().getRunId())
          .flatMap(
              s -> {
                if (ROLLBACK.contains(s)) {
                  GraphLogger.debug(tx, "Requeueing %s with status %s", tx.get().getRunId(), s);
                  return Mono.error(new RequeueableException(tx.get().getRunId(), tx));
                } else if (NEXT.contains(s)) {
                  GraphLogger.debug(tx, "Nexting %s with status %s", tx.get().getRunId(), s);
                  return Mono.just(tx);
                } else if (REJECT.contains(s)) {
                  GraphLogger.debug(tx, "Rejecting %s with status %s", tx.get().getRunId(), s);
                  return Mono.error(new DeadLetterQueueableException(tx.get().getRunId(), tx));
                } else if (COMMIT.contains(s)) {
                  GraphLogger.debug(tx, "Committing %s with status %s", tx.get().getRunId(), s);
                  return Mono.error(new CommittableException(tx.get().getRunId(), tx));
                } else {
                  GraphLogger.error(
                      tx, "Cannot map workflow status for run: %s.", tx.get().getRunId());
                  return Mono.error(new DeadLetterQueueableException(tx.get().getRunId(), tx));
                }
              });
    };
  }

  public static Function<Transaction<GraphRun>, Publisher<? extends Transaction<GraphEvent>>>
      runAnalysesToGraphEvent(RdpcClient rdpcClient) {
    return tx ->
        rdpcClient
            .createGraphEventsForRun(tx.get().getRunId())
            .onErrorContinue(Errors.handle())
            .flatMapIterable(
                response ->
                    response.stream()
                        .map(tx::spawn)
                        .collect(
                            Collectors.collectingAndThen(
                                Collectors.toList(),
                                list -> {
                                  // commit the parent transaction after spawning children
                                  // (won't be fully committed until each child is
                                  // committed once it is sent to the complete exchange)
                                  tx.commit();
                                  return list;
                                })));
  }
}
