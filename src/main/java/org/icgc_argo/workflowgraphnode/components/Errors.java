package org.icgc_argo.workflowgraphnode.components;

import com.pivotal.rabbitmq.stream.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.icgc_argo.workflow_graph_lib.exceptions.CommittableException;
import org.icgc_argo.workflow_graph_lib.exceptions.GraphException;
import org.icgc_argo.workflow_graph_lib.exceptions.NotAcknowledgeableException;
import org.icgc_argo.workflow_graph_lib.exceptions.RequeueableException;
import org.icgc_argo.workflowgraphnode.logging.GraphLogger;

import java.util.function.BiConsumer;

import static org.icgc_argo.workflowgraphnode.service.GraphTransitAuthority.*;

@Slf4j
public class Errors {

  /**
   * Biconsumer for onErrorContinue handlers which handles transaction based on Exception rules
   * (rules: https://github.com/icgc-argo/workflow-graph-lib#exceptions)
   *
   * <p>Notes:
   *
   * <p>- onErrorContinue is only supported with some operators, look for operators marked with
   * "Error Mode Support" in reactor documentation.
   *
   * <p>- onErrorContinue behaves differently with Mono since there is only one and nothing to
   * continue. See Mono doc for more info:
   * https://projectreactor.io/docs/core/3.3.2.RELEASE/api/reactor/core/publisher/Mono.html#onErrorContinue-java.lang.Class-java.util.function.BiConsumer-
   */
  public static BiConsumer<Throwable, Object> handle() {
    return (Throwable throwable, Object obj) -> {
      if (throwable instanceof GraphException && obj instanceof Transaction<?>) {
        handleGraphError((GraphException) throwable, (Transaction<?>) obj);
      } else if (obj instanceof Transaction<?>) {
        rejectTransactionOnException(throwable, (Transaction<?>) obj);
      } else {
        log.error("Encountered Error that cannot be handled with GraphExceptions.", throwable);
      }
    };
  }

  private static void handleGraphError(GraphException exception, Transaction<?> transaction) {
    if (exception instanceof CommittableException) {
      GraphLogger.error(transaction, "CommittableException when processing: %s", transaction.get());
      GraphLogger.error(transaction, "Nested Exception: %s", exception);
      commitAndRemoveTransactionFromGTA(transaction);
    } else if (exception instanceof RequeueableException) {
      GraphLogger.error(transaction, "RequeableException when processing: %s", transaction.get());
      GraphLogger.error(transaction, "Nested Exception: %s", exception);
      requeueAndRemoveTransactionFromGTA(transaction);
    } else if (exception instanceof NotAcknowledgeableException) {
      GraphLogger.error(transaction, "Encountered NotAcknowledgeableException: %s", exception);
    } else {
      GraphLogger.error(
          transaction,
          "Putting transaction %s, with exception type: %s on dlx",
          transaction.get(),
          exception.getClass());
      GraphLogger.error(transaction, "Nested Exception: %s", exception);
      rejectAndRemoveTransactionFromGTA(transaction);
    }
  }

  private static void rejectTransactionOnException(
      Throwable throwable, Transaction<?> transaction) {
    GraphLogger.error(
        transaction,
        "Encountered Exception that is not mappable to GraphException. Rejecting Transaction. Exception: %s",
        throwable);
    rejectAndRemoveTransactionFromGTA(transaction);
  }
}
