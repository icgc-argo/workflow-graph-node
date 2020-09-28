package org.icgc_argo.workflowgraphnode.components;

import com.pivotal.rabbitmq.stream.Transaction;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.icgc_argo.workflow_graph_lib.exceptions.CommittableException;
import org.icgc_argo.workflow_graph_lib.exceptions.GraphException;
import org.icgc_argo.workflow_graph_lib.exceptions.NotAcknowledgeableException;
import org.icgc_argo.workflow_graph_lib.exceptions.RequeueableException;

@Slf4j
public class Errors {

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
      log.error("CommitableException when processing: {}", transaction.get().toString());
      log.error("Nested Exception", exception);
      transaction.commit();
    } else if (exception instanceof RequeueableException) {
      log.error("RequeableException when processing: {}", transaction.get().toString());
      log.error("Nested Exception", exception);
      transaction.rollback(true);
    } else if (exception instanceof NotAcknowledgeableException) {
      log.error("Encountered NotAcknowledgeableException", exception);
    } else {
      log.error(
          "Putting transaction {}, with exception type: {} on dlx",
          transaction.get().toString(),
          exception.getClass());
      log.error("Nested Exception", exception);
      transaction.reject();
    }
  }

  private static void rejectTransactionOnException(
      Throwable throwable, Transaction<?> transaction) {
    log.error(
        "Encountered Exception that is not mappable to GraphException. Rejecting Transaction.",
        throwable);
    transaction.reject();
  }
}
