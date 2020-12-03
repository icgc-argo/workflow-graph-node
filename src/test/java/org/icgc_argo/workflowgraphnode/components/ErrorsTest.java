package org.icgc_argo.workflowgraphnode.components;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc_argo.workflow_graph_lib.exceptions.CommittableException;
import org.icgc_argo.workflow_graph_lib.exceptions.DeadLetterQueueableException;
import org.icgc_argo.workflow_graph_lib.exceptions.NotAcknowledgeableException;
import org.icgc_argo.workflow_graph_lib.exceptions.RequeueableException;
import org.icgc_argo.workflowgraphnode.service.GraphTransitAuthority;
import org.icgc_argo.workflowgraphnode.util.TransactionUtils;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc_argo.workflowgraphnode.util.TransactionUtils.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ActiveProfiles("test")
@Slf4j
public class ErrorsTest {

  private final GraphTransitAuthority graphTransitAuthority;

  @SneakyThrows
  public ErrorsTest() {
    this.graphTransitAuthority = new GraphTransitAuthority("test-pipeline", "test-node");
  }

  @Test
  public void testErrorHandler() {
    val handler = Errors.handle();

    val runnableThrowers =
        List.<Runnable>of(
            () -> {
              throw new CommittableException();
            },
            () -> {
              throw new RequeueableException();
            },
            () -> {
              throw new NotAcknowledgeableException();
            },
            () -> {
              throw new DeadLetterQueueableException();
            });

    val source =
        Flux.fromIterable(runnableThrowers)
            .map(TransactionUtils::wrapWithTransaction)
            .doOnNext(graphTransitAuthority::registerNonEntityTx)
            .flatMap(
                callableFunc -> {
                  callableFunc.get().run(); // will throw error on run
                  return Mono.empty();
                })
            .onErrorContinue(handler);

    Consumer<Collection<Object>> expectedDiscardBehavior =
        discarded -> {
          // Discarded elements are input as Objects, but they really are Transactions
          val discardedTransactions = discarded.toArray();

          assertThat(discardedTransactions.length).isEqualTo(4);

          assertTrue(isAcknowledged(discardedTransactions[0]));
          assertTrue(isRequeued(discardedTransactions[1]));
          assertTrue(isNotAcknowledged(discardedTransactions[2]));
          assertTrue(isRejected(discardedTransactions[3]));
        };

    StepVerifier.create(source)
        .expectComplete()
        .verifyThenAssertThat()
        .hasNotDroppedElements()
        .hasNotDroppedErrors()
        .hasDiscardedElementsSatisfying(expectedDiscardBehavior);
  }
}
