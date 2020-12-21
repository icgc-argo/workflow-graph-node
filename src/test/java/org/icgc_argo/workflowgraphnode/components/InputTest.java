package org.icgc_argo.workflowgraphnode.components;

import static org.icgc_argo.workflowgraphnode.util.JacksonUtils.readValue;
import static org.icgc_argo.workflowgraphnode.util.TransactionUtils.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import lombok.SneakyThrows;
import lombok.val;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import org.icgc_argo.workflowgraphnode.service.GraphTransitAuthority;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@ActiveProfiles("test")
public class InputTest {
  private final NodeProperties config;
  private final GraphTransitAuthority graphTransitAuthority;

  @SneakyThrows
  public InputTest() {
    config =
        readValue(
            this.getClass().getResourceAsStream("fixtures/config.json"), NodeProperties.class);

    this.graphTransitAuthority = new GraphTransitAuthority("test-pipeline", "test-node");
  }

  @Test
  public void testInputToRunRequestHandler() {
    val handler = Input.createInputToRunRequestHandler(config.getWorkflow());

    Map<String, Object> wfParams = Map.of("studyId", "TEST-CA");

    val wfParamTransaction = wrapWithTransaction(wfParams);

    val source = Flux.just(wfParamTransaction).transform(handler);

    val wfProperties = config.getWorkflow();

    StepVerifier.create(source)
        .expectNextMatches(
            tx ->
                tx.get().getWorkflowUrl().equalsIgnoreCase(wfProperties.getUrl())
                    && tx.get()
                        .getWorkflowEngineParams()
                        .getRevision()
                        .equalsIgnoreCase(wfProperties.getRevision()))
        .expectComplete()
        .verifyThenAssertThat()
        .hasNotDroppedElements()
        .hasNotDroppedErrors()
        .hasNotDiscardedElements();

    // handler is just mapping so transaction should not be acknowledged yet
    assertTrue(isNotAcknowledged(wfParamTransaction));
  }

  @Test
  public void testErrorHandledInInputToRunRequestHandler() {
    // imagine node was configured incorrectly and workflow object is null
    val handler = Input.createInputToRunRequestHandler(null);

    Map<String, Object> wfParams = Map.of("studyId", "TEST-CA");

    val wfParamTransaction = wrapWithTransaction(wfParams);

    val source =
        Flux.just(wfParamTransaction)
            .doOnNext(graphTransitAuthority::registerNonEntityTx)
            .transform(handler);

    StepVerifier.create(source)
        .expectComplete()
        .verifyThenAssertThat()
        .hasNotDroppedElements()
        .hasNotDroppedErrors()
        .hasNotDiscardedElements();

    assertTrue(isRejected(wfParamTransaction));
  }
}
