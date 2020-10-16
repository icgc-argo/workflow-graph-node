package org.icgc_argo.workflowgraphnode.components;

import static org.icgc_argo.workflowgraphnode.util.JacksonUtils.readValue;
import static org.icgc_argo.workflowgraphnode.util.TransactionUtils.*;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import lombok.SneakyThrows;
import lombok.val;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@ActiveProfiles("test")
public class InputTest {
  private final NodeProperties config;

  @SneakyThrows
  public InputTest() {
    config =
        readValue(
            this.getClass().getResourceAsStream("fixtures/config.json"), NodeProperties.class);
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
    assertTrue(isNotAcked(wfParamTransaction));
  }

  @Test
  public void testErrorHandledInInputToRunRequestHandler() {
    // imagine node was configured incorrectly and workflow object is null
    val handler = Input.createInputToRunRequestHandler(null);

    Map<String, Object> wfParams = Map.of("studyId", "TEST-CA");

    val wfParamTransaction = wrapWithTransaction(wfParams);

    val source = Flux.just(wfParamTransaction).transform(handler);

    StepVerifier.create(source)
            .expectComplete()
            .verify();

    assertTrue(isRejected(wfParamTransaction));
  }
}
