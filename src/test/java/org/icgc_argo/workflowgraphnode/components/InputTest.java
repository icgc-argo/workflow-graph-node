package org.icgc_argo.workflowgraphnode.components;

import static org.icgc_argo.workflowgraphnode.util.JacksonUtils.readValue;
import static org.icgc_argo.workflowgraphnode.util.TransactionUtils.wrapWithTransaction;

import java.util.Map;
import lombok.SneakyThrows;
import lombok.val;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@ActiveProfiles("test")
@SpringBootTest
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

    val source = Flux.just(wrapWithTransaction(wfParams)).transform(handler);

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
  }
}
