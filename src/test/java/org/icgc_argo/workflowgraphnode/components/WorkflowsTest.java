package org.icgc_argo.workflowgraphnode.components;

import static org.icgc_argo.workflowgraphnode.components.CommonFunctions.convertToTransaction;
import static org.icgc_argo.workflowgraphnode.components.CommonFunctions.readValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.pivotal.rabbitmq.stream.Transaction;
import com.pivotal.rabbitmq.stream.TransactionManager;
import java.util.List;
import lombok.SneakyThrows;
import lombok.val;
import org.icgc_argo.workflow_graph_lib.schema.GraphEvent;
import org.icgc_argo.workflow_graph_lib.workflow.client.RdpcClient;
import org.icgc_argo.workflow_graph_lib.workflow.model.RunRequest;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ActiveProfiles("test")
@SpringBootTest
public class WorkflowsTest {
  private final TransactionManager<GraphEvent, Transaction<GraphEvent>> tm =
      new TransactionManager<>("workflowsTest");

  private final NodeProperties config;

  @SneakyThrows
  public WorkflowsTest() {
    config =
        readValue(
            this.getClass().getResourceAsStream("fixtures/config.json"), NodeProperties.class);
  }

  @Test
  public void testStartRun() {
    val runReq = RunRequest.builder().workflowUrl(config.getWorkflow().getUrl()).build();
    val runId = "WES-123456789";

    val rdpcClientMock = mock(RdpcClient.class);

    when(rdpcClientMock.startRun(runReq)).thenReturn(Mono.just(runId));

    val func = Workflows.startRuns(rdpcClientMock);

    val source = Flux.just(convertToTransaction(runReq)).flatMap(func);

    StepVerifier.create(source)
        .expectNextMatches(transaction -> transaction.get().equalsIgnoreCase(runId))
        .expectComplete()
        .verifyThenAssertThat()
        .hasNotDroppedElements()
        .hasNotDroppedErrors()
        .hasNotDiscardedElements();
  }

  @Test
  public void testRunAnalysesToGraphEvent() {
    val runId = "WES-123456789";

    val rdpcClientMock = mock(RdpcClient.class);

    val ge =
        new GraphEvent(
            "analysisId", "analysisState", "analysisType", "studyId", "WGS", List.of(), List.of());

    when(rdpcClientMock.createGraphEventsForRun(runId)).thenReturn(Mono.just(List.of(ge)));

    val func = Workflows.runAnalysesToGraphEvent(rdpcClientMock);

    val source = Flux.just(convertToTransaction(runId)).flatMap(func);

    StepVerifier.create(source)
        .expectNextMatches(tx -> tx.get().equals(ge))
        .expectComplete()
        .verifyThenAssertThat()
        .hasNotDroppedElements()
        .hasNotDroppedErrors()
        .hasNotDiscardedElements();
  }
}
