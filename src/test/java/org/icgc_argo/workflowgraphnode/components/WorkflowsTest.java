package org.icgc_argo.workflowgraphnode.components;

import static java.util.stream.Collectors.toList;
import static org.icgc_argo.workflowgraphnode.util.JacksonUtils.readValue;
import static org.icgc_argo.workflowgraphnode.util.TransactionUtils.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc_argo.workflow_graph_lib.exceptions.RequeueableException;
import org.icgc_argo.workflow_graph_lib.schema.GraphEvent;
import org.icgc_argo.workflow_graph_lib.schema.GraphRun;
import org.icgc_argo.workflow_graph_lib.workflow.client.RdpcClient;
import org.icgc_argo.workflow_graph_lib.workflow.model.RunRequest;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Slf4j
@ActiveProfiles("test")
public class WorkflowsTest {
  private final NodeProperties config;

  @SneakyThrows
  public WorkflowsTest() {
    config =
        readValue(
            this.getClass().getResourceAsStream("fixtures/config.json"), NodeProperties.class);
  }

  @Test
  public void testStartRun() {
    val workflowUrl = config.getWorkflow().getUrl();
    val runReq = RunRequest.builder().workflowUrl(workflowUrl).build();
    val runId = "WES-123456789";

    val rdpcClientMock = mock(RdpcClient.class);
    when(rdpcClientMock.startRun(runReq)).thenReturn(Mono.just(runId));

    val startRunFunc = Workflows.startRuns(rdpcClientMock);

    val source = Flux.just(wrapWithTransaction(runReq)).flatMap(startRunFunc);

    StepVerifier.create(source)
        .expectNextMatches(transaction -> transaction.get().getRunId().equalsIgnoreCase(runId))
        .expectComplete()
        .verifyThenAssertThat()
        .hasNotDroppedElements()
        .hasNotDroppedErrors()
        .hasNotDiscardedElements();
  }

  @Test
  @SneakyThrows
  public void testHandleRunStatus() {
    val rdpcClientMock = mock(RdpcClient.class);

    val runIdToStatusMap =
        List.of(
            new RunIdStatePair("WES-1", "COMPLETE"), // nexted
            new RunIdStatePair("WES_2", "EXECUTOR_ERROR"), // rejected
            new RunIdStatePair("WES-3", "RUNNING"), // requeued
            new RunIdStatePair("WES-4", "CANCELED") // committed
            );

    // iterate all pairs to setup mock and input transactions
    val runIdTransactions =
        runIdToStatusMap.stream()
            .map(
                runIdStatePair -> {
                  when(rdpcClientMock.getWorkflowStatus(runIdStatePair.getRunId()))
                      .thenReturn(Mono.just(runIdStatePair.getState()));

                  return wrapWithTransaction(new GraphRun(UUID.randomUUID().toString(), runIdStatePair.getRunId()));
                })
            .collect(toList());

    val handler = Workflows.handleRunStatus(rdpcClientMock);

    val source = Flux.fromIterable(runIdTransactions).handle(handler);

    StepVerifier.create(source)
        // transaction 0 is sent to the next call unchanged in the flux handler
        .expectNextMatches(tx -> tx.get().getRunId().equalsIgnoreCase("WES-1"))
        .expectComplete()
        .verifyThenAssertThat()
        .hasNotDroppedElements()
        .hasNotDroppedErrors()
        .hasNotDiscardedElements();

    // assertions for transactions that were not next
    assertTrue(isRejected(runIdTransactions.get(1)));
    assertTrue(isRequeued(runIdTransactions.get(2)));
    assertTrue(isAcknowledged(runIdTransactions.get(3)));
  }

  @Test
  public void testRunNotFoundRun() {
    val graphRun = new GraphRun(UUID.randomUUID().toString(), "WES-im_not_really_here");
    val transaction = wrapWithTransaction(graphRun);

    val rdpcClientMock = mock(RdpcClient.class);
    when(rdpcClientMock.getWorkflowStatus(graphRun.getRunId()))
        .thenReturn(
            Mono.create(
                sink ->
                    sink.error(
                        new RequeueableException(String.format("Run %s not found!", graphRun.getRunId())))));

    val flux =
        Flux.just(transaction)
            .handle(Workflows.handleRunStatus(rdpcClientMock))
            .onErrorContinue(Errors.handle());

    StepVerifier.create(flux).expectComplete().verify();

    assertTrue(isRequeued(transaction));
  }

  @Test
  public void testRunAnalysesToGraphEvent() {
    val run = new GraphRun(UUID.randomUUID().toString(), "WES-123456789");

    val rdpcClientMock = mock(RdpcClient.class);

    val ge =
        new GraphEvent(
            UUID.randomUUID().toString(),
            "analysisId",
            "analysisState",
            "analysisType",
            "studyId",
            "WGS",
            List.of(),
            List.of());

    when(rdpcClientMock.createGraphEventsForRun(run.getRunId())).thenReturn(Mono.just(List.of(ge)));

    val func = Workflows.runAnalysesToGraphEvent(rdpcClientMock);

    val source = Flux.just(wrapWithTransaction(run)).flatMap(func);

    StepVerifier.create(source)
        .expectNextMatches(tx -> tx.get().equals(ge))
        .expectComplete()
        .verifyThenAssertThat()
        .hasNotDroppedElements()
        .hasNotDroppedErrors()
        .hasNotDiscardedElements();
  }

  @Value
  private static class RunIdStatePair {
    String runId;
    String state;
  }
}
