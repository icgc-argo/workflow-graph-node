package org.icgc_argo.workflowgraphnode.components;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc_argo.workflowgraphnode.util.TransactionUtils.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pivotal.rabbitmq.stream.Transaction;
import com.pivotal.rabbitmq.stream.TransactionManager;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.val;
import org.icgc_argo.workflow_graph_lib.exceptions.DeadLetterQueueableException;
import org.icgc_argo.workflow_graph_lib.exceptions.RequeueableException;
import org.icgc_argo.workflow_graph_lib.schema.GraphEvent;
import org.icgc_argo.workflow_graph_lib.workflow.client.RdpcClient;
import org.icgc_argo.workflowgraphnode.components.Node.EventFilterPair;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuples;

@ActiveProfiles("test")
public class NodeTest {

  private final ObjectMapper mapper = new ObjectMapper();
  private final TransactionManager<GraphEvent, Transaction<GraphEvent>> tm =
      new TransactionManager<>("nodeTest");

  // state
  private final NodeProperties config;

  @SneakyThrows
  public NodeTest() {
    config =
        mapper.readValue(
            this.getClass().getResourceAsStream("fixtures/config.json"), NodeProperties.class);
  }

  @Test
  @SneakyThrows
  void testFilterTransformer() {
    val input =
        createGraphEventTransactionsFromStream(
            this.getClass().getResourceAsStream("fixtures/filters/input.json"));

    val transformer = Node.createFilterTransformer(config);

    val source = Flux.fromIterable(input).transform(transformer);

    StepVerifier.create(source)
        .expectNextMatches(tx -> tx.get().getAnalysisType().equals("variantCall"))
        .expectComplete()
        .verifyThenAssertThat()
        .hasDiscardedElementsSatisfying(this::filterTransformerDiscardedTests);
  }

  /**
   * Helper method to ensure that the doOnDiscard hook is behaving as expected: 1. Should fail on
   * the first filter 2. Should correctly reject/not-reject the transaction based on the filter that
   * failed
   *
   * @param discarded the collection of discarded elements (order is maintained from input Flux)
   */
  @SneakyThrows
  private void filterTransformerDiscardedTests(Collection<Object> discarded) {
    // test that filters have been properly handled in doOnDiscard hook
    val discardedIter = discarded.toArray();
    val testDiscardOne = (EventFilterPair) discardedIter[0];
    val testDiscardTwo = (EventFilterPair) discardedIter[1];
    val testDiscardThree = (EventFilterPair) discardedIter[2];

    // Fails on second filter due to bad analysisType (should reject
    assertThat(testDiscardOne.getFilterAndResult())
        .isEqualTo(Tuples.of(config.getFilters().get(1), false));
    assertThat(isRejected(testDiscardOne.getTransaction()))
        .isTrue();

    // Fails on first filter due to bad study
    assertThat(testDiscardTwo.getFilterAndResult())
        .isEqualTo(Tuples.of(config.getFilters().get(0), false));
    assertThat(isRejected(testDiscardTwo.getTransaction()))
        .isFalse();

    // Both filters fail but we want to fail fast and ensure only the first is caught
    assertThat(testDiscardThree.getFilterAndResult())
        .isEqualTo(Tuples.of(config.getFilters().get(0), false));
    assertThat(isRejected(testDiscardThree.getTransaction()))
        .isFalse();
  }

  @Test
  @SneakyThrows
  void testGqlQueryTransformer() {
    val input =
        createGraphEventTransactionsFromStream(
            this.getClass().getResourceAsStream("fixtures/gqlQuery/input.json"));

    List<Map<String, Object>> results =
        createMapListFromJsonFileStream(
            this.getClass().getResourceAsStream("fixtures/gqlQuery/results.json"));

    val rdpcClientMock = mock(RdpcClient.class);

    // first input returns a result
    // expectation: the result makes it throught flux
    when(rdpcClientMock.simpleQueryWithEvent(config.getGqlQueryString(), input.get(0).get()))
        .thenReturn(Mono.just(results.get(0)));

    // second input returns a 400 (RDPC client throws DeadLetterQueueableException)
    // expectation: Errors.handle() deals with error then rejects to DLQ
    when(rdpcClientMock.simpleQueryWithEvent(config.getGqlQueryString(), input.get(1).get()))
        .thenThrow(DeadLetterQueueableException.class);

    // third input returns a 401 (RDPC client throws RequeueableException)
    // expectation: Errors.handle() deals with error then requeues
    when(rdpcClientMock.simpleQueryWithEvent(config.getGqlQueryString(), input.get(2).get()))
        .thenThrow(RequeueableException.class);

    val transformer = Node.createGqlQueryTransformer(rdpcClientMock, config.getGqlQueryString());

    val source = Flux.fromIterable(input).transform(transformer);

    StepVerifier.create(source)
        .expectNextMatches(
            tx ->
                mapper
                    .convertValue(tx.get(), JsonNode.class)
                    .at("/data/analyses/0/analysisId")
                    .asText()
                    .equals("does-exist"))
        .expectComplete()
        .verifyThenAssertThat()
        .hasNotDroppedElements()
        .hasNotDroppedErrors()
        .hasDiscardedElementsSatisfying(this::gqlQueryTransformerDiscardedTests);
  }

  @SneakyThrows
  private void gqlQueryTransformerDiscardedTests(Collection<Object> discarded) {
    val discardedIter = discarded.toArray();
    // expecting DLQ on DeadLetterQueueableException
    assertThat(isRejected(discardedIter[0])).isTrue();

    // expecting requeue on RequeueableException
    assertThat(isRequeued(discardedIter[1])).isTrue();
  }

  @Test
  public void testActivationFunctionTransformer() {
    // gql query result is input into activation func
    Map<String, Object> activationFuncInput =
        createMapListFromJsonFileStream(
                this.getClass().getResourceAsStream("fixtures/gqlQuery/results.json"))
            .get(0);

    Map<String, Object> invalidActivationFuncInput = Map.of();

    val transformer = Node.createActivationFunctionTransformer(config);

    val source =
        Flux.just(
                wrapWithTransaction(invalidActivationFuncInput),
                wrapWithTransaction(activationFuncInput))
            .transform(transformer);

    Map<String, Object> expected =
        Map.of(
            "analysis_id", "does-exist",
            "study_id", "GRAPH",
            "score_url", "https://score.rdpc-qa.cancercollaboratory.org",
            "song_url", "https://song.rdpc-qa.cancercollaboratory.org");

    Consumer<Collection<Object>> expectedActivationFunctionDiscardBehavior = discarded -> {
      val discardedIter = discarded.toArray();
      val discardedInput = discardedIter[0];

      assertTrue(isRejected(discardedInput));
    };

    StepVerifier.create(source)
        .expectNextMatches(tx -> expected.equals(tx.get()))
        .expectComplete()
        .verifyThenAssertThat()
        .hasNotDroppedElements()
        .hasNotDroppedErrors()
        .hasDiscardedElementsSatisfying(expectedActivationFunctionDiscardBehavior);
  }

  @SneakyThrows
  private List<Transaction<GraphEvent>> createGraphEventTransactionsFromStream(
      InputStream inputStream) {
    return mapper.readValue(inputStream, new TypeReference<List<GraphEvent>>() {}).stream()
        .map(tm.newTransaction())
        .collect(Collectors.toList());
  }

  @SneakyThrows
  private List<Map<String, Object>> createMapListFromJsonFileStream(InputStream inputStream) {
    return mapper.readValue(inputStream, new TypeReference<>() {});
  }
}
