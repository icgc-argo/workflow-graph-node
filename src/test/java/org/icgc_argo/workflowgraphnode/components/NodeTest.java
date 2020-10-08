package org.icgc_argo.workflowgraphnode.components;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pivotal.rabbitmq.stream.Transaction;
import com.pivotal.rabbitmq.stream.TransactionManager;
import com.pivotal.rabbitmq.stream.Transactional;
import lombok.SneakyThrows;
import lombok.val;
import org.icgc_argo.workflow_graph_lib.schema.GraphEvent;
import org.icgc_argo.workflowgraphnode.components.Node.EventFilterPair;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.util.function.Tuples;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@SpringBootTest
public class NodeTest {

  private final ObjectMapper mapper = new ObjectMapper();
  private final TransactionManager<GraphEvent, Transaction<GraphEvent>> tm =
      new TransactionManager<>("nodeTest");

  // state
  private final NodeProperties filterConfig;

  @SneakyThrows
  public NodeTest() {
    filterConfig =
        mapper.readValue(
            this.getClass().getResourceAsStream("fixtures/filters/filterConfig.json"),
            NodeProperties.class);
  }

  @Test
  @SneakyThrows
  void testFilterTransformer() {
    val inputStream = this.getClass().getResourceAsStream("fixtures/filters/input.json");
    val input =
        mapper.readValue(inputStream, new TypeReference<List<GraphEvent>>() {}).stream()
            .map(tm::newTransaction)
            .collect(Collectors.toList());

    val transformer = Node.createFilterTransformer(filterConfig);

    Flux<Transaction<GraphEvent>> source = Flux.fromIterable(input).transform(transformer);

    StepVerifier.create(source)
        .expectNextMatches(tx -> tx.get().getAnalysisType().equals("variantCall"))
        .expectComplete()
        .verifyThenAssertThat()
        .hasDiscardedElementsSatisfying(this::filterTransformerDiscardedTests);
  }

  /**
   * Helper method to ensure that the doOnDiscard hook is behaving as expected:
   * 1. Should fail on the first filter
   * 2. Should correctly reject/not-reject the transaction based on the filter that failed
   * @param discarded the collection of discarded elements (order is maintained from input Flux)
   */
  @SneakyThrows
  private void filterTransformerDiscardedTests(Collection<Object> discarded) {
    // also want to make sure the transaction is being handled correctly
    Field receivedRejectedField = Transactional.class.getDeclaredField("receivedRejected");
    receivedRejectedField.setAccessible(true);

    // test that filters have been properly handled in doOnDiscard hook
    val discardedIter = discarded.toArray();
    val testDiscardOne = (EventFilterPair) discardedIter[0];
    val testDiscardTwo = (EventFilterPair) discardedIter[1];
    val testDiscardThree = (EventFilterPair) discardedIter[2];

    // Fails on second filter due to bad analysisType (should reject
    assertThat(testDiscardOne.getFilterAndResult())
        .isEqualTo(Tuples.of(filterConfig.getFilters().get(1), false));
    assertThat((AtomicBoolean) receivedRejectedField.get(testDiscardOne.getTransaction()));

    // Fails on first filter due to bad study
    assertThat(testDiscardTwo.getFilterAndResult())
        .isEqualTo(Tuples.of(filterConfig.getFilters().get(0), false));
    assertThat((AtomicBoolean) receivedRejectedField.get(testDiscardTwo.getTransaction()))
        .isFalse();

    // Both filters fail but we want to fail fast and ensure only the first is caught
    assertThat(testDiscardThree.getFilterAndResult())
        .isEqualTo(Tuples.of(filterConfig.getFilters().get(0), false));
    assertThat((AtomicBoolean) receivedRejectedField.get(testDiscardThree.getTransaction()))
        .isFalse();
  }
}
