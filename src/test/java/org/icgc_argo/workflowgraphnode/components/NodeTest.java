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
import java.lang.reflect.Method;
import java.util.Collections;
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

    // Fails on second filter due to bad analysisType
    val testDiscardOne =
        new EventFilterPair(input.get(1), Tuples.of(filterConfig.getFilters().get(1), false));

    // Fails on first filter due to bad study
    val testDiscardTwo =
        new EventFilterPair(input.get(2), Tuples.of(filterConfig.getFilters().get(0), false));

    // Both filters fail but we want to fail fast and ensure only the first is caught
    val testDiscardThree =
        new EventFilterPair(input.get(3), Tuples.of(filterConfig.getFilters().get(0), false));

    StepVerifier.create(source)
        .expectNextMatches(tx -> tx.get().getAnalysisType().equals("variantCall"))
        .expectComplete()
        .verifyThenAssertThat()
        .hasDiscarded(testDiscardOne, testDiscardTwo, testDiscardThree);
  }

  @Test
  @SneakyThrows
  void testDiscardFunction() {
    val testEvent =
        tm.newTransaction(
            GraphEvent.newBuilder()
                .setAnalysisId("test")
                .setAnalysisState("COMPLETE")
                .setAnalysisType("badAnalysisTypeAndStudy")
                .setStudyId("BROKEN")
                .setExperimentalStrategy("WGS")
                .setDonorIds(Collections.emptyList())
                .setFiles(Collections.emptyList())
                .build());

    val nonRejectFilterTest =
        new EventFilterPair(testEvent, Tuples.of(filterConfig.getFilters().get(0), false));

    val rejectFilterTest =
        new EventFilterPair(testEvent, Tuples.of(filterConfig.getFilters().get(1), false));

    // really want to make sure the transaction is being handled correctly
    Method doOnFilterFail = Node.class.getDeclaredMethod("doOnFilterFail", EventFilterPair.class);
    doOnFilterFail.setAccessible(true);

    Field receivedRejectedField = Transactional.class.getDeclaredField("receivedRejected");
    receivedRejectedField.setAccessible(true);

    // Run transaction through non-rejecting filter
    doOnFilterFail.invoke(null, nonRejectFilterTest);
    val isNonRejectedTestRejected =
        (AtomicBoolean) receivedRejectedField.get(nonRejectFilterTest.getTransaction());
    assertThat(isNonRejectedTestRejected).isFalse();

    // Run transaction through rejecting filter
    doOnFilterFail.invoke(null, rejectFilterTest);
    val isRejectedTestRejected =
        (AtomicBoolean) receivedRejectedField.get(rejectFilterTest.getTransaction());
    assertThat(isRejectedTestRejected);
  }
}
