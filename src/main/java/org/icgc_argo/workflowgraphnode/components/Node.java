package org.icgc_argo.workflowgraphnode.components;

//import static org.icgc_argo.workflow_graph_lib.polyglot.Polyglot.evaluateBooleanExpression;
//import static org.icgc_argo.workflow_graph_lib.polyglot.Polyglot.runMainFunctionWithData;

import static org.icgc_argo.workflowgraphnode.rabbitmq.PolyglotCustom.evaluateBooleanExpression;
import static org.icgc_argo.workflowgraphnode.rabbitmq.PolyglotCustom.runMainFunctionWithData;

import static org.icgc_argo.workflow_graph_lib.utils.JacksonUtils.toMap;

import com.pivotal.rabbitmq.stream.Transaction;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.icgc_argo.workflow_graph_lib.polyglot.enums.GraphFunctionLanguage;
import org.icgc_argo.workflow_graph_lib.schema.GraphEvent;
import org.icgc_argo.workflow_graph_lib.workflow.client.RdpcClient;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import org.icgc_argo.workflowgraphnode.logging.GraphLogger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class Node {
  public static Function<Flux<Transaction<GraphEvent>>, Flux<Transaction<GraphEvent>>>
      createFilterTransformer(NodeProperties nodeProperties) {
    return input ->
        input
            .map(mapToEventFilterPair(nodeProperties))
            .filter(eventFilterPair -> eventFilterPair.filterAndResult.getT2())
            .doOnDiscard(EventFilterPair.class, Node::doOnFilterFail)
            .map(EventFilterPair::getTransaction)
            .onErrorContinue(Errors.handle());
  }

  public static Function<Flux<Transaction<GraphEvent>>, Flux<Transaction<Map<String, Object>>>>
      createGqlQueryTransformer(RdpcClient client, String query) {
    return (input) ->
        input
            .flatMap(gqlQuery(client, query))
            .onErrorContinue(Errors.handle())
            .doOnNext(tx -> GraphLogger.info(tx, "GQL Response: %s", tx.get()));
  }

  public static Function<
          Flux<Transaction<Map<String, Object>>>, Flux<Transaction<Map<String, Object>>>>
      createActivationFunctionTransformer(NodeProperties nodeProperties) {
    // using flatMap because in reactor 3.3.2.RELEASE, map doesn't have "Error Mode Support" for
    // onErrorContinue
    return (input) ->
        input
            .flatMap(tx -> Mono.just(activationFunction(nodeProperties).apply(tx)))
            .onErrorContinue(Errors.handle())
            .doOnNext(tx -> GraphLogger.info(tx, "Activation Result: %s", tx.get()));
  }

  private static boolean evaluateFilter(
      Transaction<GraphEvent> tx, GraphFunctionLanguage language, String expression) {
    return evaluateBooleanExpression(language, expression, toMap(tx.get().toString()));
  }

  private static Function<Transaction<GraphEvent>, Mono<Transaction<Map<String, Object>>>> gqlQuery(
      RdpcClient client, String query) {
    // we have, and want to keep the original transaction <Transaction<GenericData.Record>>,
    // but we want to map it's value to the GQL response, and return the transaction wrapped
    // in a Mono for the outer flatMap to resolve Mono<Transaction<Map<String, Object>>>
    return tx ->
        client
            .simpleQueryWithEvent(query, tx.get())
            // this flatMap also needs a function that returns a publisher (mono),
            // which gets passed up all the way to the edge config flatMap
            // preserving our original transaction and async subscribing to the
            // result of the GQL Query which get mapped onto the transaction
            // hence the return type Transaction<Map<String, Object>>>
            .flatMap(gqlResponse -> Mono.fromCallable(() -> tx.map(gqlResponse)));
  }

  private static Function<Transaction<Map<String, Object>>, Transaction<Map<String, Object>>>
      activationFunction(NodeProperties nodeProperties) {
    return tx ->
        tx.map(
            runMainFunctionWithData(
                nodeProperties.getFunctionLanguage(),
                nodeProperties.getActivationFunction(),
                tx.get()));
  }

  private static Function<Transaction<GraphEvent>, EventFilterPair> mapToEventFilterPair(
      NodeProperties nodeProperties) {
    return tx ->
        new EventFilterPair(
            tx,
            nodeProperties.getFilters().stream()
                .reduce(
                    Tuples.of(new NodeProperties.Filter(), true),
                    filterReducer(nodeProperties, tx),
                    (filterA, filterB) -> {
                      throw new RuntimeException(
                          "Beware, here there be dragons ... in the form of reducer combinators somehow being called on a non-parallel stream reduce ...");
                    }));
  }

  private static BiFunction<
          Tuple2<NodeProperties.Filter, Boolean>,
          NodeProperties.Filter,
          Tuple2<NodeProperties.Filter, Boolean>>
      filterReducer(NodeProperties nodeProperties, Transaction<GraphEvent> tx) {
    return (acc, filter) -> {
      if (!acc.getT2()) {
        // fail on first filter
        return acc;
      } else if (evaluateFilter(tx, nodeProperties.getFunctionLanguage(), filter.getExpression())) {
        logFilterMessage("Filter passed", tx, filter);
        return acc;
      } else {
        // return the first filter that evaluated to false
        return Tuples.of(filter, false);
      }
    };
  }

  private static void doOnFilterFail(EventFilterPair eventFilterPair) {
    if (eventFilterPair.filterAndResult.getT1().getReject()) {
      logFilterMessage(
          "Filter failed (rejecting)",
          eventFilterPair.getTransaction(),
          eventFilterPair.filterAndResult.getT1());
      eventFilterPair.getTransaction().reject();
    } else {
      logFilterMessage(
          "Filter failed (no ack)",
          eventFilterPair.getTransaction(),
          eventFilterPair.filterAndResult.getT1());
    }
  }

  private static void logFilterMessage(
      String preText, Transaction<GraphEvent> tx, NodeProperties.Filter filter) {
    GraphLogger.info(
        tx,
        "%s with the following expression: \"%s\" for value: %s",
        preText,
        filter.getExpression(),
        tx.get());
  }

  @Data
  @AllArgsConstructor
  public static class EventFilterPair {
    private Transaction<GraphEvent> transaction;
    private Tuple2<NodeProperties.Filter, Boolean> filterAndResult;
  }
}
