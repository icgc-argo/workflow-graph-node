package org.icgc_argo.workflowgraphnode.components;

import com.pivotal.rabbitmq.stream.Transaction;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc_argo.workflow_graph_lib.polyglot.enums.GraphFunctionLanguage;
import org.icgc_argo.workflow_graph_lib.schema.GraphEvent;
import org.icgc_argo.workflow_graph_lib.workflow.client.RdpcClient;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.icgc_argo.workflow_graph_lib.polyglot.Polyglot.evaluateBooleanExpression;
import static org.icgc_argo.workflow_graph_lib.polyglot.Polyglot.runMainFunctionWithData;
import static org.icgc_argo.workflow_graph_lib.utils.JacksonUtils.toMap;

@Slf4j
public class Node {
  public static Function<Flux<Transaction<GraphEvent>>, Flux<Transaction<GraphEvent>>>
      createFilterTransformer(NodeProperties nodeProperties) {

    AtomicReference<NodeProperties.Filter> failedFilter =
        new AtomicReference<>(new NodeProperties.Filter());

    return input ->
        input
            .filter(
                tx ->
                    nodeProperties.getFilters().stream()
                        .reduce(
                            true,
                            (acc, filter) -> {
                              if (!acc) {
                                // One filter fail == Flux.filter(false)
                                return false;
                              } else if (evaluateFilter(
                                  tx,
                                  nodeProperties.getFunctionLanguage(),
                                  filter.getExpression())) {
                                logFilterMessage("Filter passed", tx, filter);
                                return true;
                              } else {
                                // update the failed filter so it can be handled in doOnDiscard
                                failedFilter.set(filter);
                                return false;
                              }
                            },
                            (filterA, filterB) -> {
                              throw new RuntimeException(
                                  "Beware, here there be dragons ... in the form of reducer combinators somehow being called on a non-parallel stream reduce ...");
                            }))
            .doOnDiscard(
                Transaction.class,
                (tx) -> {
                  if (failedFilter.get().getReject()) {
                    logFilterMessage("Filter failed (rejecting)", tx, failedFilter.get());
                    tx.reject();
                  } else {
                    logFilterMessage("Filter failed (no ack)", tx, failedFilter.get());
                  }
                });
  }

  private static boolean evaluateFilter(
      Transaction<GraphEvent> tx, GraphFunctionLanguage language, String expression) {
    return evaluateBooleanExpression(language, expression, toMap(tx.get().toString()));
  }

  public static Function<Flux<Transaction<GraphEvent>>, Flux<Transaction<Map<String, Object>>>>
      createGqlQueryTransformer(RdpcClient client, String query) {
    return (input) ->
        input
            .flatMap(gqlQuery(client, query))
            .doOnNext(tx -> log.info("GQL Response: {}", tx.get()));
  }

  public static Function<
          Flux<Transaction<Map<String, Object>>>, Flux<Transaction<Map<String, Object>>>>
      createActivationFunctionTransformer(NodeProperties nodeProperties) {
    return (input) ->
        input
            .map(activationFunction(nodeProperties))
            .onErrorContinue(Errors.handle())
            .doOnNext(tx -> log.info("Activation Result: {}", tx.get()));
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

  private static void logFilterMessage(
      String preText, Transaction tx, NodeProperties.Filter filter) {
    log.info(
        "{} with the following expression: \"{}\" for value: {}",
        preText,
        filter.getExpression(),
        tx.get());
  }
}
