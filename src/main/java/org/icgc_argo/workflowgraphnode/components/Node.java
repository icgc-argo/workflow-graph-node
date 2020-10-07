package org.icgc_argo.workflowgraphnode.components;

import com.pivotal.rabbitmq.stream.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.icgc_argo.workflow_graph_lib.polyglot.enums.GraphFunctionLanguage;
import org.icgc_argo.workflow_graph_lib.schema.GraphEvent;
import org.icgc_argo.workflow_graph_lib.workflow.client.RdpcClient;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.icgc_argo.workflow_graph_lib.polyglot.Polyglot.evaluateBooleanExpression;
import static org.icgc_argo.workflow_graph_lib.polyglot.Polyglot.runMainFunctionWithData;
import static org.icgc_argo.workflow_graph_lib.utils.JacksonUtils.convertValue;

@Slf4j
public class Node {

  public static Function<Flux<Transaction<GraphEvent>>, Flux<Transaction<GraphEvent>>>
      createFilterApplier(NodeProperties nodeProperties) {
    return (input) -> {
      nodeProperties
          .getFilters()
          .forEach(
              filter -> {
                input.transform(
                    Node.createFilterTransformer(nodeProperties.getFunctionLanguage(), filter));
              });

      return input;
    };
  }

  public static Function<Flux<Transaction<GraphEvent>>, Flux<Transaction<GraphEvent>>>
      createFilterTransformer(GraphFunctionLanguage language, NodeProperties.Filter filter) {
    return (input) ->
        input
            .filter(filter(language, filter.getExpression()))
            .onErrorContinue(Errors.handle())
            .doOnDiscard(
                Transaction.class,
                tx -> {
                  if (filter.getReject()) {
                    logFilterMessage("Filter failed (rejecting)", tx, filter);
                    tx.reject();
                  } else {
                    logFilterMessage("Filter failed (no ack)", tx, filter);
                  }
                })
            .doOnNext(tx -> logFilterMessage("Filter passed", tx, filter));
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

  private static Predicate<Transaction<GraphEvent>> filter(
      GraphFunctionLanguage language, String expression) {
    return tx -> evaluateBooleanExpression(language, expression, convertValue(tx.get(), Map.class));
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
        "{} for value: {}, with the following expression: {}",
        preText,
        tx.get(),
        filter.getExpression());
  }
}
