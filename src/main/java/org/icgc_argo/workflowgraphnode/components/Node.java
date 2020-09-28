package org.icgc_argo.workflowgraphnode.components;

import static org.icgc_argo.workflow_graph_lib.polyglot.Polyglot.evaluateBooleanExpression;
import static org.icgc_argo.workflow_graph_lib.polyglot.Polyglot.runMainFunctionWithData;

import com.pivotal.rabbitmq.stream.Transaction;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;
import org.icgc_argo.workflow_graph_lib.workflow.model.RunRequest;
import org.icgc_argo.workflow_graph_lib.workflow.model.WorkflowEngineParams;
import org.icgc_argo.workflowgraphnode.components.exceptions.WorkflowParamsFunctionException;
import org.icgc_argo.workflowgraphnode.config.AppConfig;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class Node {

  private final NodeProperties nodeProperties;

  @Autowired
  public Node(AppConfig appConfig) {
    this.nodeProperties = appConfig.getNodeProperties();
  }

  //  public Function<Transaction<GenericData.Record>, Mono<Transaction<Map<String, Object>>>>
  //      gqlQuery() {
  //    // we have, and want to keep the original transaction <Transaction<GenericData.Record>>,
  //    // but we want to map it's value to the GQL response, and return the transaction wrapped
  //    // in a Mono for the outer flatMap to resolve Mono<Transaction<Map<String, Object>>>
  //    return tx ->
  //        graphQL
  //            .query(nodeProperties.getGqlQueryString(), tx.get())
  //            // this flatMap also needs a function that returns a publisher (mono),
  //            // which gets passed up all the way to the edge config flatMap
  //            // preserving our original transaction and async subscribing to the
  //            // result of the GQL Query which get mapped onto the transaction
  //            // hence the return type Transaction<Map<String, Object>>>
  //            .flatMap(gqlResponse -> Mono.fromCallable(() -> tx.map(gqlResponse)));
  //  }

  public Predicate<Transaction<String>> filter() {
    return tx ->
        evaluateBooleanExpression(
            nodeProperties.getFunctionLanguage(), nodeProperties.getFilterFunction(), tx.get());
  }

  public Function<Transaction<Map<String, Object>>, Transaction<Map<String, Object>>>
      activationFunction() {
    return tx ->
        tx.map(
            runMainFunctionWithData(
                nodeProperties.getFunctionLanguage(),
                nodeProperties.getActivationFunction(),
                tx.get()));
  }

  public Function<Map<String, Object>, RunRequest> inputToRunRequest() {
    return workflowParamsResponse -> {
      try {
        return RunRequest.builder()
            .workflowUrl(nodeProperties.getWorkflow().getUrl())
            // TODO: params will be Generic record with schema provided to node
            .workflowParams(workflowParamsResponse)
            .workflowEngineParams( // TODO: going to require more engine params ...
                WorkflowEngineParams.builder()
                    .revision(nodeProperties.getWorkflow().getRevision())
                    .build())
            .build();
      } catch (Throwable e) {
        throw new WorkflowParamsFunctionException(e.getLocalizedMessage());
      }
    };
  }
}
