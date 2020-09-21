package org.icgc_argo.workflowgraphnode.components;

import static org.icgc_argo.workflow_graph_lib.polyglot.Polyglot.runMainFunctionWithData;

import com.pivotal.rabbitmq.stream.Transaction;
import java.util.Map;
import java.util.function.Function;
import org.graalvm.polyglot.Value;
import org.icgc_argo.workflowgraphnode.components.exceptions.WorkflowParamsFunctionException;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import org.icgc_argo.workflowgraphnode.model.RunRequest;
import org.icgc_argo.workflowgraphnode.model.WorkflowEngineParams;

public class Node {

  public static Function<Transaction<String>, Transaction<Value>> workflowParamsFunction(
      NodeProperties nodeProperties) {
    return tx ->
        tx.map(
            runMainFunctionWithData(
                nodeProperties.getFunctionLanguage(),
                nodeProperties.getWorkflowParamsFunction(),
                tx.get()));
  }

  public static Function<Value, RunRequest> sourceToSinkProcessor(NodeProperties nodeProperties) {
    return workflowParamsResponse -> {
      try {
        return RunRequest.builder()
            .workflowUrl(nodeProperties.getWorkflow().getWorkflowUrl())
            .workflowParams(workflowParamsResponse.as(Map.class))
            .workflowEngineParams( // TODO: going to require more engine params ...
                WorkflowEngineParams.builder()
                    .revision(nodeProperties.getWorkflow().getWorkflowVersion())
                    .build())
            .build();
      } catch (Throwable e) {
        throw new WorkflowParamsFunctionException(e.getLocalizedMessage());
      }
    };
  }
}
