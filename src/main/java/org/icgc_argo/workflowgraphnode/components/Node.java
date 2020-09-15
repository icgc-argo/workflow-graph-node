package org.icgc_argo.workflowgraphnode.components;

import com.pivotal.rabbitmq.stream.Transaction;
import org.graalvm.polyglot.Value;
import org.icgc_argo.workflow_graph_lib.polyglot.enums.GraphFunctionLanguage;
import org.icgc_argo.workflowgraphnode.components.exceptions.WorkflowParamsFunctionException;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import org.icgc_argo.workflowgraphnode.model.RunRequest;
import org.icgc_argo.workflowgraphnode.model.WorkflowEngineParams;

import java.util.Map;
import java.util.function.Function;

import static org.icgc_argo.workflow_graph_lib.polyglot.Polyglot.runMainFunctionWithData;

public class Node {

  public static Function<Transaction<String>, Transaction<Value>> workflowParamsFunction() {
    return tx ->
        tx.map(
            runMainFunctionWithData(
                GraphFunctionLanguage
                    .JS, // TODO: nodeProperties.getNodeFunctionLanguage() once done by Dusan
                "return data;", // TODO: nodeProperties.getWorkflowParamsFunction() once done by
                // Dusan
                tx.get()));
  }

  public static Function<Value, RunRequest> sourceToSinkProcessor(NodeProperties nodeProperties) {
    return workflowParamsResponse -> {
      try {
        return RunRequest.builder()
            .workflowUrl(nodeProperties.getInput().getWorkflowUrl())
            .workflowParams(workflowParamsResponse.as(Map.class))
            .workflowEngineParams(WorkflowEngineParams.builder().revision(nodeProperties.getInput().getWorkflowVersion()).build())
            .build();
      } catch (Throwable e) {
        throw new WorkflowParamsFunctionException(e.getLocalizedMessage());
      }
    };
  }
}
