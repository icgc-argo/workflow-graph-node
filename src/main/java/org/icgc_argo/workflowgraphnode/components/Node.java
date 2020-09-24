package org.icgc_argo.workflowgraphnode.components;

import org.icgc_argo.workflowgraphnode.components.exceptions.WorkflowParamsFunctionException;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import org.icgc_argo.workflowgraphnode.model.RunRequest;
import org.icgc_argo.workflowgraphnode.model.WorkflowEngineParams;

import java.util.Map;
import java.util.function.Function;

public class Node {

  public static Function<Map<String, Object>, RunRequest> sourceToSinkProcessor(
      NodeProperties nodeProperties) {
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
