package org.icgc_argo.workflowgraphnode.components;

import com.pivotal.rabbitmq.stream.Transaction;
import java.util.Map;
import java.util.function.Function;
import lombok.val;
import org.icgc_argo.workflow_graph_lib.workflow.model.RunRequest;
import org.icgc_argo.workflow_graph_lib.workflow.model.WorkflowEngineParams;
import org.icgc_argo.workflowgraphnode.components.exceptions.WorkflowParamsFunctionException;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import reactor.core.publisher.Flux;

public class Input {

  public static Function<Flux<Transaction<Map<String, Object>>>, Flux<Transaction<RunRequest>>>
      createInputToRunRequestHandler(NodeProperties nodeProperties) {
    return (input) ->
        input
            .<Transaction<RunRequest>>handle(
                (tx, sink) -> sink.next(tx.map(inputToRunRequest(nodeProperties).apply(tx.get()))))
            .onErrorContinue(Errors.handle());
  }

  private static Function<Map<String, Object>, RunRequest> inputToRunRequest(
      NodeProperties nodeProperties) {
    return workflowParamsResponse -> {
      try {
        val workflow = nodeProperties.getWorkflow();
        val workflowEngineParams = nodeProperties.getWorkflowEngineParams();
        return RunRequest.builder()
            .workflowUrl(workflow.getUrl())
            // TODO: params will be Generic record with schema provided to node
            .workflowParams(workflowParamsResponse)
            .workflowEngineParams( // TODO: going to require more engine params ...
                WorkflowEngineParams.builder()
                    .revision(workflow.getRevision())
                    .projectDir(workflowEngineParams.getProjectDir())
                    .workDir(workflowEngineParams.getWorkDir())
                    .launchDir(workflowEngineParams.getLaunchDir())
                    .build())
            .build();
      } catch (Throwable e) {
        throw new WorkflowParamsFunctionException(e.getLocalizedMessage());
      }
    };
  }
}
