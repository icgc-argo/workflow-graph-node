package org.icgc_argo.workflowgraphnode.components;

import com.pivotal.rabbitmq.stream.Transaction;
import org.icgc_argo.workflow_graph_lib.workflow.model.RunRequest;
import org.icgc_argo.workflow_graph_lib.workflow.model.WorkflowEngineParams;
import org.icgc_argo.workflowgraphnode.components.exceptions.WorkflowParamsFunctionException;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.function.Function;

public class Input {

  public static Function<Flux<Transaction<Map<String, Object>>>, Flux<Transaction<RunRequest>>>
      createInputToRunRequestHandler(NodeProperties.WorkflowProperties workflow) {
    return (input) ->
        input
            .<Transaction<RunRequest>>handle(
                (tx, sink) -> sink.next(tx.map(inputToRunRequest(workflow).apply(tx.get()))))
            .onErrorContinue(Errors.handle());
  }

  private static Function<Map<String, Object>, RunRequest> inputToRunRequest(
      NodeProperties.WorkflowProperties workflow) {
    return workflowParamsResponse -> {
      try {
        return RunRequest.builder()
            .workflowUrl(workflow.getUrl())
            // TODO: params will be Generic record with schema provided to node
            .workflowParams(workflowParamsResponse)
            .workflowEngineParams( // TODO: going to require more engine params ...
                WorkflowEngineParams.builder().revision(workflow.getRevision()).build())
            .build();
      } catch (Throwable e) {
        throw new WorkflowParamsFunctionException(e.getLocalizedMessage());
      }
    };
  }
}
