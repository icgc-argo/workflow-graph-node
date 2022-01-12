package org.icgc_argo.workflowgraphnode.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pivotal.rabbitmq.source.Sender;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc_argo.workflow_graph_lib.graphql.client.GetWorkflowInfoForRestartQuery;
import org.icgc_argo.workflow_graph_lib.workflow.client.RdpcClient;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import org.icgc_argo.workflowgraphnode.controller.InvalidRequest;
import org.icgc_argo.workflowgraphnode.model.RestartRequest;
import org.icgc_argo.workflowgraphnode.model.RestartInput;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class WorkflowService {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Sender<Map<String, Object>> directInputSender;
  private final Sender<RestartInput> restartInputSender;
  private final RdpcClient rdpcClient;
  private final NodeProperties nodeProperties;

  public Mono<Boolean> inject(Map<String, Object> params) {
    return directInputSender.send(params).thenReturn(true).onErrorReturn(false);
  }

  public Mono<Boolean> restart(RestartRequest request) {
    return createParamsForRestart(request)
        .map(restartInputSender::send)
        .thenReturn(true)
        .onErrorReturn(false);
  }

  private Mono<RestartInput> createParamsForRestart(RestartRequest request) {
    return rdpcClient
        .findWorkflowByRunIdAndRepo(request.getRunId(), nodeProperties.getWorkflow().getUrl())
        .flatMap(
            runOpt -> {
              if (runOpt.isEmpty()) {
                return Mono.error(new InvalidRequest("Run with ID was not found"));
              }
              val run = runOpt.get();

              if (run.getEngineParameters().isEmpty()) {
                return Mono.error(new InvalidRequest("Run exists but has no engine params!"));
              }
              val engParam = run.getEngineParameters().get();

              if (!isMatchingConfiguredEngineParams(engParam)) {
                return Mono.error(
                    new InvalidRequest(
                        "Run to restart has conflicting engine params with this node!",
                        Map.of(
                            "nodeConfig",
                            nodeProperties.getWorkflowEngineParams(),
                            "runConfig",
                            engParam)));
              }

              val sessionId = run.getSessionId().get();

              if (!request.getParams().isEmpty()) {
                return Mono.just(new RestartInput(request.getParams(), sessionId));
              } else {
                return Mono.just(
                    new RestartInput(
                        MAPPER.convertValue(run.getParameters().get(), Map.class), sessionId));
              }
            });
  }

  private Boolean isMatchingConfiguredEngineParams(
      GetWorkflowInfoForRestartQuery.EngineParameters params) {
    return Optional.of(nodeProperties.getWorkflowEngineParams().getWorkDir())
            .equals(params.getWorkDir())
        && Optional.of(nodeProperties.getWorkflowEngineParams().getLaunchDir())
            .equals(params.getLaunchDir())
        && Optional.of(nodeProperties.getWorkflowEngineParams().getProjectDir())
            .equals(params.getProjectDir())
        && Optional.of(nodeProperties.getWorkflow().getRevision()).equals(params.getRevision());
  }
}
