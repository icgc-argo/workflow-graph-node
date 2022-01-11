package org.icgc_argo.workflowgraphnode.service;

import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc_argo.workflow_graph_lib.graphql.client.GetRunQuery;
import org.icgc_argo.workflow_graph_lib.workflow.client.RdpcClient;
import org.icgc_argo.workflow_graph_lib.workflow.model.RunRequest;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import org.icgc_argo.workflowgraphnode.controller.InvalidRequest;
import org.icgc_argo.workflowgraphnode.model.RestartRequest;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class RunRestartService {
  final RdpcClient rdpcClient;
  final NodeProperties nodeProperties;

  public void restartRun(RestartRequest request) {}

  private Mono<Map<String, Object>> createRunRequestForRestart(RestartRequest request) {
    return rdpcClient
        .findWorkflowByRunId(request.getRunId())
        .map(
            runOpt -> {
              if (runOpt.isEmpty()) {
                Mono.error(new InvalidRequest("Run with ID was not found"));
              }

              val run = runOpt.get();
              val engParam = run.getEngineParameters().get();

              if (!isMatchingConfiguredEngineParams(engParam)) {
                val errorInfo =
                    Map.of(
                        "nodeConfig",
                        nodeProperties.getWorkflowEngineParams(),
                        "runConfig",
                        engParam);
                val errorMsg = "Run to restart has conflicting engine params";
                Mono.error(new InvalidRequest(errorMsg, errorInfo));
              }
               return request.getParams().isEmpty() ? (Map<String, Object>) run.getParameters().get() : request.getParams();



            });
  }

  private Boolean isMatchingConfiguredEngineParams(GetRunQuery.EngineParameters params) {
    val configuredParams = nodeProperties.getWorkflowEngineParams();
    return Optional.of(configuredParams.getWorkDir()).equals(params.getWorkDir())
        && Optional.of(configuredParams.getLaunchDir()).equals(params.getLaunchDir())
        && Optional.of(configuredParams.getProjectDir()).equals(params.getProjectDir());
  }
}
