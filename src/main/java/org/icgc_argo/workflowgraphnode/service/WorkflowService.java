package org.icgc_argo.workflowgraphnode.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pivotal.rabbitmq.source.Sender;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc_argo.workflow_graph_lib.workflow.client.RdpcClient;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import org.icgc_argo.workflowgraphnode.controller.InvalidRequest;
import org.icgc_argo.workflowgraphnode.model.RestartInput;
import org.icgc_argo.workflowgraphnode.model.RestartRequest;
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
    return createParamsForRestart(request).map(restartInputSender::send).thenReturn(true);
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

              val builder = RestartInput.builder();

              run.getSessionId().ifPresent(builder::sessionId);

              engParam.getWorkDir().ifPresent(builder::workDir);
              engParam.getLaunchDir().ifPresent(builder::launchDir);
              engParam.getProjectDir().ifPresent(builder::projectDir);

              if (!request.getParams().isEmpty()) {
                builder.params(request.getParams());
              } else if (run.getParameters().isPresent()) {
                builder.params(MAPPER.convertValue(run.getParameters().get(), Map.class));
              } else {
                builder.params(Map.of());
              }

              return Mono.just(builder.build());
            });
  }
}
