package org.icgc_argo.workflowgraphnode.workflow;

import java.util.Map;
import org.icgc_argo.workflowgraphnode.model.RunRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Component
public class WesClient {

  /** State */
  @Value("${workflow.url}")
  private String workflowUrl;

  @Value("${workflow.service.execution.url}")
  private String executionServiceUrl;

  /**
   * Launch a workflow using the WES API
   *
   * @param runRequest run request containing the workflow params
   * @return Returns a Mono wrapped runId of the successfully launched workflow
   */
  public Mono<String> launchWorkflowWithWes(RunRequest runRequest) {
    runRequest.setWorkflowUrl(workflowUrl);
    return WebClient.create()
        .post()
        .uri(executionServiceUrl)
        .contentType(MediaType.APPLICATION_JSON)
        .body(BodyInserters.fromValue(runRequest))
        .retrieve()
        .bodyToMono(Map.class)
        .map(map -> map.get("run_id").toString());
  }
}
