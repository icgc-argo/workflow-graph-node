package org.icgc_argo.workflowgraphnode.workflow;

import org.icgc_argo.workflowgraphnode.config.AppConfig;
import org.icgc_argo.workflowgraphnode.model.RunRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.Map;

@Component
public class WesClient {

  /** Dependencies */
  private final AppConfig appConfig;

  @Autowired
  public WesClient(AppConfig appConfig) {
    this.appConfig = appConfig;
  }

  /**
   * Launch a workflow using the WES API
   *
   * @param runRequest run request containing the workflow params
   * @return Returns a Mono wrapped runId of the successfully launched workflow
   */
  public Mono<String> launchWorkflowWithWes(RunRequest runRequest) {
    // TODO: refactor to use GQL mutation (wes url should be GQL endpoint)
    return WebClient.create()
        .post()
        .uri(appConfig.getWesUrl())
        .contentType(MediaType.APPLICATION_JSON)
        .body(BodyInserters.fromValue(runRequest))
        .retrieve()
        .bodyToMono(Map.class)
        .map(map -> map.get("run_id").toString());
  }
}
