package org.icgc_argo.workflowgraphnode.workflow;

import lombok.extern.slf4j.Slf4j;
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
@Slf4j
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
        .bodyToMono(
            Map
                .class) // TODO: Errors seem to result in ...
                        // "com.fasterxml.jackson.databind.JsonMappingException: Multi threaded
                        // access requested by thread Thread[reactor-http-nio-3,5,main] but is not
                        // allowed for language(s) js. (through reference chain:
                        // org.icgc_argo.workflowgraphnode.model.RunRequest["workflow_params"])"
                        // ??????
        .onErrorResume(
            e -> {
              log.error("Unexpected response: {}", e.getLocalizedMessage());
              return Mono.empty();
            })
        .map(map -> map.get("run_id").toString());
  }
}
