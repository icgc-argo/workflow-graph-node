package org.icgc_argo.workflowgraphnode.workflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.nio.file.Files;
import java.util.Map;

@Component
public class RdpcClient {

  @Value("classpath:graphql/checkWorkflowStatus.graphql")
  private Resource statusQueryFile;

  @Getter(lazy = true)
  private final String statusQuery = loadStatusQuery();

  // TODO: FINISH THIS IMPLEMENTATION!!!
  public Mono<String> getWorkflowStatus(String runId) {
    val variables = Map.of("runId", runId);
    return Mono.just("COMPLETE");
  }

  @SneakyThrows
  private String loadStatusQuery() {
    return new String(Files.readAllBytes(statusQueryFile.getFile().toPath()));
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class GraphQLQuery {
    @JsonProperty("variables")
    private Object variables;

    @JsonProperty("query")
    private String query;
  }
}
