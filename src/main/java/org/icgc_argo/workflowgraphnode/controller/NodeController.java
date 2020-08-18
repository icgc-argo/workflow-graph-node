package org.icgc_argo.workflowgraphnode.controller;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pivotal.rabbitmq.source.Sender;
import java.util.Map;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@Slf4j
@RestController
public class NodeController {

  /** Constants */
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Dependencies */
  private final Sender<String> sender;

  @Autowired
  public NodeController(@NonNull Sender<String> sender) {
    this.sender = sender;
  }

  @PostMapping("/enqueue")
  public Mono<ResponseEntity<String>> enqueueBatch(@RequestBody Mono<IncomingEvent> job) {
    return job.flatMap(runRequest -> Mono.fromCallable(() -> MAPPER.writeValueAsString(runRequest)))
        .flatMap(sender::send)
        .map(ResponseEntity::ok);
  }

  @Data
  public static class IncomingEvent {
    @JsonProperty(value = "workflow_params")
    Map<String, Object> workflowParams;
  }
}
