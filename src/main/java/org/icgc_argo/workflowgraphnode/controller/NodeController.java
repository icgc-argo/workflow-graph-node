package org.icgc_argo.workflowgraphnode.controller;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pivotal.rabbitmq.source.Sender;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.icgc_argo.workflowgraphnode.model.PipeStatus;
import org.icgc_argo.workflowgraphnode.service.NodeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.Map;

@Slf4j
@RestController
public class NodeController {

  /** Constants */
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Dependencies */
  private final Sender<Map<String, Object>> sender;

  private final NodeService service;

  @Autowired
  public NodeController(@NonNull Sender<Map<String, Object>> sender, @NonNull NodeService service) {
    this.sender = sender;
    this.service = service;
  }

  @PostMapping("/enqueue")
  public Mono<ResponseEntity<Map<String, Object>>> enqueueBatch(
      @RequestBody Mono<Map<String, Object>> job) {
    return job.flatMap(runRequest -> Mono.fromCallable(() -> runRequest))
        .flatMap(sender::send)
        .map(ResponseEntity::ok);
  }

  @GetMapping("/status")
  public Mono<Map<String, PipeStatus>> getPipelineStatus() {
    return Mono.fromCallable(service::getStatus);
  }

  @Data
  public static class IncomingEvent {
    @JsonProperty(value = "workflow_params")
    Map<String, Object> workflowParams;
  }
}
