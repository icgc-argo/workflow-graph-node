package org.icgc_argo.workflowgraphnode.controller;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pivotal.rabbitmq.source.Sender;
import java.util.Map;
import lombok.Data;
import lombok.NonNull;
import org.icgc_argo.workflowgraphnode.model.PipeStatus;
import org.icgc_argo.workflowgraphnode.model.RestartRequest;
import org.icgc_argo.workflowgraphnode.service.PipelineManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
public class NodeController {

  /** Constants */
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Dependencies */
  private final Sender<Map<String, Object>> sender;

  private final PipelineManager service;

  @Autowired
  public NodeController(
      @NonNull Sender<Map<String, Object>> sender, @NonNull PipelineManager service) {
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

  @PostMapping("/restart")
  public Mono<ResponseEntity<Map<String, Object>>> restart(RestartRequest request) {
    return Mono.empty();
  }

  @ExceptionHandler
  public ResponseEntity<String> handle(Throwable ex) {
    if (ex instanceof InvalidRequest) {
      return new ResponseEntity<>(ex.toString(), HttpStatus.BAD_REQUEST);
    } else {
      return new ResponseEntity<>("Internal Server Error", HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @Data
  public static class IncomingEvent {
    @JsonProperty(value = "workflow_params")
    Map<String, Object> workflowParams;
  }
}
