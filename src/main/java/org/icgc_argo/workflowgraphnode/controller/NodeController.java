package org.icgc_argo.workflowgraphnode.controller;

import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.icgc_argo.workflowgraphnode.model.PipeStatus;
import org.icgc_argo.workflowgraphnode.model.RestartRequest;
import org.icgc_argo.workflowgraphnode.service.PipelineManager;
import org.icgc_argo.workflowgraphnode.service.WorkflowService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@Slf4j
@RestController
@RequiredArgsConstructor
public class NodeController {
  /** Dependencies */
  private final WorkflowService workflowService;

  private final PipelineManager service;

  @PostMapping("/workflow/inject")
  public Mono<ResponseEntity<Boolean>> inject(@RequestBody Map<String, Object> params) {
    return workflowService.inject(params).map(ResponseEntity::ok);
  }

  @PostMapping("/workflow/restart")
  public Mono<ResponseEntity<Boolean>> restart(@RequestBody RestartRequest request) {
    return workflowService.restart(request).map(ResponseEntity::ok);
  }

  @GetMapping("/status")
  public Mono<Map<String, PipeStatus>> getPipelineStatus() {
    return Mono.fromCallable(service::getStatus);
  }

  @ExceptionHandler
  public ResponseEntity<String> handle(Throwable ex) {
    log.error(ex.toString());
    if (ex instanceof InvalidRequest) {
      return new ResponseEntity<>(ex.toString(), ((InvalidRequest) ex).getStatusCode());
    } else {
      return new ResponseEntity<>("Internal Server Error", HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }
}
