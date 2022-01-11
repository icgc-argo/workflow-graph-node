package org.icgc_argo.workflowgraphnode.model;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor
public class RestartRequest {
  String runId;
  Map<String, Object> params;

  public RestartRequest(String runId) {
    this.runId = runId;
    this.params = Map.of();
  }
}
