package org.icgc_argo.workflowgraphnode.model;

import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class RestartRequest {
  private String runId;
  private Map<String, Object> params = new HashMap<>();
}
