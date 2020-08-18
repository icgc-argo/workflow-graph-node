package org.icgc_argo.workflowgraphnode.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import lombok.Data;

@Data
public class RunRequest {
  @JsonProperty(value = "workflow_url")
  String workflowUrl;

  @JsonProperty(value = "workflow_params")
  Map<String, Object> workflowParams;
}
