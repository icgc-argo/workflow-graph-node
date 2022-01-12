package org.icgc_argo.workflowgraphnode.model;

import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
@AllArgsConstructor
public class RestartInput {
  private final Map<String, Object> params;
  private String sessionId;

  public Optional<String> getSessionId() {
    return Optional.ofNullable(sessionId);
  }
}
