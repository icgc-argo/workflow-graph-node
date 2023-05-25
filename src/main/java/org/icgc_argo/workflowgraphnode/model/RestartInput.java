package org.icgc_argo.workflowgraphnode.model;

import java.util.Map;
import java.util.Optional;
import lombok.*;

@Getter
@RequiredArgsConstructor
@AllArgsConstructor
@Builder
public class RestartInput {
  @NonNull private final Map<String, Object> params;
  private String sessionId;
  private String workDir;
  private String projectDir;
  private String launchDir;

  public Optional<String> getSessionId() {
    return Optional.ofNullable(sessionId);
  }

  public Optional<String> getProjectDir() {
    return Optional.ofNullable(projectDir);
  }

  public Optional<String> getLaunchDir() {
    return Optional.ofNullable(launchDir);
  }

  public Optional<String> getWorkDir() {
    return Optional.ofNullable(workDir);
  }
}
