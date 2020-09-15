package org.icgc_argo.workflowgraphnode.config;

import lombok.Data;

@Data
public class PipesProperties {

  private PipeProperties input;
  private PipeProperties running;
  private PipeProperties complete;

  @Data
  public static class PipeProperties {
    private String exchange;
    private String queue;
  }
}
