package org.icgc_argo.workflowgraphnode.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties("pipes")
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
