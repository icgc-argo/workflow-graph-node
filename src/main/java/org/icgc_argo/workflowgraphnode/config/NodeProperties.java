package org.icgc_argo.workflowgraphnode.config;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.icgc_argo.workflow_graph_lib.polyglot.enums.GraphFunctionLanguage;

@Data
public class NodeProperties {

  private GraphFunctionLanguage graphFunctionLanguage;
  private InputPipeProperties input;
  private PipeProperties running;
  private PipeProperties complete;

  @Data
  public static class PipeProperties {
    private String exchange;
    private String queue;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class InputPipeProperties extends PipeProperties {
    private String workflowUrl;
    private String workflowVersion;
    private String workflowParamsFunction;
  }
}
