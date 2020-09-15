package org.icgc_argo.workflowgraphnode.config;

import lombok.Data;
import org.icgc_argo.workflow_graph_lib.polyglot.enums.GraphFunctionLanguage;

@Data
public class NodeProperties {

  private WorkflowProperties workflow;
  private String workflowParamsFunction;
  private GraphFunctionLanguage workflowParamsFunctionLanguage;
  private PipeProperties input;
  private PipeProperties running;
  private PipeProperties complete;

  @Data
  public static class PipeProperties {
    private String exchange;
    private String queue;
  }

  @Data
  public static class WorkflowProperties {
    private String workflowUrl;
    private String workflowVersion;
  }
}
