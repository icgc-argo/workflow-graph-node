package org.icgc_argo.workflowgraphnode.config;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.icgc_argo.workflow_graph_lib.polyglot.enums.GraphFunctionLanguage;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class NodeProperties {

  private GraphFunctionLanguage functionLanguage;
  private List<Filter> filters;
  private String gqlQueryString;
  private String activationFunction;

  // Declares new subscribers on fanout exchange(s)
  private List<TopologyProperties> input;

  // Direct exchange internal to this Node
  private TopologyProperties running;

  // Complete exchange with self-archiving complete queue
  private TopologyProperties complete;

  private WorkflowProperties workflow;

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class TopologyProperties {
    private String exchange;
    private String queue;
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class Filter {
    private String expression;
    private Boolean reject;
  }

  @Data
  @RequiredArgsConstructor
  public static class WorkflowProperties {
    private String url;
    private String revision;
    private String schema;
    private String schemaAvscPath;
  }
}
