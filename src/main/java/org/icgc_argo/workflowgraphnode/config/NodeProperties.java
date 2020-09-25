package org.icgc_argo.workflowgraphnode.config;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.icgc_argo.workflow_graph_lib.polyglot.enums.GraphFunctionLanguage;

import java.util.List;

@Data
@RequiredArgsConstructor
public class NodeProperties {

  private final GraphFunctionLanguage functionLanguage;
  private final String filterFunction;
  private final String gqlQueryString;
  private final String activationFunction;

  // Declares new subscribers on fanout exchange(s)
  private final List<TopologyProperties> input;

  // Direct exchange internal to this Node
  private final TopologyProperties running;

  // Complete exchange with self-archiving complete queue
  private final TopologyProperties complete;

  private final WorkflowProperties workflow;


  @Data
  @RequiredArgsConstructor
  public static class TopologyProperties {
    private final String exchange;
    private final String queue;
  }

  @Data
  @RequiredArgsConstructor
  public static class WorkflowProperties {
    private final String url;
    private final String revision;
    private final String schema;
    private final String schemaAvscPath;
  }
}
