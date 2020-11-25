package org.icgc_argo.workflowgraphnode.model;

import lombok.Data;

import static java.lang.String.format;

@Data
public class GraphTransitObject {
  private final String pipeline;
  private final String node;
  private final String queue;
  private final String messageId;

  @Override
  public String toString() {
    return format("{ pipeline: %s, node: %s, queue: %s, messageId: %s }", pipeline, node, queue, messageId);
  }
}
