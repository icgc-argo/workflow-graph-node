package org.icgc_argo.workflowgraphnode.logging;

import lombok.Data;
import lombok.NonNull;

import java.time.Instant;

import static org.icgc_argo.workflow_graph_lib.utils.JacksonUtils.toJsonString;

@Data
public class GraphLog {
  private final String log;
  private final String graphMessageId;
  private final String queue;
  private final String node;
  private final String pipeline;
  private final Long timestamp;

  public GraphLog(
      @NonNull String log,
      @NonNull String graphMessageId,
      @NonNull String queue,
      @NonNull String node,
      @NonNull String pipeline) {
    this.log = log;
    this.graphMessageId = graphMessageId;
    this.queue = queue;
    this.node = node;
    this.pipeline = pipeline;
    this.timestamp = Instant.now().toEpochMilli();
  }

  /**
   * Outputs object as JSON string using Jackson OBJECT_MAPPER.writeValueAsString()
   * @return JSON string representation of instantiated object
   */
  public String toJSON() {
    return toJsonString(this);
  }
}
