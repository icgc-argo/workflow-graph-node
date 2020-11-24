package org.icgc_argo.workflowgraphnode.logging;

import lombok.Data;
import lombok.NonNull;

import java.time.Instant;

import static java.lang.String.format;
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

  // TODO: Look into making this output to the console while toJSON outputs to Kafka
  @Override
  public String toString() {
    return format("%s - GraphLog: %s", this.log, this.toJSON());
  }

  public String toJSON() {
    return toJsonString(this);
  }
}
