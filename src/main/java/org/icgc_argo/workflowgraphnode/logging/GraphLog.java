package org.icgc_argo.workflowgraphnode.logging;

import lombok.NonNull;

import java.time.Instant;
import java.util.UUID;

import static java.lang.String.format;
import static org.icgc_argo.workflow_graph_lib.utils.JacksonUtils.toJsonString;

public class GraphLog {
  private final UUID id;
  private final Long timestamp;
  private final String log;
  private final String pipeline;
  private final String node;
  private final String queue;
  private final String graphMessageId;

  public GraphLog(
      @NonNull String log,
      @NonNull String pipeline,
      @NonNull String node,
      @NonNull String queue,
      @NonNull String graphMessageId) {
    this.id = UUID.randomUUID();
    this.timestamp = Instant.now().toEpochMilli();
    this.log = log;
    this.pipeline = pipeline;
    this.node = node;
    this.queue = queue;
    this.graphMessageId = graphMessageId;
  }

  @Override
  public String toString() {
    return format("%s - GraphLog: %s", this.log, this.toJSON());
  }

  public String toJSON() {
    return toJsonString(this);
  }
}
