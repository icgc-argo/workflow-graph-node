package org.icgc_argo.workflowgraphnode.service;

import com.pivotal.rabbitmq.stream.Transaction;
import com.pivotal.rabbitmq.stream.Transactional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc_argo.workflow_graph_lib.schema.GraphEvent;
import org.icgc_argo.workflow_graph_lib.schema.GraphRun;
import org.icgc_argo.workflowgraphnode.config.AppConfig;
import org.icgc_argo.workflowgraphnode.model.GraphTransitObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.UUID;

@Service
@Slf4j
public class GraphTransitAuthority {

  private final String pipeline;
  private final String node;

  private static final HashMap<Transactional.Identifier, GraphTransitObject> registry =
      new HashMap<>();

  @Autowired
  public GraphTransitAuthority(@NonNull AppConfig appConfig) {
    this.pipeline = appConfig.getNodeProperties().getPipelineId();
    this.node = appConfig.getNodeProperties().getNodeId();
  }

  public GraphTransitAuthority(@NonNull String pipeline, @NonNull String node) {
    this.pipeline = pipeline;
    this.node = node;
  }

  public void registerGraphEventTx(Transaction<GraphEvent> tx) {
    registry.put(
        tx.id(), new GraphTransitObject(pipeline, node, tx.id().getName(), tx.get().getId()));
  }

  public void registerGraphRunTx(Transaction<GraphRun> tx) {
    registry.put(
        tx.id(), new GraphTransitObject(pipeline, node, tx.id().getName(), tx.get().getId()));
  }

  public void registerNonEntityTx(Transaction<?> tx) {
    registry.put(tx.id(), new GraphTransitObject(pipeline, node, tx.id().getName(), "NON-ENTITY"));
  }

  public static GraphTransitObject getTransactionByIdentifier(Transactional.Identifier id) {
    return registry.get(id);
  }

  public static void removeTransactionFromGTARegistry(Transactional.Identifier id) {
    val result = registry.remove(id);
    log.debug(
        "Transaction {} removed from GTA Registry. Thank you for transiting! GTO: {}", id, result);
  }
}
