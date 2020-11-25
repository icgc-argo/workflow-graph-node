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

  /**
   * Registers a GraphTransitObject from a GraphEvent transaction into the GTA registry
   *
   * @param tx the GraphEvent transaction to be registered as a GraphTransitObject
   * @return the previous GraphTransitObject registered for this id, or null if nothing previously
   *     registered (a null return can also indicate that the registry previously associated null
   *     with the id.)
   */
  public GraphTransitObject registerGraphEventTx(Transaction<GraphEvent> tx) {
    return registry.put(
        tx.id(), new GraphTransitObject(pipeline, node, tx.id().getName(), tx.get().getId()));
  }

  /**
   * Registers a GraphTransitObject from a GraphRun transaction into the GTA registry
   *
   * @param tx the GraphRun transaction to be registered as a GraphTransitObject
   * @return the previous GraphTransitObject registered for this id, or null if nothing previously
   *     registered (a null return can also indicate that the registry previously associated null
   *     with the id.)
   */
  public GraphTransitObject registerGraphRunTx(Transaction<GraphRun> tx) {
    return registry.put(
        tx.id(), new GraphTransitObject(pipeline, node, tx.id().getName(), tx.get().getId()));
  }

  /**
   * Registers a GraphTransitObject from a non-graph-entity transaction into the GTA registry
   *
   * @param tx the non-graph-entity transaction to be registered as a GraphTransitObject
   * @return the previous GraphTransitObject registered for this id, or null if nothing previously
   *     registered (a null return can also indicate that the registry previously associated null
   *     with the id.)
   */
  public GraphTransitObject registerNonEntityTx(Transaction<?> tx) {
    return registry.put(
        tx.id(), new GraphTransitObject(pipeline, node, tx.id().getName(), "NON-GRAPH-ENTITY"));
  }

  /**
   * Retrieve GraphTransitObject by id
   *
   * @param id - Transaction Identifier
   * @return the GraphTransitObject registered with the given id
   */
  public static GraphTransitObject getTransactionByIdentifier(Transactional.Identifier id) {
    return registry.get(id);
  }

  /**
   * Removes the GraphTransitObject with the given id from the registry or null if there was no
   * mapping for id
   *
   * @param id
   * @return the GraphTransitObject removed with the given id
   */
  public static GraphTransitObject removeTransactionFromGTARegistry(Transactional.Identifier id) {
    val result = registry.remove(id);
    log.debug(
        "Transaction {} removed from GTA Registry. Thank you for transiting! GTO: {}", id, result);

    return result;
  }
}
