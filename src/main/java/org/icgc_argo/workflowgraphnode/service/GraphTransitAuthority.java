package org.icgc_argo.workflowgraphnode.service;

import com.pivotal.rabbitmq.stream.Transaction;
import com.pivotal.rabbitmq.stream.Transactional;
import java.util.HashMap;
import java.util.Optional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc_argo.workflow_graph_lib.schema.GraphEvent;
import org.icgc_argo.workflow_graph_lib.schema.GraphRun;
import org.icgc_argo.workflowgraphnode.config.AppConfig;
import org.icgc_argo.workflowgraphnode.model.GraphTransitObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
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
    val gto = new GraphTransitObject(pipeline, node, tx.id().getName(), tx.get().getId());
    log.info(("registerGraphEventTx: tx: "+tx.get()+" -- id: "+tx.id()));
    return putGTOinRegistry(tx.id(), gto);
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
    val gto = new GraphTransitObject(pipeline, node, tx.id().getName(), tx.get().getId());
    log.info(("registerGraphRunTx: tx: "+tx.get()+" -- id: "+tx.id()));
    return putGTOinRegistry(tx.id(), gto);
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
    val gto = new GraphTransitObject(pipeline, node, tx.id().getName(), "NON-GRAPH-ENTITY");
    log.info(("registerNonEntityTx: tx: "+tx.get()+" -- id: "+tx.id()));
    return putGTOinRegistry(tx.id(), gto);
  }

  /**
   * Retrieve GraphTransitObject by id
   *
   * @param id - Transaction Identifier
   * @return an optional GraphTransitObject registered with the given id
   */
  public static Optional<GraphTransitObject> getTransactionByIdentifier(
      Transactional.Identifier id) {
    return Optional.ofNullable(registry.get(id));
  }

  /**
   * Commits the transaction and removes it's associated GraphTransitObject from the
   * GraphTransitAuthority registry (should only be used in the Errors component or as the final
   * subscribe consumer)
   *
   * @param tx the transaction to be committed and for which the corresponding GTO should be removed
   *     from the GTA
   */
  public static void commitAndRemoveTransactionFromGTA(Transaction<?> tx) {
    tx.commit();
    removeTransactionFromGTARegistry(tx.id());
  }

  /**
   * Rejects the transaction and removes it's associated GraphTransitObject from the
   * GraphTransitAuthority registry (should only be used in the Errors component)
   *
   * @param tx the transaction to be rejected and for which the corresponding GTO should be removed
   *     from the GTA
   */
  public static void rejectAndRemoveTransactionFromGTA(Transaction<?> tx) {
    tx.reject();
    removeTransactionFromGTARegistry(tx.id());
  }

  /**
   * Requeue (tx.rollback(true)) the transaction and removes it's associated GraphTransitObject from
   * the GraphTransitAuthority registry (should only be used in the Errors component)
   *
   * @param tx the transaction that will requeue and for which the corresponding GTO should be
   *     removed from the GTA
   */
  public static void requeueAndRemoveTransactionFromGTA(Transaction<?> tx) {
    tx.rollback(true);
    removeTransactionFromGTARegistry(tx.id());
  }

  private static GraphTransitObject putGTOinRegistry(
      Transactional.Identifier id, GraphTransitObject gto) {
    log.info(("putGTOinRegistry: identifier: "+id));
    val result = registry.put(id, gto);
    if (result != null) {
      log.warn(
          "Previously registered GraphTransitObject with id \"{}\" has been replaced before being cleared via a commit/reject/requeue action, this may be an error. Previous GTO: {}",
          id,
          result);
    } else {
      log.debug(
          "Transaction with id \"{}\" registered with Graph Transit Authority! Graph Transit Object: {}",
          id,
          gto);
    }
    log.info(("putGTOinRegistry: registry: "+registry));
    return result;
  }

  private static GraphTransitObject removeTransactionFromGTARegistry(Transactional.Identifier id) {
    log.info(("putGTOinRegistry: removing from registry: "+id));
    val result = registry.remove(id);

    if (result == null) {
      log.warn(
          "Attempted to remove GraphTransitObject with id: \"{}\", but no such id exists in the GTA registry",
          id);
    } else {
      log.debug(
          "Transaction {} removed from GTA Registry. Thank you for transiting! GTO: {}",
          id,
          result);
    }
    log.info(("putGTOinRegistry: registry after clearing: "+registry));
    return result;
  }
}
