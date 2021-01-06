package org.icgc_argo.workflowgraphnode.logging;

import com.pivotal.rabbitmq.stream.Transaction;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;

import static java.lang.String.format;
import static org.icgc_argo.workflowgraphnode.service.GraphTransitAuthority.getTransactionByIdentifier;

@Slf4j
public class GraphLogger {

  /**
   * Wrapper around Slf4j logger, creates GraphLog, logs at INFO level, with marker "GraphLogMarker"
   * for a GTA registered transaction
   *
   * @param tx the transaction to lookup in the registry
   * @param formattedMessage - the formatted message string that will go in the `log` field of
   *     GraphLog
   * @param msgArgs - the formatted string args to be passed to String.format
   */
  public static void info(Transaction<?> tx, String formattedMessage, Object... msgArgs) {
    log.info(GraphLogMarker.getMarker(), graphLog(tx, formattedMessage, msgArgs));
  }

  /**
   * Wrapper around Slf4j logger, creates GraphLog, logs at DEBUG level, with marker
   * "GraphLogMarker" for a GTA registered transaction
   *
   * @param tx the transaction to lookup in the registry
   * @param formattedMessage - the formatted message string that will go in the `log` field of
   *     GraphLog
   * @param msgArgs - the formatted string args to be passed to String.format
   */
  public static void debug(Transaction<?> tx, String formattedMessage, Object... msgArgs) {
    log.debug(GraphLogMarker.getMarker(), graphLog(tx, formattedMessage, msgArgs));
  }

  /**
   * Wrapper around Slf4j logger, creates GraphLog, logs at WARN level, with marker "GraphLogMarker"
   * for a GTA registered transaction
   *
   * @param tx the transaction to lookup in the registry
   * @param formattedMessage - the formatted message string that will go in the `log` field of
   *     GraphLog
   * @param msgArgs - the formatted string args to be passed to String.format
   */
  public static void warn(Transaction<?> tx, String formattedMessage, Object... msgArgs) {
    log.warn(GraphLogMarker.getMarker(), graphLog(tx, formattedMessage, msgArgs));
  }

  /**
   * Wrapper around Slf4j logger, creates GraphLog, logs at ERROR level, with marker
   * "GraphLogMarker" for a GTA registered transaction
   *
   * @param tx the transaction to lookup in the registry
   * @param formattedMessage - the formatted message string that will go in the `log` field of
   *     GraphLog
   * @param msgArgs - the formatted string args to be passed to String.format
   */
  public static void error(Transaction<?> tx, String formattedMessage, Object... msgArgs) {
    log.error(GraphLogMarker.getMarker(), graphLog(tx, formattedMessage, msgArgs));
  }

  /**
   * Wrapper around Slf4j logger, creates GraphLog, logs at INFO level, with marker "GraphLogMarker"
   * for a GraphNode level log (ie. logs not related to messages directly but rather from the Node
   * itself)
   *
   * @param nodeProperties - the node to associate the log with
   * @param formattedMessage - the formatted message string that will go in the `log` field of
   *     GraphLog
   * @param msgArgs - the formatted string args to be passed to String.format
   */
  public static void info(
      NodeProperties nodeProperties, String formattedMessage, Object... msgArgs) {
    log.info(GraphLogMarker.getMarker(), graphLog(nodeProperties, formattedMessage, msgArgs));
  }

  /**
   * Wrapper around Slf4j logger, creates GraphLog, logs at DEBUG level, with marker
   * "GraphLogMarker" for a GraphNode level log (ie. logs not related to messages directly but
   * rather from the Node itself)
   *
   * @param nodeProperties - the node to associate the log with
   * @param formattedMessage - the formatted message string that will go in the `log` field of
   *     GraphLog
   * @param msgArgs - the formatted string args to be passed to String.format
   */
  public static void debug(
      NodeProperties nodeProperties, String formattedMessage, Object... msgArgs) {
    log.debug(GraphLogMarker.getMarker(), graphLog(nodeProperties, formattedMessage, msgArgs));
  }

  /**
   * Wrapper around Slf4j logger, creates GraphLog, logs at WARN level, with marker "GraphLogMarker"
   * for a GraphNode level log (ie. logs not related to messages directly but rather from the Node
   * itself)
   *
   * @param nodeProperties - the node to associate the log with
   * @param formattedMessage - the formatted message string that will go in the `log` field of
   *     GraphLog
   * @param msgArgs - the formatted string args to be passed to String.format
   */
  public static void warn(
      NodeProperties nodeProperties, String formattedMessage, Object... msgArgs) {
    log.warn(GraphLogMarker.getMarker(), graphLog(nodeProperties, formattedMessage, msgArgs));
  }

  /**
   * Wrapper around Slf4j logger, creates GraphLog, logs at ERROR level, with marker
   * "GraphLogMarker" for a GraphNode level log (ie. logs not related to messages directly but
   * rather from the Node itself)
   *
   * @param nodeProperties - the node to associate the log with
   * @param formattedMessage - the formatted message string that will go in the `log` field of
   *     GraphLog
   * @param msgArgs - the formatted string args to be passed to String.format
   */
  public static void error(
      NodeProperties nodeProperties, String formattedMessage, Object... msgArgs) {
    log.error(GraphLogMarker.getMarker(), graphLog(nodeProperties, formattedMessage, msgArgs));
  }

  /**
   * Creates a JSON string representation of a GraphLog object for a GTA registered transaction
   *
   * @param tx the transaction to lookup in the registry
   * @param formattedMessage - the formatted message string that will go in the `log` field of
   *     GraphLog
   * @param msgArgs - the formatted string args to be passed to String.format
   * @return the JSON string representation of the newly created GraphLog object
   */
  private static String graphLog(Transaction<?> tx, String formattedMessage, Object... msgArgs) {
    val graphTransitObject = getTransactionByIdentifier(tx.id());

    if (graphTransitObject.isPresent()) {
      val gto = graphTransitObject.get();

      return new GraphLog(
              format(formattedMessage, msgArgs),
              gto.getMessageId(),
              gto.getQueue(),
              gto.getNode(),
              gto.getPipeline())
          .toJSON();
    } else {
      return String.format("GraphTransitObject with id: %s not found in the Graph Transit Registry, it either never existed or more likely was removed before this log statement", tx.id());
    }
  }

  /**
   * Creates a JSON string representation of a GraphLog object for a GraphNode level log (ie. logs
   * not related to messages directly but rather from the Node itself)
   *
   * @param nodeProperties - the node to associate the log with
   * @param formattedMessage - the formatted message string that will go in the `log` field of
   *     GraphLog
   * @param msgArgs - the formatted string args to be passed to String.format
   * @return the JSON string representation of the newly created GraphLog object
   */
  private static String graphLog(
      NodeProperties nodeProperties, String formattedMessage, Object... msgArgs) {
    return new GraphLog(
            format(formattedMessage, msgArgs),
            "",
            "",
            nodeProperties.getNodeId(),
            nodeProperties.getPipelineId())
        .toJSON();
  }
}
