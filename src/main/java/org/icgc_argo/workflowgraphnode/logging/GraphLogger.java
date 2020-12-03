package org.icgc_argo.workflowgraphnode.logging;

import com.pivotal.rabbitmq.stream.Transaction;
import lombok.val;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;

import static java.lang.String.format;
import static org.icgc_argo.workflowgraphnode.service.GraphTransitAuthority.getTransactionByIdentifier;

public class GraphLogger {

  /**
   * Creates a JSON string representation of a GraphLog object for a GTA registered transaction
   *
   * @param tx the transaction to lookup in the registry
   * @param formattedMessage - the formatted message string that will go in the `log` field of
   *     GraphLog
   * @param msgArgs - the formatted string args to be passed to String.format
   * @return the JSON string representation of the newly created GraphLog object
   */
  public static String graphLog(Transaction<?> tx, String formattedMessage, Object... msgArgs) {
    val gto = getTransactionByIdentifier(tx.id());
    return new GraphLog(
            format(formattedMessage, msgArgs),
            gto.getMessageId(),
            gto.getQueue(),
            gto.getNode(),
            gto.getPipeline())
        .toJSON();
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
  public static String graphLog(
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
