package org.icgc_argo.workflowgraphnode.logging;

import com.pivotal.rabbitmq.stream.Transaction;
import lombok.val;

import static java.lang.String.format;
import static org.icgc_argo.workflowgraphnode.service.GraphTransitAuthority.getTransactionByIdentifier;

public class GraphLogger {

  public static String graphLog(Transaction<?> tx, String formattedMessage, Object... msgArgs) {
    val gto = getTransactionByIdentifier(tx.id());
    return new GraphLog(
            formatLog(formattedMessage, msgArgs),
            gto.getMessageId(),
            gto.getQueue(),
            gto.getNode(),
            gto.getPipeline())
        .toJSON();
  }

  private static String formatLog(String formattedMessage, Object... msgArgs) {
    return format(formattedMessage, msgArgs);
  }
}
