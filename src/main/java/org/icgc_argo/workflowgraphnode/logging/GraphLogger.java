package org.icgc_argo.workflowgraphnode.logging;

import com.pivotal.rabbitmq.stream.Transaction;
import org.icgc_argo.workflow_graph_lib.schema.GraphEvent;
import org.icgc_argo.workflow_graph_lib.schema.GraphRun;

import static java.lang.String.format;

public class GraphLogger {

    public GraphLog createGraphEventLog(Transaction<GraphEvent> tx, String formattedMessage, Object... msgArgs) {
        return new GraphLog(formatLog(formattedMessage, msgArgs), pipeline, node, tx.id().getName(), tx.get().getId());
    }

    public GraphLog createGraphRunLog(Transaction<GraphRun> tx, String formattedMessage, Object... msgArgs) {
        return new GraphLog(formatLog(formattedMessage, msgArgs), pipeline, node, tx.id().getName(), tx.get().getId());
    }

    private String formatLog(String formattedMessage, Object... msgArgs) {
        return format(formattedMessage, msgArgs);
    }
}
