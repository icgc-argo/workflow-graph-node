package org.icgc_argo.workflowgraphnode.logging;

import com.pivotal.rabbitmq.stream.Transaction;
import lombok.NonNull;
import lombok.val;
import org.icgc_argo.workflow_graph_lib.schema.GraphEvent;
import org.icgc_argo.workflow_graph_lib.schema.GraphRun;
import org.icgc_argo.workflowgraphnode.config.AppConfig;
import org.springframework.context.annotation.Configuration;

import static java.lang.String.format;

@Configuration
public class GraphLogger {
    private final String pipeline;
    private final String node;

    public GraphLogger(@NonNull AppConfig appConfig) {
        pipeline = appConfig.getNodeProperties().getPipelineId();
        node = appConfig.getNodeProperties().getNodeId();
    }

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
