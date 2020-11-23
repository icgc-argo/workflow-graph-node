package org.icgc_argo.workflowgraphnode.service;

import com.pivotal.rabbitmq.stream.Transaction;
import com.pivotal.rabbitmq.stream.Transactional;
import lombok.NonNull;
import org.icgc_argo.workflowgraphnode.config.AppConfig;
import org.icgc_argo.workflowgraphnode.model.GraphTransitObject;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;

@Configuration
public class GraphTransitAuthority {

  private final String pipelineId;
  private final String nodeId;

  private final HashMap<Transactional.Identifier, GraphTransitObject> registry = new HashMap<>();

  public GraphTransitAuthority(@NonNull AppConfig appConfig) {
    pipelineId = appConfig.getNodeProperties().getPipelineId();
    nodeId = appConfig.getNodeProperties().getNodeId();
  }

  public void registerTransaction(Transactional.Identifier transactionId) {
    registry.put(transactionId, new GraphTransitObject(pipelineId, nodeId, transactionId.getName()));
  }

  public GraphTransitObject lookupTransactionByIdentifier(Transactional.Identifier id) {
    return registry.get(id);
  }
}
