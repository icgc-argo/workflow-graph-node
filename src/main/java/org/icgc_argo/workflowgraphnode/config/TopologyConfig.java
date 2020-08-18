package org.icgc_argo.workflowgraphnode.config;

import com.pivotal.rabbitmq.source.OnDemandSource;
import com.pivotal.rabbitmq.source.Sender;
import com.pivotal.rabbitmq.topology.ExchangeType;
import com.pivotal.rabbitmq.topology.TopologyBuilder;
import java.util.function.Consumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class TopologyConfig {

  @Bean
  OnDemandSource<String> runRequests() {
    return new OnDemandSource<>("runRequests");
  }

  @Bean
  @Primary
  Sender<String> runRequestsSender(OnDemandSource<String> runRequests) {
    return runRequests;
  }

  public Consumer<TopologyBuilder> queueTopology() {
    return topologyBuilder ->
        topologyBuilder
            .declareExchange("node-input")
            .type(ExchangeType.direct)
            .and()
            .declareQueue("run-queue")
            .boundTo("node-input");
  }

  public Consumer<TopologyBuilder> runningTopology() {
    return topologyBuilder ->
        topologyBuilder
            .declareExchange("node-state")
            .type(ExchangeType.direct)
            .and()
            .declareQueue("running")
            .boundTo("node-state");
  }

  public Consumer<TopologyBuilder> completeTopology() {
    return topologyBuilder ->
        topologyBuilder
            .declareExchange("node-complete")
            .type(ExchangeType.topic)
            .and()
            .declareQueue("default.complete")
            .boundTo("node-complete");
  }
}
