package org.icgc_argo.workflowgraphnode.config;

import com.pivotal.rabbitmq.source.OnDemandSource;
import com.pivotal.rabbitmq.source.Sender;
import com.pivotal.rabbitmq.topology.ExchangeType;
import com.pivotal.rabbitmq.topology.TopologyBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.function.Consumer;

@Configuration
public class TopologyConfig {

  private final AppConfig appConfig;

  @Autowired
  public TopologyConfig(AppConfig appConfig) {
    this.appConfig = appConfig;
  }

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
            .declareExchange(appConfig.getNodeProperties().getInput().getExchange())
            .type(ExchangeType.direct)
            .and()
            .declareQueue(appConfig.getNodeProperties().getInput().getQueue())
            .boundTo(appConfig.getNodeProperties().getInput().getExchange());
  }

  public Consumer<TopologyBuilder> runningTopology() {
    return topologyBuilder ->
        topologyBuilder
            .declareExchange(appConfig.getNodeProperties().getRunning().getExchange())
            .type(ExchangeType.direct)
            .and()
            .declareQueue(appConfig.getNodeProperties().getRunning().getQueue())
            .boundTo(appConfig.getNodeProperties().getRunning().getExchange());
  }

  public Consumer<TopologyBuilder> completeTopology() {
    return topologyBuilder ->
        topologyBuilder
            .declareExchange(appConfig.getNodeProperties().getComplete().getExchange())
            .type(ExchangeType.topic)
            .and()
            .declareQueue(appConfig.getNodeProperties().getComplete().getQueue())
            .boundTo(appConfig.getNodeProperties().getComplete().getExchange());
  }
}
