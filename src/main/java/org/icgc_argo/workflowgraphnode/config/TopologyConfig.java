package org.icgc_argo.workflowgraphnode.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pivotal.rabbitmq.source.OnDemandSource;
import com.pivotal.rabbitmq.source.Sender;
import com.pivotal.rabbitmq.topology.ExchangeType;
import com.pivotal.rabbitmq.topology.TopologyBuilder;

import java.io.FileInputStream;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class TopologyConfig {


  @Getter private final PipesProperties properties;

  @SneakyThrows
  public TopologyConfig(@Value("${node.jsonConfigPath}") String location) {
    val inputStream = new FileInputStream(location);
    this.properties = new ObjectMapper().readValue(inputStream, PipesProperties.class);
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
            .declareExchange(properties.getInput().getExchange())
            .type(ExchangeType.direct)
            .and()
            .declareQueue(properties.getInput().getQueue())
            .boundTo(properties.getInput().getExchange());
  }

  public Consumer<TopologyBuilder> runningTopology() {
    return topologyBuilder ->
        topologyBuilder
            .declareExchange(properties.getRunning().getExchange())
            .type(ExchangeType.direct)
            .and()
            .declareQueue(properties.getRunning().getQueue())
            .boundTo(properties.getRunning().getExchange());
  }

  public Consumer<TopologyBuilder> completeTopology() {
    return topologyBuilder ->
        topologyBuilder
            .declareExchange(properties.getComplete().getExchange())
            .type(ExchangeType.topic)
            .and()
            .declareQueue(properties.getComplete().getQueue())
            .boundTo(properties.getComplete().getExchange());
  }
}
