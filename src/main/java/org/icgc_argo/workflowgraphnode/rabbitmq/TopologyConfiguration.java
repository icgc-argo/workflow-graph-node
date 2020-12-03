package org.icgc_argo.workflowgraphnode.rabbitmq;

import com.pivotal.rabbitmq.source.OnDemandSource;
import com.pivotal.rabbitmq.source.Sender;
import com.pivotal.rabbitmq.topology.ExchangeType;
import com.pivotal.rabbitmq.topology.TopologyBuilder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.icgc_argo.workflowgraphnode.config.AppConfig;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.pivotal.rabbitmq.topology.ExchangeType.direct;
import static com.pivotal.rabbitmq.topology.ExchangeType.fanout;

@Configuration
public class TopologyConfiguration {

  private final List<NodeProperties.TopologyProperties> input;
  private final NodeProperties.TopologyProperties running;
  private final NodeProperties.TopologyProperties complete;

  @Autowired
  public TopologyConfiguration(AppConfig appConfig) {
    this.input = appConfig.getNodeProperties().getInput();
    this.running = appConfig.getNodeProperties().getRunning();
    this.complete = appConfig.getNodeProperties().getComplete();
  }

  @Bean
  OnDemandSource<Map<String, Object>> runRequests() {
    return new OnDemandSource<>("runRequests");
  }

  @Bean
  @Primary
  Sender<Map<String, Object>> runRequestsSender(OnDemandSource<Map<String, Object>> runRequests) {
    return runRequests;
  }

  public Stream<NodeInput> inputPropertiesAndTopologies() {
    return input.stream()
        .map(inputItem -> new NodeInput(inputItem, exchangeWithDLQTopoBuilder(inputItem, fanout)));
  }

  public Consumer<TopologyBuilder> runningTopology() {
    return exchangeWithDLQTopoBuilder(running, direct);
  }

  public Consumer<TopologyBuilder> completeTopology() {
    return exchangeWithDLQTopoBuilder(complete, fanout);
  }

  private Consumer<TopologyBuilder> exchangeWithDLQTopoBuilder(
      NodeProperties.TopologyProperties properties, ExchangeType type) {
    return (builder) -> {
      builder
          // DLX
          .declareExchange(properties.getExchange().concat("-dlx"))
          .type(type)
          .and()
          .declareQueue(properties.getQueue().concat("-dlq"))
          .boundTo(properties.getExchange().concat("-dlx"))
          .and()
          // Exchange + Queue Binding
          .declareExchange(properties.getExchange())
          .type(type)
          .and()
          .declareQueue(properties.getQueue())
          .boundTo(properties.getExchange())
          .withDeadLetterExchange(properties.getExchange().concat("-dlx"));
    };
  }

  @Data
  @RequiredArgsConstructor
  public static class NodeInput {
    private final NodeProperties.TopologyProperties properties;
    private final Consumer<TopologyBuilder> topologyBuilder;
  }
}
