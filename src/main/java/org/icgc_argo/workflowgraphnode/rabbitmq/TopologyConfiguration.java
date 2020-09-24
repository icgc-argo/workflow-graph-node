package org.icgc_argo.workflowgraphnode.rabbitmq;

import com.pivotal.rabbitmq.topology.ExchangeType;
import com.pivotal.rabbitmq.topology.TopologyBuilder;
import org.icgc_argo.workflowgraphnode.config.AppConfig;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Consumer;

import static com.pivotal.rabbitmq.topology.ExchangeType.direct;
import static com.pivotal.rabbitmq.topology.ExchangeType.fanout;

@Configuration
public class TopologyConfiguration {

  private final NodeProperties.TopologyProperties input;
  private final NodeProperties.TopologyProperties running;
  private final NodeProperties.TopologyProperties complete;

  @Autowired
  public TopologyConfiguration(AppConfig appConfig) {
    this.input = appConfig.getNodeProperties().getInput();
    this.running = appConfig.getNodeProperties().getRunning();
    this.complete = appConfig.getNodeProperties().getComplete();
  }

  public Consumer<TopologyBuilder> inputTopology() {
    return exchangeWithDLQTopoBuilder(input, fanout);
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
}
