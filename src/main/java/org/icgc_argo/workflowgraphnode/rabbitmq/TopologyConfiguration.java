package org.icgc_argo.workflowgraphnode.rabbitmq;

import static com.pivotal.rabbitmq.topology.ExchangeType.direct;
import static com.pivotal.rabbitmq.topology.ExchangeType.fanout;

import com.pivotal.rabbitmq.source.OnDemandSource;
import com.pivotal.rabbitmq.source.Sender;
import com.pivotal.rabbitmq.topology.ExchangeType;
import com.pivotal.rabbitmq.topology.TopologyBuilder;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.icgc_argo.workflowgraphnode.config.AppConfig;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import org.icgc_argo.workflowgraphnode.model.RestartInput;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

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

  /**
   * OnDemandSource used to inject Workflows/Runs directly into nodeConfiguration. A run needs
   * engineParams and RunParams (i.e. a map) but this source expectsFlux of RunParams only. The
   * engineParams is generated at run time with the configured nodeProperties.
   *
   * @return OnDemandSource for genrating Flux of run param Transactions.
   */
  @Bean
  OnDemandSource<Map<String, Object>> directInputSource() {
    return new OnDemandSource<>("directInputSource");
  }
  /**
   * Used to send events to the directInputSource. Sender is actually the other half of an
   * OnDemandSource, which allows us to send events to anyone listening to the source end.
   *
   * @return Sender for sending run param Transactions.
   */
  @Bean
  @Primary
  Sender<Map<String, Object>> directInputSender(
      OnDemandSource<Map<String, Object>> directInputSource) {
    return directInputSource;
  }

  /**
   * OnDemandSource used to inject re-run or resume Workflows/Runs into nodeConfiguration. The
   * source generates RestartInput which has RunParams and sessionId. RunParams is needed to start a
   * run same as in directInput. The sessionId isn't used by node, but it needs to be passed to RDPC
   * to decide if run can be resumed.
   *
   * @return OnDemandSource for generating Flux of RestartInput Transactions.
   */
  @Bean
  OnDemandSource<RestartInput> restartInputSource() {
    return new OnDemandSource<>("restartInputSource");
  }

  /**
   * Used to send events to the restartInputSource.
   *
   * @return Sender for sending RestartInput Transactions.
   */
  @Bean
  @Primary
  Sender<RestartInput> restartInputSender(OnDemandSource<RestartInput> restartInputSource) {
    return restartInputSource;
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
