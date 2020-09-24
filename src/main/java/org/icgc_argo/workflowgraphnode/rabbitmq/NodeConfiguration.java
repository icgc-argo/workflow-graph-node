package org.icgc_argo.workflowgraphnode.rabbitmq;

import com.pivotal.rabbitmq.RabbitEndpointService;
import com.pivotal.rabbitmq.source.Source;
import com.pivotal.rabbitmq.stream.Transaction;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.icgc_argo.workflowgraphnode.components.Node;
import org.icgc_argo.workflowgraphnode.config.AppConfig;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import org.icgc_argo.workflowgraphnode.model.RunRequest;
import org.icgc_argo.workflowgraphnode.workflow.RdpcClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.time.Duration;
import java.util.Map;
import java.util.function.BiConsumer;

@Slf4j
@Configuration
public class NodeConfiguration {
  private final RdpcClient rdpcClient;
  private final RabbitEndpointService rabbit;
  private final TopologyConfiguration topologyConfig;
  private final NodeProperties nodeProperties;
  private final Node node;
  private final Source<Map<String, Object>> directInputSource;

  @Autowired
  public NodeConfiguration(
      @NonNull RdpcClient rdpcClient,
      @NonNull RabbitEndpointService rabbit,
      @NonNull TopologyConfiguration topologyConfig,
      @NonNull AppConfig appConfig,
      @NonNull Node node,
      @NonNull Source<Map<String, Object>> directInputSource) {
    this.rdpcClient = rdpcClient;
    this.rabbit = rabbit;
    this.topologyConfig = topologyConfig;
    this.nodeProperties = appConfig.getNodeProperties();
    this.node = node;
    this.directInputSource = directInputSource;
  }

  public Disposable nodeStream() {
    return rabbit
        .declareTopology(topologyConfig.completeTopology())
        .createTransactionalProducerStream(String.class) // TODO replace with universal message schema
        .route()
        .toExchange(nodeProperties.getComplete().getExchange())
        .and()
        .whenNackByBroker()
        .alwaysRetry(Duration.ofSeconds(5))
        .then()
        .send(runningToCompleteStream())
        .doOnNext(tx -> log.info("Completed: {}", tx.get()))
        .subscribe(Transaction::commit);
  }

  private Flux<Transaction<String>> runningToCompleteStream() {
    return rabbit
        .declareTopology(topologyConfig.runningTopology())
        .createTransactionalConsumerStream(nodeProperties.getRunning().getQueue(), String.class)
        .receive()
        .delayElements(Duration.ofSeconds(10))
        .doOnNext(r -> log.info("Checking status of: {}", r.get()))
        .filterWhen(
            tx ->
                rdpcClient
                    .getWorkflowStatus(tx.get())
                    .map(
                        s -> {
                          log.info("Status for {}: {}", tx.get(), s);
                          return "COMPLETE".equals(s);
                        }))
        .doOnDiscard(Transaction.class, tx -> tx.rollback(true));
        // TODO: transform to universal event type here
  }

  private Flux<Transaction<String>> inputToRunningStream() {
    Flux<Transaction<String>> launchWorkflowStream =
        Flux.merge(directInputStream(), queuedInputStream())
            .doOnNext(item -> log.info("Attempting to run workflow with: {}", item.get()))
            .flatMap(
                tx ->
                    rdpcClient
                        .startRun(tx.get())
                        .flatMap(response -> Mono.fromCallable(() -> tx.map(response))));

    return rabbit
        .declareTopology(topologyConfig.runningTopology())
        .createTransactionalProducerStream(String.class)
        .route()
        .toExchange(nodeProperties.getRunning().getExchange())
        .and()
        .whenNackByBroker()
        .alwaysRetry(Duration.ofSeconds(5))
        .then()
        .send(launchWorkflowStream)
        .doOnNext(tx -> log.info("Run request sent to RDPC: {}", tx.get()));
  }

  private Flux<Transaction<RunRequest>> directInputStream() {
    return directInputSource
        .source()
        .handle(handleInputToRunning())
        .doOnNext(tx -> log.info("Run request created: {}", tx.get()));
  }

  private Flux<Transaction<RunRequest>> queuedInputStream() {
    return rabbit
        .declareTopology(topologyConfig.inputTopology())
        .createTransactionalConsumerStream(
            nodeProperties.getInput().getQueue(),
            GenericData.Record.class) // TODO replace with universal message schema
        .receive()
        .filter(node.filter())
        .doOnDiscard(
            Transaction.class,
            tx -> {
              log.info("Source Filter Fail (no ack): {}", tx.get());
            })
        .doOnNext(tx -> log.info("Source Filter Pass: {}", tx.get()))
        // flatMap needs a function that returns a Publisher that it then
        // resolves async by subscribing to it (ex. mono)
        .flatMap(node.gqlQuery())
        .doOnNext(tx -> log.info("GQL Response: {}", tx.get()))
        .map(node.activationFunction())
        .doOnNext(tx -> log.info("Activation Result: {}", tx.get()))
        .handle(handleInputToRunning())
        .doOnNext(tx -> log.info("Run request created: {}", tx.get()));
  }

  private BiConsumer<Transaction<Map<String, Object>>, SynchronousSink<Transaction<RunRequest>>>
      handleInputToRunning() {
    return (tx, sink) -> {
      try {
        sink.next(tx.map(node.inputToRunning().apply(tx.get())));
      } catch (Throwable e) {
        log.error(e.getLocalizedMessage());
        tx.reject();
      }
    };
  }
}
