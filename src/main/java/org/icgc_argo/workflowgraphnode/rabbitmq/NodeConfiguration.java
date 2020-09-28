package org.icgc_argo.workflowgraphnode.rabbitmq;

import com.pivotal.rabbitmq.RabbitEndpointService;
import com.pivotal.rabbitmq.source.Source;
import com.pivotal.rabbitmq.stream.Transaction;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.icgc_argo.workflow_graph_lib.workflow.client.RdpcClient;
import org.icgc_argo.workflow_graph_lib.workflow.model.RunRequest;
import org.icgc_argo.workflowgraphnode.components.Errors;
import lombok.val;
import org.icgc_argo.workflowgraphnode.components.Node;
import org.icgc_argo.workflowgraphnode.config.AppConfig;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.time.Duration;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.icgc_argo.workflow_graph_lib.utils.JacksonUtils.toMap;

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

  public Disposable runningToComplete() {
    return rabbit
        .declareTopology(topologyConfig.completeTopology())
        .createTransactionalProducerStream(
            String.class) // TODO replace with universal message schema
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

  public Disposable inputToRunning() {
    // TODO: error handling and retry
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
        .doOnNext(tx -> log.info("Run request confirmed by RDPC, runId: {}", tx.get()))
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
        .doOnDiscard(Transaction.class, tx -> tx.rollback(true))
        .onErrorContinue(Errors.handle());
    // TODO: Enrich via graphQL and create universal event type here
  }

  private Flux<Transaction<RunRequest>> directInputStream() {
    return directInputSource
        .source()
        // TODO: source will provide Generic Record with schema spec'd in config json
        .handle(handleInputToRunRequest())
        .onErrorContinue(Errors.handle())
        .doOnNext(tx -> log.info("Run request created: {}", tx.get()));
  }

  private Flux<Transaction<RunRequest>> queuedInputStream() {
    // declare and merge all input queues provided in config
    val inputStreams =
        Flux.merge(
            topologyConfig
                .inputPropertiesAndTopologies()
                .map(
                    input ->
                        rabbit
                            .declareTopology(input.getTopologyBuilder())
                            .createTransactionalConsumerStream(
                                // TODO: use universal event type here instead of Record
                                input.getProperties().getQueue(), String.class)
                            .receive())
                .collect(Collectors.toList()));

    // Apply user filters
    for (NodeProperties.Filter filter : nodeProperties.getFilters()) {
      inputStreams
          .filter(node.filter(filter.getExpression()))
          .doOnDiscard(
              Transaction.class,
              tx -> {
                if (filter.getReject()) {
                  logFilterMessage("Filter failed (rejecting)", tx, filter);
                  tx.reject();
                } else {
                  logFilterMessage("Filter failed (no ack)", tx, filter);
                }
              })
          .doOnNext(tx -> logFilterMessage("Filter passed", tx, filter));
    }

    return inputStreams
        // flatMap needs a function that returns a Publisher that it then
        // resolves async by subscribing to it (ex. mono)
        //        .flatMap(node.gqlQuery())
        .map(tx -> tx.map(toMap(tx.get()))) // TODO: temp until we move RDPC client
        .doOnNext(tx -> log.info("GQL Response: {}", tx.get()))
        .map(node.activationFunction())
        .onErrorContinue(Errors.handle())
        .doOnNext(tx -> log.info("Activation Result: {}", tx.get()))
        // TODO: activation response should be Generic Record with schema spec'd in config json
        .handle(handleInputToRunRequest())
        .onErrorContinue(Errors.handle())
        .doOnNext(tx -> log.info("Run request created: {}", tx.get()));
  }

  private BiConsumer<Transaction<Map<String, Object>>, SynchronousSink<Transaction<RunRequest>>>
      handleInputToRunRequest() {
    return (tx, sink) -> sink.next(tx.map(node.inputToRunRequest().apply(tx.get())));
  }

  private void logFilterMessage(String preText, Transaction<String> tx, NodeProperties.Filter filter) {
    log.info(
        "{} for value: {}, with the following expression: {}",
        preText,
        tx.get(),
        filter.getExpression());
  }
}
