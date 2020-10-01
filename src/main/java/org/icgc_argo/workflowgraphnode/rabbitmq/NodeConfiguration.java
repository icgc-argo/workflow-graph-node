package org.icgc_argo.workflowgraphnode.rabbitmq;

import static java.lang.String.format;

import com.pivotal.rabbitmq.RabbitEndpointService;
import com.pivotal.rabbitmq.ReactiveRabbit;
import com.pivotal.rabbitmq.source.Source;
import com.pivotal.rabbitmq.stream.Transaction;
import java.time.Duration;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.AvroRuntimeException;
import org.icgc_argo.workflow_graph_lib.exceptions.DeadLetterQueueableException;
import org.icgc_argo.workflow_graph_lib.schema.GraphEvent;
import org.icgc_argo.workflow_graph_lib.workflow.client.RdpcClient;
import org.icgc_argo.workflow_graph_lib.workflow.model.RunRequest;
import org.icgc_argo.workflowgraphnode.components.Errors;
import org.icgc_argo.workflowgraphnode.components.Node;
import org.icgc_argo.workflowgraphnode.config.AppConfig;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

@Slf4j
@Configuration
public class NodeConfiguration {
  private final RdpcClient rdpcClient;
  private final RabbitEndpointService rabbit;
  private final ReactiveRabbit reactiveRabbit;
  private final TopologyConfiguration topologyConfig;
  private final NodeProperties nodeProperties;
  private final Node node;
  private final Source<Map<String, Object>> directInputSource;
  private final String schemaFullName;

  @Autowired
  public NodeConfiguration(
      @NonNull RdpcClient rdpcClient,
      @NonNull RabbitEndpointService rabbit,
      @NonNull ReactiveRabbit reactiveRabbit,
      @NonNull TopologyConfiguration topologyConfig,
      @NonNull AppConfig appConfig,
      @NonNull Node node,
      @NonNull Source<Map<String, Object>> directInputSource) {
    this.rdpcClient = rdpcClient;
    this.rabbit = rabbit;
    this.reactiveRabbit = reactiveRabbit;
    this.topologyConfig = topologyConfig;
    this.nodeProperties = appConfig.getNodeProperties();
    this.node = node;
    this.directInputSource = directInputSource;
    this.schemaFullName =
        format(
            "%s.%s",
            this.nodeProperties.getWorkflow().getSchemaNamespace(),
            this.nodeProperties.getWorkflow().getSchemaName());
  }

  public Disposable runningToComplete() {
    return rabbit
        .declareTopology(topologyConfig.completeTopology())
        .createTransactionalProducerStream(GraphEvent.class)
        .route()
        .toExchange(nodeProperties.getComplete().getExchange())
        .and()
        .whenNackByBroker()
        .alwaysRetry(Duration.ofSeconds(5))
        .then()
        .send(runningToCompleteStream())
        .onErrorContinue(Errors.handle())
        .doOnNext(tx -> log.info("Completed: {}", tx.get()))
        .subscribe(Transaction::commit);
  }

  public Disposable inputToRunning() {
    // TODO: error handling and retry
    Flux<Transaction<String>> launchWorkflowStream =
        Flux.merge(directInputStream(), queuedInputStream())
            .doOnNext(item -> log.info("Attempting to run workflow with: {}", item.get()))
            // flatMap needs a function that returns a Publisher that it then
            // resolves async by subscribing to it (ex. mono)
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

  private Flux<Transaction<GraphEvent>> runningToCompleteStream() {
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
        .onErrorContinue(Errors.handle())
        .flatMap(
            tx ->
                rdpcClient
                    .createGraphEventsForRun(tx.get())
                    .onErrorContinue(Errors.handle())
                    .flatMapIterable(
                        response ->
                            response.stream()
                                .map(tx::spawn)
                                .collect(
                                    Collectors.collectingAndThen(
                                        Collectors.toList(),
                                        list -> {
                                          // commit the parent transaction after spawning children
                                          // (won't be fully committed until each child is
                                          // committed once it is sent to the complete exchange)
                                          tx.commit();
                                          return list;
                                        }))));
  }

  private Flux<Transaction<RunRequest>> directInputStream() {
    return directInputSource
        .source()
        .handle(verifyParamsWithSchema()) // verify manually provided input matches schema
        .onErrorContinue(Errors.handle())
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
                                input.getProperties().getQueue(), GraphEvent.class)
                            .receive())
                .collect(Collectors.toList()));

    // Apply user filters
    for (NodeProperties.Filter filter : nodeProperties.getFilters()) {
      inputStreams
          .filter(node.filter(filter.getExpression()))
          .onErrorContinue(Errors.handle())
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
        .flatMap(node.gqlQuery())
        .doOnNext(tx -> log.info("GQL Response: {}", tx.get()))
        .map(node.activationFunction())
        .onErrorContinue(Errors.handle())
        .handle(verifyParamsWithSchema()) // Verify output of act-fn matches wf param schema
        .onErrorContinue(Errors.handle())
        .doOnNext(tx -> log.info("Activation Result: {}", tx.get()))
        .handle(handleInputToRunRequest())
        .onErrorContinue(Errors.handle())
        .doOnNext(tx -> log.info("Run request created: {}", tx.get()));
  }

  private BiConsumer<Transaction<Map<String, Object>>, SynchronousSink<Transaction<RunRequest>>>
      handleInputToRunRequest() {
    return (tx, sink) -> sink.next(tx.map(node.inputToRunRequest().apply(tx.get())));
  }

  /**
   * Helper that returns a function that will use the schema in the workflow configuration to
   * validate incoming inputs to the workflow. It does not actually transform the input, it emits
   * the same Map it received but it does attempt to construct an Avro GenericData.Record for
   * validation.
   *
   * @return BiConsumer that takes the transaction and synchronous sink of the Flux/Mono.
   */
  private BiConsumer<
          Transaction<Map<String, Object>>, SynchronousSink<Transaction<Map<String, Object>>>>
      verifyParamsWithSchema() {
    return (tx, sink) -> {
      val newRecord = reactiveRabbit.schemaManager().newRecord(schemaFullName);
      try {
        tx.get().forEach(newRecord::put);
      } catch (AvroRuntimeException e) {
        log.error("Provided input parameters do not match parameter schema specified by workflow");
        sink.error(new DeadLetterQueueableException(e));
      }
      sink.next(tx);
    };
  }

  private void logFilterMessage(String preText, Transaction tx, NodeProperties.Filter filter) {
    log.info(
        "{} for value: {}, with the following expression: {}",
        preText,
        tx.get(),
        filter.getExpression());
  }
}
