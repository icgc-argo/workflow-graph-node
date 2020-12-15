package org.icgc_argo.workflowgraphnode.rabbitmq;

import static java.lang.String.format;

import com.pivotal.rabbitmq.RabbitEndpointService;
import com.pivotal.rabbitmq.ReactiveRabbit;
import com.pivotal.rabbitmq.source.Source;
import com.pivotal.rabbitmq.stream.Transaction;
import java.time.Duration;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import lombok.val;
import org.apache.avro.AvroRuntimeException;
import org.icgc_argo.workflow_graph_lib.exceptions.DeadLetterQueueableException;
import org.icgc_argo.workflow_graph_lib.schema.GraphEvent;
import org.icgc_argo.workflow_graph_lib.schema.GraphRun;
import org.icgc_argo.workflow_graph_lib.workflow.client.RdpcClient;
import org.icgc_argo.workflow_graph_lib.workflow.model.RunRequest;
import org.icgc_argo.workflowgraphnode.components.Errors;
import org.icgc_argo.workflowgraphnode.components.Input;
import org.icgc_argo.workflowgraphnode.components.Node;
import org.icgc_argo.workflowgraphnode.components.Workflows;
import org.icgc_argo.workflowgraphnode.config.AppConfig;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import org.icgc_argo.workflowgraphnode.logging.GraphLogger;
import org.icgc_argo.workflowgraphnode.service.GraphTransitAuthority;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;

@Configuration
public class NodeConfiguration {
  private final RdpcClient rdpcClient;
  private final RabbitEndpointService rabbit;
  private final ReactiveRabbit reactiveRabbit;
  private final TopologyConfiguration topologyConfig;
  private final NodeProperties nodeProperties;
  private final Source<Map<String, Object>> directInputSource;
  private final String schemaFullName;
  private final GraphTransitAuthority graphTransitAuthority;

  @Getter(lazy = true)
  private final Function<Flux<Transaction<GraphEvent>>, Flux<Transaction<GraphEvent>>>
      filterTransformer = Node.createFilterTransformer(nodeProperties);

  @Getter(lazy = true)
  private final Function<Flux<Transaction<GraphEvent>>, Flux<Transaction<Map<String, Object>>>>
      gqlQueryTransformer =
          Node.createGqlQueryTransformer(rdpcClient, nodeProperties.getGqlQueryString());

  @Getter(lazy = true)
  private final Function<
          Flux<Transaction<Map<String, Object>>>, Flux<Transaction<Map<String, Object>>>>
      activationFunctionTransformer = Node.createActivationFunctionTransformer(nodeProperties);

  @Getter(lazy = true)
  private final Function<Flux<Transaction<Map<String, Object>>>, Flux<Transaction<RunRequest>>>
      inputToRunRequestHandler = Input.createInputToRunRequestHandler(nodeProperties.getWorkflow());

  @Autowired
  public NodeConfiguration(
      @NonNull RdpcClient rdpcClient,
      @NonNull RabbitEndpointService rabbit,
      @NonNull ReactiveRabbit reactiveRabbit,
      @NonNull TopologyConfiguration topologyConfig,
      @NonNull AppConfig appConfig,
      @NonNull Source<Map<String, Object>> directInputSource,
      @NonNull GraphTransitAuthority graphTransitAuthority) {
    this.rdpcClient = rdpcClient;
    this.rabbit = rabbit;
    this.reactiveRabbit = reactiveRabbit;
    this.topologyConfig = topologyConfig;
    this.nodeProperties = appConfig.getNodeProperties();
    this.directInputSource = directInputSource;
    this.graphTransitAuthority = graphTransitAuthority;
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
        .doOnNext(tx -> GraphLogger.info(tx, "Completed: %s", tx.get()))
        .subscribe(GraphTransitAuthority::commitAndRemoveTransactionFromGTA);
  }

  public Disposable inputToRunning() {
    return rabbit
        .declareTopology(topologyConfig.runningTopology())
        .createTransactionalProducerStream(GraphRun.class)
        .route()
        .toExchange(nodeProperties.getRunning().getExchange())
        .and()
        .whenNackByBroker()
        .alwaysRetry(Duration.ofSeconds(5))
        .then()
        .send(mergedInputStreams())
        .doOnNext(
            tx ->
                GraphLogger.info(
                    tx, "Run request confirmed by RDPC, runId: %s", tx.get().getRunId()))
        .subscribe(GraphTransitAuthority::commitAndRemoveTransactionFromGTA);
  }

  private Flux<Transaction<GraphEvent>> runningToCompleteStream() {
    return rabbit
        .declareTopology(topologyConfig.runningTopology())
        .createTransactionalConsumerStream(nodeProperties.getRunning().getQueue(), GraphRun.class)
        .receive()
        .doOnNext(graphTransitAuthority::registerGraphRunTx)
        .delayElements(Duration.ofSeconds(10))
        .doOnNext(tx -> GraphLogger.debug(tx, "Checking status of: %s", tx.get().getRunId()))
        .handle(Workflows.handleRunStatus(rdpcClient))
        .onErrorContinue(Errors.handle())
        .flatMap(Workflows.runAnalysesToGraphEvent(rdpcClient));
  }

  private Flux<Transaction<GraphRun>> mergedInputStreams() {
    return Flux.merge(directInputStream(), queuedInputStream())
        .doOnNext(tx -> GraphLogger.info(tx, "Attempting to run workflow with: %s", tx.get()))
        .flatMap(Workflows.startRuns(rdpcClient))
        .onErrorContinue(Errors.handle());
  }

  private Flux<Transaction<RunRequest>> directInputStream() {
    return directInputSource
        .source()
        .handle(verifyParamsWithSchema()) // verify manually provided input matches schema
        .onErrorContinue(Errors.handle())
        .transform(getInputToRunRequestHandler())
        .doOnNext(tx -> GraphLogger.info(tx, "Run request created: %s", tx.get()));
  }

  private Flux<Transaction<RunRequest>> queuedInputStream() {
    // declare and merge all input queues provided in config
    return Flux.merge(
            topologyConfig
                .inputPropertiesAndTopologies()
                .map(
                    input ->
                        rabbit
                            .declareTopology(input.getTopologyBuilder())
                            .createTransactionalConsumerStream(
                                input.getProperties().getQueue(), GraphEvent.class)
                            .receive()
                            .doOnNext(graphTransitAuthority::registerGraphEventTx))
                .collect(Collectors.toList()))
        .transform(getFilterTransformer())
        .transform(getGqlQueryTransformer())
        .transform(getActivationFunctionTransformer())
        .handle(verifyParamsWithSchema()) // Verify output of act-fn matches wf param schema
        .onErrorContinue(Errors.handle())
        .transform(getInputToRunRequestHandler())
        .doOnNext(tx -> GraphLogger.info(tx, "Run request created: %s", tx.get()));
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
        sink.next(tx);
      } catch (AvroRuntimeException e) {
        GraphLogger.error(
            tx, "Provided input parameters do not match parameter schema specified by workflow");
        sink.error(new DeadLetterQueueableException(e));
      } catch (Exception e) {
        sink.error(e);
      }
    };
  }
}
