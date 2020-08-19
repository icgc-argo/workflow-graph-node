package org.icgc_argo.workflowgraphnode.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pivotal.rabbitmq.RabbitEndpointService;
import com.pivotal.rabbitmq.ReactiveRabbit;
import com.pivotal.rabbitmq.source.Source;
import com.pivotal.rabbitmq.stream.Transaction;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc_argo.workflowgraphnode.config.TopologyConfig;
import org.icgc_argo.workflowgraphnode.model.PipeStatus;
import org.icgc_argo.workflowgraphnode.model.RunRequest;
import org.icgc_argo.workflowgraphnode.workflow.RdpcClient;
import org.icgc_argo.workflowgraphnode.workflow.WesClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Slf4j
@Configuration
public class NodeService {

  /** Constants */
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String INGEST = "httpIngest";
  private static final String QUEUED_TO_RUNNING = "queuedToRunning";
  private static final String RUNNING_TO_COMPLETE = "runningToComplete";

  /** State */
  private final Scheduler scheduler = Schedulers.newElastic("wes-scheduler");

  private final Map<String, Disposable> pipelines = Collections.synchronizedMap(new HashMap<>());

  /** Dependencies */
  private final RabbitEndpointService rabbit;

  private final RdpcClient rdpcClient;
  private final WesClient wesClient;
  private final TopologyConfig topologyConfig;
  private final Source<String> runRequestSource;

  @Autowired
  public NodeService(
      @NonNull RabbitEndpointService rabbit,
      @NonNull RdpcClient rdpcClient,
      @NonNull WesClient wesClient,
      @NonNull TopologyConfig topologyConfig,
      @NonNull Source<String> runRequestSource) {
    this.rabbit = rabbit;
    this.rdpcClient = rdpcClient;
    this.wesClient = wesClient;
    this.topologyConfig = topologyConfig;
    this.runRequestSource = runRequestSource;

    startIngest();
    startQueued();
    startRunning();
  }

  public Map<String, PipeStatus> getStatus() {
    return this.pipelines.keySet().stream()
        .map(disposable -> Map.entry(disposable, checkPipe(disposable)))
        .collect(Collectors.toUnmodifiableMap(Entry::getKey, Entry::getValue));
  }

  public void stopIngest() {
    pipelines.get(INGEST).dispose();
  }

  public void stopQueued() {
    pipelines.get(QUEUED_TO_RUNNING).dispose();
  }

  public void stopRunning() {
    pipelines.get(RUNNING_TO_COMPLETE).dispose();
  }

  public void startIngest() {
    startPipe(INGEST, () -> ingestHttpJobs(this.runRequestSource));
  }

  public void startQueued() {
    startPipe(QUEUED_TO_RUNNING, this::queuedToRunning);
  }

  public void startRunning() {
    startPipe(RUNNING_TO_COMPLETE, this::runningToCompleteOrError);
  }

  /**
   * Starts a pipe with the given name and callable pipeline builder. Built pipe is stored as part
   * of the service's state in the form of a synchronized hashmap.
   *
   * @param name Name of the pipe
   * @param pipeBuilder Callable that will be invoked as a pipeline builder
   */
  @SneakyThrows
  private void startPipe(String name, Callable<Disposable> pipeBuilder) {
    val pipe = this.pipelines.get(name);
    if (pipe == null || pipe.isDisposed()) {
      this.pipelines.put(name, pipeBuilder.call());
    } else {
      log.error("Error trying to start {} pipelines.", name);
      throw new IllegalStateException("Cannot start pipeline as one already exists.");
    }
  }

  /**
   * Check the status of the disposable pipe
   *
   * @param name Name of the pipe
   * @return Returns enabled if Disposable is still subscribed, otherwise returns disabled if has
   *     been disposed.
   */
  private PipeStatus checkPipe(String name) {
    val pipe = this.pipelines.get(name);
    if (pipe == null || pipe.isDisposed()) {
      return PipeStatus.DISABLED;
    } else {
      return PipeStatus.ENABLED;
    }
  }

  /**
   * The job of this pipe is to take the incoming messages from the Node's HTTP ingest endpoint and
   * to place them on the run queue.
   *
   * @param runRequestSource The on demand source to send messages to.
   * @return Returns the pipe in the form of a Disposable from the subscribed flux.
   */
  private Disposable ingestHttpJobs(Source<String> runRequestSource) {
    return rabbit
        .declareTopology(topologyConfig.queueTopology())
        .createTransactionalProducerStream(String.class)
        .route()
        .toExchange(topologyConfig.getProperties().getInput().getExchange())
        .then()
        .send(runRequestSource.source().doOnNext(i -> log.info("Trying to send: {}", i.get())))
        .doOnError(throwable -> log.info(throwable.getLocalizedMessage()))
        .transform(ReactiveRabbit.commitElseTerminate())
        .doOnError(throwable -> log.info(throwable.getLocalizedMessage()))
        .subscribe();
  }

  /**
   * This pipe takes the queued messages from the run queue, attempts to run them as new workflow
   * runs, and then places the runId of the running workflow onto the running message queue.
   *
   * @return Returns the pipe in the form of a Disposable from the subscribed flux.
   */
  private Disposable queuedToRunning() {
    final Flux<Transaction<RunRequest>> incomingStream =
        rabbit
            .declareTopology(topologyConfig.queueTopology())
            .createTransactionalConsumerStream(
                topologyConfig.getProperties().getInput().getQueue(), String.class)
            .receive()
            .flatMap(
                tx ->
                    Mono.fromCallable(() -> tx.map(MAPPER.readValue(tx.get(), RunRequest.class))));

    // TODO: Handle errors from the workflow API
    final Flux<Transaction<String>> launchedWorkflowStream =
        incomingStream
            .doOnNext(item -> log.info("Attempting to run workflow with: {}", item.get()))
            .flatMap(
                tx ->
                    Mono.fromCallable(
                        () -> tx.map(wesClient.launchWorkflowWithWes(tx.get()).block())));

    return rabbit
        .declareTopology(topologyConfig.runningTopology())
        .createTransactionalProducerStream(String.class)
        .route()
        .toExchange(topologyConfig.getProperties().getRunning().getExchange())
        .then()
        .send(launchedWorkflowStream)
        .subscribeOn(scheduler)
        .subscribe(Transaction::commit);
  }

  /**
   * This pipe checks the running jobs on the running message queue by querying the rdpc gateway to
   * see if they have completed or errored out. On complete they are sent to the complete exchange
   * and the transaction is commited, otherwise the transaction is rejected.
   *
   * @return
   */
  private Disposable runningToCompleteOrError() {
    final Flux<Transaction<String>> completedWorkflows =
        rabbit
            .declareTopology(topologyConfig.runningTopology())
            .createTransactionalConsumerStream(
                topologyConfig.getProperties().getRunning().getQueue(), String.class)
            .receive()
            .delayElements(Duration.ofSeconds(10))
            .doOnNext(r -> log.info("Checking status of: {}", r.get()))
            .filterWhen(tx -> rdpcClient.getWorkflowStatus(tx.get()).map("COMPLETE"::equals))
            .doOnDiscard(Transaction.class, tx -> tx.rollback(true));

    // TODO: Handle executor error events, will probably require a dlq

    return rabbit
        .declareTopology(topologyConfig.completeTopology())
        .createTransactionalProducerStream(String.class)
        .route()
        .toExchange(topologyConfig.getProperties().getComplete().getExchange())
        .and()
        .whenNackByBroker()
        .alwaysRetry(Duration.ofSeconds(5))
        .then()
        .send(completedWorkflows)
        .doOnNext(tx -> log.info("Completed: {}", tx.get()))
        .subscribe(Transaction::commit);
  }
}
