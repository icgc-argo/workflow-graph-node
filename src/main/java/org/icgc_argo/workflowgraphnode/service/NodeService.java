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
import java.util.concurrent.Callable;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc_argo.workflowgraphnode.config.TopologyConfig;
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

  private static final String INGEST = "ingest";
  private static final String QUEUED_TO_RUNNING = "runQueue";
  private static final String RUNNING_TO_COMPLETE = "checkRunning";

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
    startPipe(QUEUED_TO_RUNNING, this::runQueuedJobs);
  }

  public void startRunning() {
    startPipe(RUNNING_TO_COMPLETE, this::checkRunningJobs);
  }

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

  private Disposable ingestHttpJobs(Source<String> runRequestSource) {
    return rabbit
        .declareTopology(topologyConfig.queueTopology())
        .createTransactionalProducerStream(String.class)
        .route()
        .toExchange("node-input")
        .then()
        .send(runRequestSource.source().doOnNext(i -> log.info("Trying to send: {}", i.get())))
        .doOnError(throwable -> log.info(throwable.getLocalizedMessage()))
        .transform(ReactiveRabbit.commitElseTerminate())
        .doOnError(throwable -> log.info(throwable.getLocalizedMessage()))
        .subscribe();
  }

  private Disposable runQueuedJobs() {
    final Flux<Transaction<RunRequest>> incomingStream =
        rabbit
            .declareTopology(topologyConfig.queueTopology())
            .createTransactionalConsumerStream("run-queue", String.class)
            .receive()
            .doOnNext(item -> log.info("Consumed: {}", item.get()))
            .flatMap(
                tx ->
                    Mono.fromCallable(() -> tx.map(MAPPER.readValue(tx.get(), RunRequest.class))));

    final Flux<Transaction<String>> launchedWorkflowStream =
        incomingStream.flatMap(
            tx ->
                Mono.fromCallable(() -> tx.map(wesClient.launchWorkflowWithWes(tx.get()).block())));

    return rabbit
        .declareTopology(topologyConfig.runningTopology())
        .createTransactionalProducerStream(String.class)
        .route()
        .toExchange("node-state")
        .then()
        .send(launchedWorkflowStream)
        .subscribeOn(scheduler)
        .subscribe(Transaction::commit);
  }

  private Disposable checkRunningJobs() {
    final Flux<Transaction<String>> completedWorkflows =
        rabbit
            .declareTopology(topologyConfig.runningTopology())
            .createTransactionalConsumerStream("running", String.class)
            .receive()
            .delayElements(Duration.ofSeconds(10))
            .doOnNext(r -> log.info("Checking status of: {}", r.get()))
            .filterWhen(tx -> rdpcClient.getWorkflowStatus(tx.get()).map("COMPLETE"::equals))
            .doOnDiscard(Transaction.class, Transaction::reject);

    return rabbit
        .declareTopology(topologyConfig.completeTopology())
        .createTransactionalProducerStream(String.class)
        .route()
        .toExchange("node-complete")
        .then()
        .send(completedWorkflows)
        .doOnNext(tx -> log.info("Completed: {}", tx.get()))
        .subscribe(Transaction::commit);
  }
}
