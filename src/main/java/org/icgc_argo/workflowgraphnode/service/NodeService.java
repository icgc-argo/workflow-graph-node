package org.icgc_argo.workflowgraphnode.service;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pivotal.rabbitmq.RabbitEndpointService;
import com.pivotal.rabbitmq.ReactiveRabbit;
import com.pivotal.rabbitmq.source.OnDemandSource;
import com.pivotal.rabbitmq.source.Sender;
import com.pivotal.rabbitmq.source.Source;
import com.pivotal.rabbitmq.stream.Transaction;
import com.pivotal.rabbitmq.topology.ExchangeType;
import com.pivotal.rabbitmq.topology.TopologyBuilder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

@Slf4j
@Configuration
public class NodeService {

  /** Constants */
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** State */
  @Value("${workflow.url}")
  private String workflowUrl;

  @Value("${workflow.service.execution.url}")
  private String executionServiceUrl;

  private final Scheduler scheduler = Schedulers.newElastic("wes-scheduler");

  /** Dependencies */
  private final RabbitEndpointService rabbit;

  public NodeService(RabbitEndpointService rabbit) {
    this.rabbit = rabbit;
  }

  @Bean
  public Disposable.Composite pipes(Source<String> runRequestSource) {
    Disposable.Composite pipelines = Disposables.composite();
    pipelines.add(ingestHttpJobs(runRequestSource));
    pipelines.add(runQueuedJobs());
    return pipelines;
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

  private Disposable ingestHttpJobs(Source<String> runRequestSource) {
    return rabbit
        .declareTopology(queueTopology())
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
    final Flux<Transaction<RunRequest>> incomingStream = rabbit
        .declareTopology(queueTopology())
        .createTransactionalConsumerStream("run-queue", String.class)
        .receive()
        .doOnNext(item -> log.info("Consumed: {}", item.get()))
        .map(
            tx -> {
              try {
                return tx.map(MAPPER.readValue(tx.get(), RunRequest.class));
              } catch (IOException e) {
                log.warn("Could not parse queued job event.");
                throw new RuntimeException(e);
              }
            });

    final Flux<Transaction<String>> launchedWorkflowStream = incomingStream
      .map(tx -> tx.map(launchWorkflowWithWes(tx.get()).block()));

    return rabbit.declareTopology(runningTopology())
      .createTransactionalProducerStream(String.class)
      .route()
        .toExchange("node-state")
      .then()
      .send(launchedWorkflowStream)
      .doOnNext(stringTransaction -> log.info("Sent: {}", stringTransaction.get()))
      .subscribeOn(scheduler)
      .subscribe(Transaction::commit);
  }

  private Consumer<TopologyBuilder> queueTopology() {
    return topologyBuilder ->
        topologyBuilder
            .declareExchange("node-input")
            .type(ExchangeType.direct)
            .and()
            .declareQueue("run-queue")
            .boundTo("node-input");
  }

  private Consumer<TopologyBuilder> runningTopology() {
    return topologyBuilder ->
      topologyBuilder
        .declareExchange("node-state")
        .type(ExchangeType.direct)
        .and()
        .declareQueue("running")
        .boundTo("node-state");
  }

  private Mono<String> launchWorkflowWithWes(RunRequest runRequest) {
    runRequest.setWorkflowUrl(workflowUrl);
    return WebClient.create()
        .post()
        .uri(executionServiceUrl)
        .contentType(MediaType.APPLICATION_JSON)
        .body(BodyInserters.fromValue(runRequest))
        .retrieve()
        .bodyToMono(Map.class)
        .map(map -> map.get("run_id").toString());
  }

  @Data
  public static class RunRequest {
    @JsonProperty(value = "workflow_url")
    String workflowUrl;

    @JsonProperty(value = "workflow_params")
    Map<String, Object> workflowParams;
  }
}
