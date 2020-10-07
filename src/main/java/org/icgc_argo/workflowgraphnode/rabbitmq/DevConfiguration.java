package org.icgc_argo.workflowgraphnode.rabbitmq;

import com.pivotal.rabbitmq.RabbitEndpointService;
import com.pivotal.rabbitmq.topology.ExchangeType;
import java.util.Collections;
import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc_argo.workflow_graph_lib.schema.AnalysisFile;
import org.icgc_argo.workflow_graph_lib.schema.GraphEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

@Slf4j
@Configuration
@Profile("dev")
public class DevConfiguration {
  private final RabbitEndpointService rabbit;
  private final String exchange;
  private final String analysisId;
  private final String analysisState;
  private final String analysisType;
  private final String studyId;
  private final String experimentalStrategy;

  @Autowired
  public DevConfiguration(
      @NonNull RabbitEndpointService rabbit,
      @Value("${dev.exchange}") String exchange,
      @Value("${dev.event.analysisId}") String analysisId,
      @Value("${dev.event.analysisState}") String analysisState,
      @Value("${dev.event.analysisType}") String analysisType,
      @Value("${dev.event.studyId}") String studyId,
      @Value("${dev.event.experimentalStrategy}") String experimentalStrategy) {
    this.rabbit = rabbit;
    this.exchange = exchange;
    this.analysisId = analysisId;
    this.analysisState = analysisState;
    this.analysisType = analysisType;
    this.studyId = studyId;
    this.experimentalStrategy = experimentalStrategy;
  }

  @Bean
  public Disposable sendWorkflowEvents() {
    val demoEvent =
        new GraphEvent(
            analysisId,
            analysisState,
            analysisType,
            studyId,
            experimentalStrategy,
            List.of("DO123"),
            List.of(new AnalysisFile("SNV")));

    val demoStream =  Flux.fromIterable(List.of(demoEvent));

    return rabbit
        .declareTopology(
            topologyBuilder -> topologyBuilder.declareExchange(exchange).type(ExchangeType.fanout))
        .createProducerStream(GraphEvent.class)
        .route()
        .toExchange(exchange)
        .then()
        .send(demoStream)
        .doOnNext(number -> log.info("Demo event sent: {}", number))
        .subscribe();
  }
}
