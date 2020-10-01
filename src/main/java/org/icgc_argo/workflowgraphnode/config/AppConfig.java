package org.icgc_argo.workflowgraphnode.config;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pivotal.rabbitmq.ReactiveRabbit;
import com.pivotal.rabbitmq.schema.MissingAvroSchemaException;
import java.io.FileInputStream;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc_argo.workflow_graph_lib.workflow.client.RdpcClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Slf4j
@Configuration
public class AppConfig {
  @Getter private final NodeProperties nodeProperties;
  private final ReactiveRabbit reactiveRabbit;

  @Value("${rdpc.url}")
  @Getter
  private String rdpcUrl;

  @SneakyThrows
  public AppConfig(
      @Value("${node.jsonConfigPath}") String jsonConfigPath,
      @Autowired ReactiveRabbit reactiveRabbit,
      @Autowired Environment environment) {
    val inputStream = new FileInputStream(jsonConfigPath);
    this.nodeProperties = new ObjectMapper().readValue(inputStream, NodeProperties.class);
    this.reactiveRabbit = reactiveRabbit;

    if (!asList(environment.getActiveProfiles()).contains("test")) {
      loadWorkflowSchema();
    } else {
      log.info("Running with test profile enabled, will not load workflow schema.");
    }
  }

  @SneakyThrows
  private void loadWorkflowSchema() {
    val schemaName = this.nodeProperties.getWorkflow().getSchemaName();
    val schemaVersion = this.nodeProperties.getWorkflow().getSchemaVersion();
    val schemaManager = this.reactiveRabbit.schemaManager();

    val schemaObj =
        schemaManager.fetchReadSchemaByContentType(
            format("application/vnd.%s.v%s+avro", schemaName, schemaVersion));
    if (schemaObj.isError()) {
      throw new MissingAvroSchemaException(
          "Cannot load required avro schema for workflow parameters.");
    } else {
      log.info(
          "Successfully loaded schema {} with version {} from schema registry.",
          schemaName,
          schemaVersion);
      log.info("\n\033[32m" + schemaObj.toString(true) + "\033[39m");
    }
  }

  @Bean
  public RdpcClient createRdpcClient() {
    return new RdpcClient(rdpcUrl);
  }
}
