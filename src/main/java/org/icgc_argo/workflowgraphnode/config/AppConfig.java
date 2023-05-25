package org.icgc_argo.workflowgraphnode.config;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pivotal.rabbitmq.ReactiveRabbit;
import com.pivotal.rabbitmq.schema.MissingAvroSchemaException;
import com.pivotal.rabbitmq.schema.SchemaManager;
import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Method;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.avro.Schema;
import org.icgc_argo.workflow_graph_lib.schema.GraphEvent;
import org.icgc_argo.workflow_graph_lib.schema.GraphRun;
import org.icgc_argo.workflow_graph_lib.workflow.client.RdpcClient;
import org.icgc_argo.workflow_graph_lib.workflow.client.oauth.ClientCredentials;
import org.icgc_argo.workflowgraphnode.logging.GraphLogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;

@Configuration
public class AppConfig {
  private static final String GRAPH_EVENT_CONTENT_TYPE = "application/vnd.GraphEvent+avro";
  private static final String GRAPH_RUN_CONTENT_TYPE = "application/vnd.GraphRun+avro";

  @Getter private final NodeProperties nodeProperties;
  private final ReactiveRabbit reactiveRabbit;
  private final ApplicationContext context;

  @Value("${rdpc.url}")
  @Getter
  private String rdpcUrl;

  @SneakyThrows
  public AppConfig(
      @Value("${node.jsonConfigPath}") String jsonConfigPath,
      @Value("${node.localSchemaPath}") String localSchemaPath,
      @Autowired ReactiveRabbit reactiveRabbit,
      @Autowired Environment environment,
      @Autowired ApplicationContext context) {
    val inputStream = new FileInputStream(jsonConfigPath);
    this.nodeProperties = new ObjectMapper().readValue(inputStream, NodeProperties.class);
    this.reactiveRabbit = reactiveRabbit;
    this.context = context;

    val profiles = asList(environment.getActiveProfiles());
    if (profiles.contains("registry") && !profiles.contains("test")) {
      GraphLogger.info(nodeProperties, "Loading workflow schema from Registry.");
      loadWorkflowSchemaFromRegistry();
      ensureGraphSchemas();
    } else if (!profiles.contains("test")) {
      GraphLogger.info(nodeProperties, "Loading workflow schema from file system.");
      loadWorkflowSchemaFromFileSystem(localSchemaPath);
    } else {
      GraphLogger.info(
          nodeProperties, "Running with test profile enabled, will not load workflow schema.");
    }
  }

  @SneakyThrows
  private void loadWorkflowSchemaFromRegistry() {
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
      GraphLogger.info(
          nodeProperties,
          "Successfully loaded schema %s with version %s from schema registry.",
          schemaName,
          schemaVersion);
      GraphLogger.info(nodeProperties, "\n\033[32m" + schemaObj.toString(true) + "\033[39m");
    }
  }

  /**
   * Make sure that the Graph schemas are loaded from classpath into the content type storage of the
   * schema manager.
   */
  @SneakyThrows
  private void ensureGraphSchemas() {
    val schemaManager = this.reactiveRabbit.schemaManager();

    try {
      schemaManager.fetchReadSchemaByContentType(GRAPH_EVENT_CONTENT_TYPE);
    } catch (NullPointerException e) {
      classPathToContentTypeStorage(GRAPH_EVENT_CONTENT_TYPE, GraphEvent.SCHEMA$);
    }

    try {
      schemaManager.fetchReadSchemaByContentType(GRAPH_RUN_CONTENT_TYPE);
    } catch (NullPointerException e) {
      classPathToContentTypeStorage(GRAPH_RUN_CONTENT_TYPE, GraphRun.SCHEMA$);
    }
  }

  @SneakyThrows
  private void loadWorkflowSchemaFromFileSystem(String localSchemaPath) {
    val schemaFullName =
        format(
            "%s.%s",
            this.nodeProperties.getWorkflow().getSchemaNamespace(),
            this.nodeProperties.getWorkflow().getSchemaName());

    val schemaManager = this.reactiveRabbit.schemaManager();

    val parser = new Schema.Parser();
    val schema = parser.parse(new File(localSchemaPath));
    schemaManager.importSchema(schema);

    // Verify schema was loaded correctly and matches one specified in config.
    val storedSchema = schemaManager.fetchSchemaByFullName(schemaFullName);
    if (storedSchema == null || storedSchema.isError()) {
      GraphLogger.error(
          nodeProperties,
          "Cannot load required avro schema (%s) listed in workflow parameters from filesystem.",
          schemaFullName);
      throw new MissingAvroSchemaException(schemaFullName);
    }
  }

  @SneakyThrows
  private void classPathToContentTypeStorage(String contentType, Schema schema) {
    val schemaManager = this.reactiveRabbit.schemaManager();

    Method registerMethod =
        SchemaManager.class.getDeclaredMethod(
            "importRegisteredSchema", String.class, Schema.class, Integer.class);
    registerMethod.setAccessible(true);

    GraphLogger.info(
        nodeProperties,
        "Loading GraphRun AVRO Schema from classpath into registry with ContentType.");
    registerMethod.invoke(schemaManager, contentType, schema, null);

    val graphRunSchemaObj = schemaManager.fetchReadSchemaByContentType(contentType);
    if (graphRunSchemaObj.isError()) {
      GraphLogger.error(
          nodeProperties,
          "Cannot load %s schema by Content Type, shutting down.",
          schema.getFullName());
      SpringApplication.exit(context, () -> 1);
    } else {
      GraphLogger.info(
          nodeProperties,
          "Successfully loaded schema %s from classpath.",
          graphRunSchemaObj.getFullName());
      GraphLogger.info(
          nodeProperties, "\n\033[32m" + graphRunSchemaObj.toString(true) + "\033[39m");
    }
  }

  @Bean
  @Profile("!oauth")
  public RdpcClient createRdpcClient() {
    return new RdpcClient(rdpcUrl);
  }

  @Bean
  @Profile("oauth")
  public RdpcClient createRdpcClientWithAuth(ClientCredentials clientCredentials) {
    return new RdpcClient(rdpcUrl, 60L, clientCredentials);
  }

  @Bean
  public NodeProperties nodeProperties() {
    return this.nodeProperties;
  }
}
