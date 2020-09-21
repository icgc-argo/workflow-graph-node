package org.icgc_argo.workflowgraphnode.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileInputStream;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AppConfig {
  @Getter private final NodeProperties nodeProperties;

  @Value("${rdpc.url}")
  @Getter
  private String rdpcUrl;

  @SneakyThrows
  public AppConfig(@Value("${node.jsonConfigPath}") String jsonConfigPath) {
    val inputStream = new FileInputStream(jsonConfigPath);
    this.nodeProperties = new ObjectMapper().readValue(inputStream, NodeProperties.class);
  }
}
