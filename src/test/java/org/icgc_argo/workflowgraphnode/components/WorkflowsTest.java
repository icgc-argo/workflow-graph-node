package org.icgc_argo.workflowgraphnode.components;

import static org.icgc_argo.workflowgraphnode.components.CommonFunctions.readValue;

import lombok.SneakyThrows;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles("test")
@SpringBootTest
public class WorkflowsTest {
  private final NodeProperties config;

  @SneakyThrows
  public WorkflowsTest() {
    config =
        readValue(
            this.getClass().getResourceAsStream("fixtures/config.json"), NodeProperties.class);
  }
}
