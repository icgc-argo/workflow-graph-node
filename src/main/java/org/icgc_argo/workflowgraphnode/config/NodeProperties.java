package org.icgc_argo.workflowgraphnode.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.val;
import org.icgc_argo.workflow_graph_lib.polyglot.enums.GraphFunctionLanguage;

import java.io.FileInputStream;

@Data
public class NodeProperties {

  private GraphFunctionLanguage graphFunctionLanguage;
  private InputPipeProperties input;
  private PipeProperties running;
  private PipeProperties complete;

  @SneakyThrows
  public static NodeProperties nodePropertiesFromJSON(String fileLocation) {
    val inputStream = new FileInputStream(fileLocation);
    return new ObjectMapper().readValue(inputStream, NodeProperties.class);
  }

  @Data
  public static class PipeProperties {
    private String exchange;
    private String queue;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class InputPipeProperties extends PipeProperties {
    private String workflowUrl;
    private String workflowVersion;
    private String workflowParamsFunction;
  }
}
