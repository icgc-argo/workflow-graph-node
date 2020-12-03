package org.icgc_argo.workflowgraphnode.service;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc_argo.workflowgraphnode.config.AppConfig;
import org.icgc_argo.workflowgraphnode.config.NodeProperties;
import org.icgc_argo.workflowgraphnode.logging.GraphLogger;
import org.icgc_argo.workflowgraphnode.model.PipeStatus;
import org.icgc_argo.workflowgraphnode.rabbitmq.NodeConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import reactor.core.Disposable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@Slf4j
@Configuration
public class PipelineManager {

  private static final String INPUT_TO_RUNNING = "inputToRunning";
  private static final String RUNNING_TO_COMPLETE = "runningToComplete";

  private final Map<String, Disposable> pipelines = Collections.synchronizedMap(new HashMap<>());

  /** Dependencies */
  private final NodeProperties nodeProperties;

  private final NodeConfiguration nodeConfiguration;

  @Autowired
  public PipelineManager(
      @NonNull AppConfig appConfig, @NonNull NodeConfiguration nodeConfiguration) {
    this.nodeProperties = appConfig.getNodeProperties();
    this.nodeConfiguration = nodeConfiguration;

    startInputToRunning();
    startRunningToComplete();
  }

  public Map<String, PipeStatus> getStatus() {
    return this.pipelines.keySet().stream()
        .map(disposable -> Map.entry(disposable, checkPipe(disposable)))
        .collect(Collectors.toUnmodifiableMap(Entry::getKey, Entry::getValue));
  }

  public void stopInputToRunning() {
    pipelines.get(INPUT_TO_RUNNING).dispose();
  }

  public void stopRunningToComplete() {
    pipelines.get(RUNNING_TO_COMPLETE).dispose();
  }

  public void startInputToRunning() {
    startPipe(INPUT_TO_RUNNING, nodeConfiguration::inputToRunning);
  }

  public void startRunningToComplete() {
    startPipe(RUNNING_TO_COMPLETE, nodeConfiguration::runningToComplete);
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
      GraphLogger.error(nodeProperties, "Error trying to start %s pipelines.", name);
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
}
