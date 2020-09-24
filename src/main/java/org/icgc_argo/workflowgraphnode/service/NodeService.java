package org.icgc_argo.workflowgraphnode.service;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
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
public class NodeService {

  private static final String NODE = "nodeStream";

  private final Map<String, Disposable> pipelines = Collections.synchronizedMap(new HashMap<>());

  /** Dependencies */
  private final NodeConfiguration nodeConfiguration;

  @Autowired
  public NodeService(@NonNull NodeConfiguration nodeConfiguration) {
    this.nodeConfiguration = nodeConfiguration;

    startNode();
  }

  public Map<String, PipeStatus> getStatus() {
    return this.pipelines.keySet().stream()
        .map(disposable -> Map.entry(disposable, checkPipe(disposable)))
        .collect(Collectors.toUnmodifiableMap(Entry::getKey, Entry::getValue));
  }

  public void stopNode() {
    pipelines.get(NODE).dispose();
  }

  public void startNode() {
    startPipe(NODE, nodeConfiguration::nodeStream);
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
}
