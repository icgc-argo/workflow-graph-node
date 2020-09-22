package org.icgc_argo.workflowgraphnode.workflow;

import static java.lang.String.format;

import com.apollographql.apollo.ApolloCall;
import com.apollographql.apollo.ApolloClient;
import com.apollographql.apollo.api.Response;
import com.apollographql.apollo.exception.ApolloException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import okhttp3.OkHttpClient;
import org.icgc_argo.workflowgraphnode.config.AppConfig;
import org.icgc_argo.workflowgraphnode.graphql.client.GetWorkflowStateQuery;
import org.icgc_argo.workflowgraphnode.graphql.client.StartRunMutation;
import org.icgc_argo.workflowgraphnode.graphql.client.type.WorkflowEngineParams;
import org.icgc_argo.workflowgraphnode.model.RunRequest;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

@Slf4j
@Component
public class RdpcClient {

  /** State */
  private final ApolloClient client;

  @Autowired
  public RdpcClient(@NonNull AppConfig appConfig) {
    val okHttpBuilder = new OkHttpClient.Builder();
    okHttpBuilder.connectTimeout(60, TimeUnit.SECONDS);
    okHttpBuilder.callTimeout(60, TimeUnit.SECONDS);
    okHttpBuilder.readTimeout(60, TimeUnit.SECONDS);
    okHttpBuilder.writeTimeout(60, TimeUnit.SECONDS);

    this.client =
        ApolloClient.builder()
            .serverUrl(appConfig.getRdpcUrl())
            .okHttpClient(okHttpBuilder.build())
            .build();
  }

  /**
   * Start a new workflow run
   *
   * @param runRequest Run Request describing all information to run the workflow
   * @return Returns a Mono of the Workflow RunId
   */
  public Mono<String> startRun(RunRequest runRequest) {
    return Mono.create(
        sink -> {
          val mutationBuilder =
              StartRunMutation.builder()
                  .workflowUrl(runRequest.getWorkflowUrl())
                  .workflowParams(runRequest.getWorkflowParams());
          if (runRequest.getWorkflowEngineParams() != null) {
            mutationBuilder.workflowEngineParams(
                engineParamsAdapter(runRequest.getWorkflowEngineParams()));
          }

          client
              .mutate(mutationBuilder.build())
              .enqueue(
                  new ApolloCall.Callback<>() {
                    @Override
                    public void onResponse(
                        @NotNull Response<Optional<StartRunMutation.Data>> response) {
                      response
                          .getData()
                          .ifPresentOrElse(
                              data ->
                                  data.getStartRun()
                                      .ifPresentOrElse(
                                          startRun ->
                                              startRun
                                                  .getRunId()
                                                  .ifPresentOrElse(
                                                      sink::success,
                                                      () ->
                                                          sinkError(
                                                              sink, "No runId Found in response.")),
                                          () -> sinkError(sink, "Empty Response from API.")),
                              () -> sinkError(sink, "No Response from API."));
                    }

                    @Override
                    public void onFailure(@NotNull ApolloException e) {
                      sink.error(e);
                    }
                  });
        });
  }

  /**
   * Get the status of a workflow
   *
   * @param runId The runId of the workflow as a String
   * @return Returns a Mono of the state of the workflow
   */
  public Mono<String> getWorkflowStatus(String runId) {
    return Mono.create(
        sink ->
            client
                .query(new GetWorkflowStateQuery(runId))
                .enqueue(
                    new ApolloCall.Callback<>() {
                      @Override
                      public void onResponse(
                          @NotNull Response<Optional<GetWorkflowStateQuery.Data>> response) {
                        response
                            .getData()
                            .ifPresentOrElse(
                                data ->
                                    data.getRuns()
                                        .ifPresentOrElse(
                                            runs ->
                                                runs.stream()
                                                    .findFirst()
                                                    .ifPresentOrElse(
                                                        run ->
                                                            run.getState()
                                                                .ifPresentOrElse(
                                                                    sink::success,
                                                                    () ->
                                                                        sinkError(
                                                                            sink,
                                                                            format(
                                                                                "Missing state for run %s.",
                                                                                runId))),
                                                        () ->
                                                            sinkError(
                                                                sink,
                                                                format(
                                                                    "Run %s not found.", runId))),
                                            () ->
                                                sinkError(
                                                    sink, format("Run %s not found.", runId))),
                                () -> sinkError(sink, format("Run %s not found.", runId)));
                      }

                      @Override
                      public void onFailure(@NotNull ApolloException e) {
                        sink.error(e);
                      }
                    }));
  }

  private static void sinkError(MonoSink<?> sink, String message) {
    log.error(message);
    sink.error(new RuntimeException(message));
  }

  /**
   * Adapter to convert between Models of workflow engine params for use with apollo
   *
   * @param params WorkflowEngineParams model owned by developer
   * @return WorkflowEngineParams model owned by Apollo code gen
   */
  private static WorkflowEngineParams engineParamsAdapter(
      @NotNull org.icgc_argo.workflowgraphnode.model.WorkflowEngineParams params) {
    val builder = WorkflowEngineParams.builder();
    builder.revision(params.getRevision());
    builder.launchDir(params.getLaunchDir());
    builder.projectDir(params.getProjectDir());
    builder.workDir(params.getWorkDir());
    return builder.build();
  }
}
