package org.icgc_argo.workflowgraphnode.components.exceptions;

public class WorkflowParamsFunctionException extends RuntimeException {
  public WorkflowParamsFunctionException(String exception) {
    super(exception);
  }

  public WorkflowParamsFunctionException() {
    super();
  }
}
