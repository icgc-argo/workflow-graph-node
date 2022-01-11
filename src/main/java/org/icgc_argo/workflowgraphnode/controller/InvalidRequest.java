package org.icgc_argo.workflowgraphnode.controller;

import java.util.Map;
import lombok.*;
import org.springframework.http.HttpStatus;

@Getter
@RequiredArgsConstructor
@AllArgsConstructor
public class InvalidRequest extends Throwable {
  final String message;
  Map<String, Object> errorInfo;

  public HttpStatus getStatusCode() {
    return HttpStatus.BAD_REQUEST;
  }
}
