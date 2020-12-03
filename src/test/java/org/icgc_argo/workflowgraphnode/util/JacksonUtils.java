package org.icgc_argo.workflowgraphnode.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;

import java.io.InputStream;

public final class JacksonUtils {
  private static final ObjectMapper mapper = new ObjectMapper();

  /** Util class, doesn't need to be instantiated. */
  private JacksonUtils() {}

  @SneakyThrows
  public static <T> T readValue(InputStream src, Class<T> valueType) {
    return mapper.readValue(src, valueType);
  }
}
