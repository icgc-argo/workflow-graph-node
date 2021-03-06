package org.icgc_argo.workflowgraphnode.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import lombok.SneakyThrows;

public final class JacksonUtils {
  private static final ObjectMapper mapper = new ObjectMapper();

  /** Util class, doesn't need to be instantiated. */
  private JacksonUtils() {}

  @SneakyThrows
  public static <T> T readValue(InputStream src, Class<T> valueType) {
    return mapper.readValue(src, valueType);
  }
}
