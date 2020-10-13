package org.icgc_argo.workflowgraphnode.components;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pivotal.rabbitmq.stream.Transaction;
import com.pivotal.rabbitmq.stream.TransactionManager;
import java.io.InputStream;
import lombok.SneakyThrows;

public final class CommonFunctions {
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final TransactionManager<Object, Transaction<Object>> tm =
      new TransactionManager<>("componentTest");

  /** Util class, doesn't need to be instantiated. */
  private CommonFunctions() {}

  @SneakyThrows
  public static <T> T readValue(InputStream src, Class<T> valueType) {
    return mapper.readValue(src, valueType);
  }

  public static <T> Transaction<T> convertToTransaction(T obj) {
    return tm.newTransaction(null) // start with dummy transaction
        .map(obj);
  }
}
