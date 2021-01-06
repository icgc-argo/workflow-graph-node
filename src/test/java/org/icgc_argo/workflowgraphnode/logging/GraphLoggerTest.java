package org.icgc_argo.workflowgraphnode.logging;

import com.pivotal.rabbitmq.stream.Transaction;
import com.pivotal.rabbitmq.stream.TransactionManager;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles("test")
public class GraphLoggerTest {

  private final TransactionManager<String, Transaction<String>> tm =
      new TransactionManager<>("graphLoggerTest");

  @Test
  public void testUnregisteredLog() {
    val tx = tm.newTransaction("shouldLogWithoutNullPointer");
    GraphLogger.info(tx, "");
  }
}
