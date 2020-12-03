package org.icgc_argo.workflowgraphnode.logging;

import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

/**
 * log4j2.xml configures Kafka Audit Appender to only format messages with this marker before
 * forwarding to Kafka
 */
public class GraphLogMarker {
  public static Marker getMarker() {
    return MarkerFactory.getMarker("GraphLog");
  }
}
