package org.icgc_argo.workflowgraphnode.utils;

import lombok.val;
import org.apache.avro.generic.GenericData;
import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class RecordToFlattenedMap {
  public static Map<String, Object> from(GenericData.Record record) {
    return Flux.fromIterable(record.getSchema().getFields())
        .map(field -> Map.entry(field.name(), record.get(field.name())))
        .flatMapSequential(RecordToFlattenedMap::flatten)
        .toStream()
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static Flux<Map.Entry<String, ?>> flatten(Map.Entry<String, ?> entry) {
    if (entry.getValue() instanceof GenericData.Record) {
      val nestedRecord = (GenericData.Record) entry.getValue();
      return Flux.fromIterable(nestedRecord.getSchema().getFields())
          .map(
              field ->
                  Map.entry(
                      format("%s_%s", entry.getKey(), field.name()),
                      nestedRecord.get(field.name())))
          .flatMapSequential(RecordToFlattenedMap::flatten);
    } else if (entry.getValue() instanceof GenericData.Array) {
      return Flux.fromIterable((GenericData.Array<?>) entry.getValue())
          .index()
          .map(
              valueIdxTuple ->
                  Map.entry(
                      format("%s_%s", entry.getKey(), valueIdxTuple.getT1()),
                      valueIdxTuple.getT2()))
          .flatMapSequential(RecordToFlattenedMap::flatten);
    }
    return Flux.just(Map.entry(entry.getKey(), entry.getValue().toString()));
  }
}
