package org.icgc_argo.workflowgraphnode.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import lombok.val;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class RecordToFlattenedMapTest {
  private static GenericData.Record testRecord;

  @BeforeAll
  public static void init() {
    try {
      val schema =
          Schema.parse(RecordToFlattenedMapTest.class.getResourceAsStream("TestRecord.avsc"));

      val workflowSchema = schema.getField("workflow").schema();
      val runSchema = workflowSchema.getField("run").schema();
      val donorSchema = schema.getField("donors").schema().getElementType();
      val specimenSchema = donorSchema.getField("specimens").schema().getElementType();

      val testStringArray = new GenericData.Array<>(2, schema.getField("someStringArray").schema());
      testStringArray.add("test-id-0");
      testStringArray.add("test-id-1");

      val testSpecimenOne = new GenericData.Array<>(2, donorSchema.getField("specimens").schema());
      testSpecimenOne.add(
          new GenericRecordBuilder(specimenSchema)
              .set("specimenId", "array-nested-donorId-0-specimenId-0")
              .build());
      testSpecimenOne.add(
          new GenericRecordBuilder(specimenSchema)
              .set("specimenId", "array-nested-donorId-0-specimenId-1")
              .build());

      val testSpecimenTwo = new GenericData.Array<>(2, donorSchema.getField("specimens").schema());
      testSpecimenTwo.add(
          new GenericRecordBuilder(specimenSchema)
              .set("specimenId", "array-nested-donorId-1-specimenId-0")
              .build());
      testSpecimenTwo.add(
          new GenericRecordBuilder(specimenSchema)
              .set("specimenId", "array-nested-donorId-1-specimenId-1")
              .build());

      val testDonors = new GenericData.Array<>(2, schema.getField("donors").schema());
      testDonors.add(
          new GenericRecordBuilder(donorSchema)
              .set("donorId", "array-nested-donorId-0")
              .set("specimens", testSpecimenOne)
              .build());
      testDonors.add(
          new GenericRecordBuilder(donorSchema)
              .set("donorId", "array-nested-donorId-1")
              .set("specimens", testSpecimenTwo)
              .build());

      testRecord =
          new GenericRecordBuilder(schema)
              .set("analysisId", "top-level-analysisId")
              .set("donorId", "top-level-donorId")
              .set("runId", "top-level-runId")
              .set("someStringArray", testStringArray)
              .set(
                  "workflow",
                  new GenericRecordBuilder(workflowSchema)
                      .set("runId", "nested-runId")
                      .set("state", "COMPLETE")
                      .set(
                          "run",
                          new GenericRecordBuilder(runSchema)
                              .set("runId", "double-nested-runId")
                              .set("state", "COMPLETE")
                              .build())
                      .build())
              .set("donors", testDonors)
              .build();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testMapsRecordToFlatMap() {
    val testMap = RecordToFlattenedMap.from(testRecord);

    // Uber test, if this flattens ... anything will flatten :grimacing:

    // top level fields
    assertEquals("top-level-analysisId", testMap.get("analysisId"));
    assertEquals("top-level-donorId", testMap.get("donorId"));
    assertEquals("top-level-runId", testMap.get("runId"));

    // simple array
    assertEquals("test-id-0", testMap.get("someStringArray_0"));
    assertEquals("test-id-1", testMap.get("someStringArray_1"));

    // object nested fields
    assertEquals("nested-runId", testMap.get("workflow_runId"));
    assertEquals("COMPLETE", testMap.get("workflow_state"));

    // object double nested
    assertEquals("double-nested-runId", testMap.get("workflow_run_runId"));
    assertEquals("COMPLETE", testMap.get("workflow_run_state"));

    // array nested fields
    assertEquals("array-nested-donorId-0", testMap.get("donors_0_donorId"));
    assertEquals("array-nested-donorId-1", testMap.get("donors_1_donorId"));

    // array double nested fields
    assertEquals(
        "array-nested-donorId-0-specimenId-0", testMap.get("donors_0_specimens_0_specimenId"));
    assertEquals(
        "array-nested-donorId-0-specimenId-1", testMap.get("donors_0_specimens_1_specimenId"));
    assertEquals(
        "array-nested-donorId-1-specimenId-0", testMap.get("donors_1_specimens_0_specimenId"));
    assertEquals(
        "array-nested-donorId-1-specimenId-1", testMap.get("donors_1_specimens_1_specimenId"));
  }
}
