/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// IMPORTANT: this file lives in `package org.apache.iceberg` ON PURPOSE — that is the only way to reach
// the package-private `@VisibleForTesting SchemaUpdate(Schema schema, int lastColumnId)` constructor that
// drives the UpdateSchema state machine without a live TableOperations / catalog, the package-private
// `BaseUpdatePartitionSpec` plumbing the partition-spec oracle uses, AND the package-private
// `new BaseSnapshot(...)` constructor + `TableMetadata.Builder.{addSnapshot,setBranchSnapshot,setRef}`
// that the manage-snapshots oracle uses to assemble a base with a real snapshot history. This class is a
// TEST-ONLY ORACLE (a dev tool, like dev/spark/); it is not part of the shipped Rust library.
package org.apache.iceberg;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;

/**
 * Java reference oracle for the UpdateSchema, UpdatePartitionSpec, AND ManageSnapshots interop suites.
 *
 * <p>Two modes, selected by the first program argument; each mode runs ALL THREE capabilities (schema +
 * partition + manage-snapshots) in one pass so a single {@code generate}/{@code verify} invocation covers
 * the whole surface:
 *
 * <ul>
 *   <li><b>generate</b> — for each named scenario, build a base {@link TableMetadata}, apply the
 *       scenario's op-sequence via the Java reference, and write {@code base.metadata.json} +
 *       {@code java_evolved.metadata.json} (via {@link TableMetadataParser#toJson}) into the scenario's
 *       testdata directory. Schema scenarios drive the package-private {@code @VisibleForTesting
 *       SchemaUpdate(Schema, int)} state machine; partition scenarios drive a real
 *       {@link BaseUpdatePartitionSpec} via {@link BaseTable#updateSpec()} over an in-memory
 *       {@link TableOperations} (so {@code base != null} and historical field-id recycling is exercised);
 *       manage-snapshots scenarios drive a real {@link SnapshotManager} via {@link
 *       BaseTable#manageSnapshots()} over a forked base that carries a real snapshot history + refs.
 *   <li><b>verify</b> — read {@code rust_evolved.metadata.json} from each scenario directory, assert Java
 *       parses it without error AND its current schema (schema scenarios), default partition spec
 *       (partition scenarios), or refs map + current-snapshot-id (manage-snapshots scenarios) is
 *       structurally equal to Java's own {@code java_evolved}. Prints PASS/FAIL per scenario; exits
 *       non-zero on any FAIL.
 * </ul>
 */
public final class InteropOracle {
  private static final String SCHEMA_LOCATION = "s3://interop-bucket/update_schema";
  private static final String PARTITION_LOCATION = "s3://interop-bucket/update_partition_spec";
  private static final String SNAPSHOT_LOCATION = "s3://interop-bucket/manage_snapshots";
  private static final String INSPECTION_LOCATION = "s3://interop-bucket/inspection";

  private InteropOracle() {}

  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("usage: InteropOracle <generate|verify>");
      System.exit(2);
      return;
    }
    Path schemaFixturesDir = requireFixturesDir("interop.fixtures.dir");
    Path partitionFixturesDir = requireFixturesDir("interop.partition.fixtures.dir");
    Path snapshotFixturesDir = requireFixturesDir("interop.manage_snapshots.fixtures.dir");

    String mode = args[0];
    switch (mode) {
      case "generate":
        SchemaOracle.generate(schemaFixturesDir);
        PartitionOracle.generate(partitionFixturesDir);
        SnapshotOracle.generate(snapshotFixturesDir);
        break;
      case "generate-inspection":
        // A SEPARATE exec mode (its own fixtures dir) so the inspection increment never touches the
        // committed update_schema / update_partition_spec / manage_snapshots fixtures. The dir is supplied
        // via -Dinterop.inspection.fixtures.dir on the CLI (exec:java runs in the same JVM, so
        // System.getProperty sees it).
        Path inspectionFixturesDir = requireFixturesDir("interop.inspection.fixtures.dir");
        InspectionOracle.generate(inspectionFixturesDir);
        break;
      case "generate-inspection-log":
        // A SEPARATE exec mode (its own fixtures dir) for the two remaining pure-metadata inspection
        // tables — `history` and `metadata_log_entries`. Like `generate-inspection`, the dir is supplied
        // via -Dinterop.inspection_log.fixtures.dir on the CLI (same JVM, so System.getProperty sees it),
        // so this increment never touches the committed `inspection/` fixtures.
        Path inspectionLogFixturesDir = requireFixturesDir("interop.inspection_log.fixtures.dir");
        InspectionLogOracle.generate(inspectionLogFixturesDir);
        break;
      case "generate-inspection-manifests":
        // A SEPARATE exec mode (its own temp dir) for the FIRST manifest-READING inspection increment —
        // the content-filtered `files` / `data_files` / `delete_files` tables. Unlike the pure-metadata
        // modes above, this one WRITES A REAL TABLE to local disk (real AVRO manifests + manifest-list +
        // metadata via org.apache.iceberg.Files.localOutput) so the Rust test reads the same on-disk
        // manifests Java's FilesTable read. The temp dir is supplied via
        // -Dinterop.inspection_manifests.dir on the CLI (same JVM, so System.getProperty sees it), so this
        // increment never touches any committed fixture.
        Path inspectionManifestsDir = requireFixturesDir("interop.inspection_manifests.dir");
        InspectionManifestsOracle.generate(inspectionManifestsDir);
        // A2 (the entries / manifests / partitions tables) reuses the SAME exec mode + temp dir: it writes
        // a SECOND, richer table to <dir>/table_a2 (the A1 table at <dir>/table is untouched) and emits
        // java_entries.json / java_manifests.json / java_partitions.json. Driven from here so a single
        // run-inspection-manifests.sh invocation produces both A1's and A2's fixtures in one JVM pass.
        InspectionManifestsA2Oracle.generate(inspectionManifestsDir);
        // A4 (SCAN PLANNING interop) reuses the SAME exec mode + temp dir: it writes a THIRD, dedicated
        // table to <dir>/table_a4 (the A1/A2 tables are untouched) and, for each named filter scenario,
        // emits java_scan_<name>.json — the SET of planned data-file paths, their applicable delete files,
        // and whether each task's residual is fully covered by partitioning — via Java's REAL
        // table.newScan().filter(expr).planFiles(). Driven from here so a single run produces A1/A2/A3/A4.
        InspectionScanA4Oracle.generate(inspectionManifestsDir);
        // READABLE_METRICS interop reuses the SAME exec mode + temp dir: it writes a FOURTH, dedicated
        // table to <dir>/table_rm (the A1/A2/A4 tables are untouched) and emits java_rm_files.json — the
        // `files` table rows INCLUDING the trailing virtual `readable_metrics` STRUCT (one per-leaf-column
        // struct of the six human-readable metrics), keyed by leaf column NAME then metric NAME. This is
        // the typed-decode of the same metric/bound maps A1 already round-trips; A1-A4 DEFER this column.
        // Driven from here so a single run-inspection-manifests.sh invocation produces A1/A2/A3/A4 AND RM.
        InspectionReadableMetricsRmOracle.generate(inspectionManifestsDir);
        break;
      case "generate-interop-scan-exec":
        // The FIRST DATA-LEVEL scan-execution interop increment. Unlike every mode above (which reads
        // MANIFESTS / pure metadata), this writes a REAL PARQUET DATA file + a REAL POSITION-DELETE file
        // via the generic parquet appender (iceberg-data's GenericAppenderFactory) and materializes Java's
        // OWN merge-on-read READ (IcebergGenerics) as the ground truth. The Rust test reads the SAME table
        // and asserts its scan→Arrow (with the position deletes applied) equals Java's live rows. The temp
        // dir is supplied via -Dinterop.scan_exec.dir on the CLI (same JVM, so System.getProperty sees it).
        Path scanExecDir = requireFixturesDir("interop.scan_exec.dir");
        ScanExecOracle.generate(scanExecDir);
        break;
      case "verify-interop-scan-exec":
        // DIRECTION 2 — "Java reads what RUST writes". The Rust GEN path (env
        // ICEBERG_INTEROP_SCAN_GEN_DIR) wrote a REAL on-disk table to <dir>/rust_table via its production
        // write path (MemoryCatalog over LocalFsStorageFactory: real parquet data + a real position-delete
        // written by PositionDeleteFileWriter, committed through fast_append + row_delta), landing a
        // final.metadata.json at a known path. Here Java loads that RUST-written metadata, builds a BaseTable
        // over a LocalFileIO (so io() reads the real on-disk parquet/avro), reads with IcebergGenerics (which
        // APPLIES Rust's position delete), and asserts the merge-on-read rows == {(10,a),(30,c),(50,e)}. A
        // failure here is a REAL write-incompatibility finding (Rust wrote something Java cannot read).
        Path scanExecVerifyDir = requireFixturesDir("interop.scan_exec.dir");
        int scanExecFailures = ScanExecOracle.verify(scanExecVerifyDir);
        System.out.println("verify-interop-scan-exec: " + scanExecFailures + " failures");
        if (scanExecFailures > 0) {
          System.exit(1);
        }
        break;
      case "generate-interop-part-scan":
        // PARTITIONED merge-on-read, DIRECTION 1 — "Rust reads what JAVA writes". The partition-handling
        // proof: unlike generate-interop-scan-exec (UNPARTITIONED), this writes a V2 table partitioned by
        // identity(category) with one REAL parquet DATA file PER PARTITION (category=a: ids 10/20/30;
        // category=b: ids 40/50), each DataFile stamped with its partition value (a PartitionData carrying
        // the category Struct, spec id 0) via GenericAppenderFactory.newDataWriter(..., partition). It then
        // writes a PARTITION-SCOPED position-delete in partition a (referencing the partition-a data file,
        // deleting position 1 = id=20) via newPosDeleteWriter(..., partitionA), committed at sequence 2 via
        // newRowDelta (the data is appended FIRST at sequence 1). Live merge-on-read rows = {10,30,40,50}
        // (only id=20 deleted; both partitions otherwise intact). It materializes Java's OWN read into
        // java_part_scan_rows.json and writes final.metadata.json. The dir is via -Dinterop.part_scan.dir.
        Path partScanDir = requireFixturesDir("interop.part_scan.dir");
        PartScanExecOracle.generate(partScanDir);
        break;
      case "verify-interop-part-scan":
        // PARTITIONED merge-on-read, DIRECTION 2 — "Java reads what RUST writes". The Rust GEN path (env
        // ICEBERG_INTEROP_PART_SCAN_GEN_DIR) wrote a REAL on-disk table partitioned by identity(category) to
        // <dir>/rust_table via its production write path (MemoryCatalog over LocalFsStorageFactory: one real
        // parquet DATA file per partition stamped with its partition Struct + spec id 0, fast_appended at
        // sequence 1, plus a partition-scoped position-delete in partition a written by
        // PositionDeleteFileWriter with the partition_key set, row_delta'd at sequence 2), landing a
        // final.metadata.json. Here Java loads that RUST-written metadata, reads with IcebergGenerics (which
        // APPLIES Rust's partition-scoped position delete), and asserts the merge-on-read rows ==
        // {(10,a),(30,a),(40,b),(50,b)} (id=20 deleted, both partitions otherwise intact). A failure here is
        // a REAL partition-aware write-incompatibility finding (Rust wrote a partitioned table / partition-
        // scoped delete Java cannot read).
        Path partScanVerifyDir = requireFixturesDir("interop.part_scan.dir");
        int partScanFailures = PartScanExecOracle.verify(partScanVerifyDir);
        System.out.println("verify-interop-part-scan: " + partScanFailures + " failures");
        if (partScanFailures > 0) {
          System.exit(1);
        }
        break;
      case "generate-interop-eq-delete":
        // EQUALITY-DELETE merge-on-read, DIRECTION 1 — "Rust reads what JAVA writes". The sibling of
        // generate-interop-scan-exec, but the merge-on-read mechanism is delete-by-VALUE (an equality
        // delete), not delete-by-position. The Java oracle writes a REAL parquet DATA file (5 rows,
        // appended at sequence 1) + a REAL parquet EQUALITY-delete file (keyed on field id 1 = `id`,
        // delete rows id=20 and id=40) via GenericAppenderFactory.newEqDeleteWriter, committed via
        // newRowDelta at sequence 2 (LATER than the data, so the eq-delete applies to the seq-1 data).
        // It materializes Java's OWN read into java_eq_scan_rows.json (= {10,30,50}) and writes
        // final.metadata.json for the Rust test. The dir is supplied via -Dinterop.eq_delete.dir.
        Path eqDeleteDir = requireFixturesDir("interop.eq_delete.dir");
        EqDeleteOracle.generate(eqDeleteDir);
        break;
      case "verify-interop-eq-delete":
        // EQUALITY-DELETE merge-on-read, DIRECTION 2 — "Java reads what RUST writes". The Rust GEN path
        // (env ICEBERG_INTEROP_EQ_SCAN_GEN_DIR) wrote a REAL on-disk table to <dir>/rust_table via its
        // production write path (MemoryCatalog over LocalFsStorageFactory: real parquet data fast_appended
        // at sequence 1 + a real EQUALITY-delete written by EqualityDeleteFileWriter committed via row_delta
        // at sequence 2), landing a final.metadata.json at a known path. Here Java loads that RUST-written
        // metadata, reads with IcebergGenerics (which APPLIES Rust's equality delete), and asserts the
        // merge-on-read rows == {(10,a),(30,c),(50,e)}. A failure here is a REAL write-incompatibility
        // finding (Rust wrote an equality delete Java cannot read).
        Path eqDeleteVerifyDir = requireFixturesDir("interop.eq_delete.dir");
        int eqDeleteFailures = EqDeleteOracle.verify(eqDeleteVerifyDir);
        System.out.println("verify-interop-eq-delete: " + eqDeleteFailures + " failures");
        if (eqDeleteFailures > 0) {
          System.exit(1);
        }
        break;
      case "verify":
        int failures = 0;
        failures += SchemaOracle.verify(schemaFixturesDir);
        failures += PartitionOracle.verify(partitionFixturesDir);
        failures += SnapshotOracle.verify(snapshotFixturesDir);
        System.out.println("verify (all capabilities): " + failures + " failures");
        if (failures > 0) {
          System.exit(1);
        }
        break;
      default:
        System.err.println("unknown mode: " + mode + " (expected generate|verify)");
        System.exit(2);
    }
  }

  /** Read a required system property naming an absolute fixtures directory, or exit non-zero. */
  private static Path requireFixturesDir(String property) {
    String value = System.getProperty(property);
    if (value == null || value.isEmpty()) {
      System.err.println("system property " + property + " must be set");
      System.exit(2);
    }
    return Paths.get(value).toAbsolutePath().normalize();
  }

  // ===========================================================================================
  // UpdateSchema oracle — unchanged behavior; the scenario registry + generate/verify moved into a
  // nested class so the partition oracle can sit beside it in the same exec entrypoint.
  // ===========================================================================================

  /**
   * The UpdateSchema half of the oracle. Each scenario is a base schema + last column id + an
   * UpdateSchema op-sequence applied via the {@code @VisibleForTesting SchemaUpdate(Schema, int)}
   * constructor. The Rust test mirrors EACH op-sequence exactly against the same {@code base.metadata.json}.
   */
  static final class SchemaOracle {
    private SchemaOracle() {}

    private static Map<String, SchemaScenario> scenarios() {
      Map<String, SchemaScenario> scenarios = new LinkedHashMap<>();

      // add_top_level_columns — append two optional and one required-with-default top-level columns.
      // The required-with-default add needs an initial default, which is V3-only in Java.
      scenarios.put(
          "add_top_level_columns",
          SchemaScenario.v3(
              new Schema(
                  Types.NestedField.required(1, "id", Types.LongType.get()),
                  Types.NestedField.optional(2, "data", Types.StringType.get())),
              2,
              update ->
                  update
                      .addColumn("count", Types.IntegerType.get())
                      .addColumn("note", Types.StringType.get(), "a free-text note")
                      .addRequiredColumn(
                          "category", Types.StringType.get(), Literal.of("uncategorized"))));

      // add_nested_struct_and_map — THE level-order fresh-field-id case. Adding a map<struct,struct> to a
      // 1-column schema must assign key=3, value=4, then the key struct's fields 5..8, then the value
      // struct's fields 9..10 (Java AssignFreshIds / CustomOrderSchemaVisitor level order). The incoming
      // ids are deliberately scrambled to prove they are reassigned.
      scenarios.put(
          "add_nested_struct_and_map",
          SchemaScenario.v2(
              new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
              1,
              update ->
                  update.addColumn(
                      "locations",
                      Types.MapType.ofOptional(
                          11,
                          12,
                          Types.StructType.of(
                              Types.NestedField.required(20, "address", Types.StringType.get()),
                              Types.NestedField.required(21, "city", Types.StringType.get()),
                              Types.NestedField.required(22, "state", Types.StringType.get()),
                              Types.NestedField.required(23, "zip", Types.IntegerType.get())),
                          Types.StructType.of(
                              Types.NestedField.required(30, "lat", Types.IntegerType.get()),
                              Types.NestedField.optional(31, "long", Types.IntegerType.get()))))));

      // rename_and_move — rename a column and reorder columns (move first + move after). Java resolves
      // move targets by their ORIGINAL name.
      scenarios.put(
          "rename_and_move",
          SchemaScenario.v2(
              new Schema(
                  Types.NestedField.required(1, "id", Types.LongType.get()),
                  Types.NestedField.optional(2, "first_name", Types.StringType.get()),
                  Types.NestedField.optional(3, "last_name", Types.StringType.get()),
                  Types.NestedField.optional(4, "email", Types.StringType.get())),
              4,
              update ->
                  update
                      .renameColumn("email", "email_address")
                      .moveFirst("email")
                      .moveAfter("id", "first_name")));

      // update_type_promotion — int->long, float->double, decimal(9,2)->decimal(18,2) widen.
      scenarios.put(
          "update_type_promotion",
          SchemaScenario.v2(
              new Schema(
                  Types.NestedField.required(1, "id", Types.IntegerType.get()),
                  Types.NestedField.optional(2, "measure", Types.FloatType.get()),
                  Types.NestedField.optional(3, "amount", Types.DecimalType.of(9, 2))),
              3,
              update ->
                  update
                      .updateColumn("id", Types.LongType.get())
                      .updateColumn("measure", Types.DoubleType.get())
                      .updateColumn("amount", Types.DecimalType.of(18, 2))));

      // make_optional_and_delete — relax a required column to optional, and delete another column.
      scenarios.put(
          "make_optional_and_delete",
          SchemaScenario.v2(
              new Schema(
                  Types.NestedField.required(1, "id", Types.LongType.get()),
                  Types.NestedField.required(2, "name", Types.StringType.get()),
                  Types.NestedField.optional(3, "legacy", Types.StringType.get())),
              3,
              update -> update.makeColumnOptional("name").deleteColumn("legacy")));

      // set_identifier_fields — promote a required field to the identifier-field set.
      scenarios.put(
          "set_identifier_fields",
          SchemaScenario.v2(
              new Schema(
                  Types.NestedField.required(1, "id", Types.LongType.get()),
                  Types.NestedField.required(2, "tenant", Types.StringType.get()),
                  Types.NestedField.optional(3, "data", Types.StringType.get())),
              3,
              update -> update.setIdentifierFields("id", "tenant")));

      // add_required_with_default_and_update_default — add a required column WITH a default (legal
      // without allowIncompatibleChanges), then change its write default via updateColumnDefault.
      scenarios.put(
          "add_required_with_default_and_update_default",
          SchemaScenario.v3(
              new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
              1,
              update ->
                  update
                      .addRequiredColumn("status", Types.StringType.get(), Literal.of("active"))
                      .updateColumnDefault("status", Literal.of("pending"))));

      return scenarios;
    }

    private static void generate(Path fixturesDir) throws IOException {
      Map<String, SchemaScenario> scenarios = scenarios();
      for (Map.Entry<String, SchemaScenario> entry : scenarios.entrySet()) {
        String name = entry.getKey();
        SchemaScenario scenario = entry.getValue();

        Map<String, String> props = new LinkedHashMap<>();
        props.put(TableProperties.FORMAT_VERSION, Integer.toString(scenario.formatVersion));
        TableMetadata base =
            TableMetadata.newTableMetadata(
                scenario.baseSchema,
                PartitionSpec.unpartitioned(),
                SortOrder.unsorted(),
                SCHEMA_LOCATION + "/" + name,
                props);

        SchemaUpdate update = new SchemaUpdate(base.schema(), base.lastColumnId());
        Schema evolved = scenario.ops.apply(update).apply();

        // The new last-column-id must never DECREASE below the base's (a delete lowers
        // highestFieldId() but ids are never reused), so pass max(base.lastColumnId,
        // evolved.highestFieldId) — exactly what Java's addSchema does internally.
        int evolvedLastColumnId = Math.max(base.lastColumnId(), evolved.highestFieldId());
        TableMetadata javaEvolved =
            TableMetadata.buildFrom(base).setCurrentSchema(evolved, evolvedLastColumnId).build();

        Path scenarioDir = fixturesDir.resolve(name);
        Files.createDirectories(scenarioDir);
        writeJson(scenarioDir.resolve("base.metadata.json"), TableMetadataParser.toJson(base));
        writeJson(
            scenarioDir.resolve("java_evolved.metadata.json"),
            TableMetadataParser.toJson(javaEvolved));
        System.out.println("generated schema: " + name);
      }
      System.out.println(
          "generate (schema): wrote " + scenarios.size() + " scenarios to " + fixturesDir);
    }

    /** Returns the number of failed scenarios. */
    private static int verify(Path fixturesDir) throws IOException {
      Map<String, SchemaScenario> scenarios = scenarios();
      int failures = 0;
      for (Map.Entry<String, SchemaScenario> entry : scenarios.entrySet()) {
        String name = entry.getKey();
        SchemaScenario scenario = entry.getValue();
        Path scenarioDir = fixturesDir.resolve(name);
        Path rustEvolvedPath = scenarioDir.resolve("rust_evolved.metadata.json");

        if (!Files.exists(rustEvolvedPath)) {
          System.out.println(
              "FAIL schema/" + name + ": missing rust_evolved.metadata.json (run the Rust gen)");
          failures++;
          continue;
        }

        Path basePath = scenarioDir.resolve("base.metadata.json");
        TableMetadata base = TableMetadataParser.fromJson(basePath.toString(), readString(basePath));
        SchemaUpdate update = new SchemaUpdate(base.schema(), base.lastColumnId());
        Schema javaEvolvedSchema = scenario.ops.apply(update).apply();

        TableMetadata rustEvolved;
        try {
          rustEvolved =
              TableMetadataParser.fromJson(rustEvolvedPath.toString(), readString(rustEvolvedPath));
        } catch (RuntimeException parseError) {
          System.out.println(
              "FAIL schema/" + name + ": Java could not parse rust_evolved: " + parseError);
          failures++;
          continue;
        }

        String mismatch = structuralSchemaMismatch(javaEvolvedSchema, rustEvolved.schema());
        if (mismatch != null) {
          System.out.println("FAIL schema/" + name + ": " + mismatch);
          failures++;
          continue;
        }
        int expectedLastColumnId =
            Math.max(base.lastColumnId(), javaEvolvedSchema.highestFieldId());
        if (expectedLastColumnId != rustEvolved.lastColumnId()) {
          System.out.println(
              "FAIL schema/"
                  + name
                  + ": last-column-id mismatch: java="
                  + expectedLastColumnId
                  + " rust="
                  + rustEvolved.lastColumnId());
          failures++;
          continue;
        }
        System.out.println("PASS schema/" + name);
      }
      System.out.println(
          "verify (schema): "
              + (scenarios.size() - failures)
              + "/"
              + scenarios.size()
              + " scenarios passed");
      return failures;
    }

    /**
     * Compare two schemas structurally. {@link Schema#asStruct} includes field id, name, type,
     * required, doc, and default recursively; identifier-field ids are compared separately.
     */
    private static String structuralSchemaMismatch(Schema expected, Schema actual) {
      if (!expected.asStruct().equals(actual.asStruct())) {
        return "schema struct mismatch:\n  java= "
            + expected.asStruct()
            + "\n  rust= "
            + actual.asStruct();
      }
      if (!expected.identifierFieldIds().equals(actual.identifierFieldIds())) {
        return "identifier-field-id mismatch: java="
            + expected.identifierFieldIds()
            + " rust="
            + actual.identifierFieldIds();
      }
      return null;
    }
  }

  /** A base schema + last column id + the UpdateSchema op-sequence + the base format version. */
  private static final class SchemaScenario {
    final Schema baseSchema;
    final int baseLastColumnId;
    final int formatVersion;
    final Function<UpdateSchema, UpdateSchema> ops;

    SchemaScenario(
        Schema baseSchema,
        int baseLastColumnId,
        int formatVersion,
        Function<UpdateSchema, UpdateSchema> ops) {
      this.baseSchema = baseSchema;
      this.baseLastColumnId = baseLastColumnId;
      this.formatVersion = formatVersion;
      this.ops = ops;
    }

    static SchemaScenario v2(
        Schema baseSchema, int baseLastColumnId, Function<UpdateSchema, UpdateSchema> ops) {
      return new SchemaScenario(baseSchema, baseLastColumnId, 2, ops);
    }

    static SchemaScenario v3(
        Schema baseSchema, int baseLastColumnId, Function<UpdateSchema, UpdateSchema> ops) {
      return new SchemaScenario(baseSchema, baseLastColumnId, 3, ops);
    }
  }

  // ===========================================================================================
  // UpdatePartitionSpec oracle — the new capability. Each scenario supplies a base TableMetadata and
  // an op-sequence applied through a REAL BaseUpdatePartitionSpec via an in-memory TableOperations, so
  // `base != null` and the historical field-id recycling path (recycleOrCreatePartitionField) is live.
  // The Rust test mirrors EACH op-sequence exactly against the same base.metadata.json.
  // ===========================================================================================

  /**
   * The UpdatePartitionSpec half of the oracle. We drive {@code BaseTable(ops, name).updateSpec()...} so
   * the recycling branch (guarded on {@code formatVersion >= 2 && base != null}) is exercised — the
   * {@code @VisibleForTesting} constructors set {@code base = null} and would silently skip it.
   */
  static final class PartitionOracle {
    private PartitionOracle() {}

    private static Map<String, PartitionScenario> scenarios() {
      Map<String, PartitionScenario> scenarios = new LinkedHashMap<>();

      // A V2 schema reused by most scenarios: id (long), category (string), event_ts (timestamp).
      Schema v2Schema =
          new Schema(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "category", Types.StringType.get()),
              Types.NestedField.optional(3, "event_ts", Types.TimestampType.withZone()));

      // add_identity_field — start unpartitioned, add identity(category). The base case.
      scenarios.put(
          "add_identity_field",
          PartitionScenario.unpartitioned(
              v2Schema, 2, "add_identity_field", spec -> spec.addField("category")));

      // add_transform_fields — start unpartitioned, add bucket[16](id), truncate[8](category),
      // year(event_ts). Pins the auto-generated names (PartitionNameGenerator) AND the field-ids that
      // BaseUpdatePartitionSpec assigns sequentially from last-partition-id (999 -> 1000, 1001, 1002).
      scenarios.put(
          "add_transform_fields",
          PartitionScenario.unpartitioned(
              v2Schema,
              2,
              "add_transform_fields",
              spec ->
                  spec.addField(org.apache.iceberg.expressions.Expressions.bucket("id", 16))
                      .addField(org.apache.iceberg.expressions.Expressions.truncate("category", 8))
                      .addField(org.apache.iceberg.expressions.Expressions.year("event_ts"))));

      // remove_field_v2 — base is partitioned by identity(category); removing it on V2 OMITS the field,
      // yielding an unpartitioned (empty) default spec.
      scenarios.put(
          "remove_field_v2",
          PartitionScenario.partitioned(
              v2Schema,
              2,
              2,
              "remove_field_v2",
              builder -> builder.identity("category"),
              spec -> spec.removeField("category")));

      // remove_field_v1_void — base (V1) partitioned by identity(category); removing it must RE-ADD it
      // with the void/alwaysNull transform to preserve the field id (Java V1 apply() branch).
      scenarios.put(
          "remove_field_v1_void",
          PartitionScenario.partitioned(
              v2Schema,
              1,
              2,
              "remove_field_v1_void",
              builder -> builder.identity("category"),
              spec -> spec.removeField("category")));

      // rename_field — base partitioned by identity(category); rename it. Field id preserved.
      scenarios.put(
          "rename_field",
          PartitionScenario.partitioned(
              v2Schema,
              2,
              2,
              "rename_field",
              builder -> builder.identity("category"),
              spec -> spec.renameField("category", "cat")));

      // field_id_recycling — base carries TWO historical specs: spec 0 (default) is identity(category)
      // @1000, spec 1 is bucket[8](id) under the CUSTOM name "id_shard" @1001. Re-adding bucket[8](id)
      // with NO explicit name must recycle BOTH the historical field id (1001) AND the historical name
      // ("id_shard") — not a fresh id or the generated default name "id_bucket_8". Needs base != null.
      scenarios.put(
          "field_id_recycling",
          PartitionScenario.forked(
              v2Schema,
              "field_id_recycling",
              spec ->
                  spec.addField(org.apache.iceberg.expressions.Expressions.bucket("id", 8))));

      // delete_then_readd — base partitioned by identity(category); remove then re-add the same
      // (source, transform) → Java's rewrite/un-delete (field restored with its original id, no new
      // field). The result equals the base spec, so the metadata layer dedups back to it.
      scenarios.put(
          "delete_then_readd",
          PartitionScenario.partitioned(
              v2Schema,
              2,
              2,
              "delete_then_readd",
              builder -> builder.identity("category"),
              spec -> spec.removeField("category").addField("category")));

      return scenarios;
    }

    private static void generate(Path fixturesDir) throws IOException {
      Map<String, PartitionScenario> scenarios = scenarios();
      for (Map.Entry<String, PartitionScenario> entry : scenarios.entrySet()) {
        String name = entry.getKey();
        PartitionScenario scenario = entry.getValue();

        TableMetadata base = scenario.baseMetadata();
        TableMetadata javaEvolved = scenario.evolve(base);

        Path scenarioDir = fixturesDir.resolve(name);
        Files.createDirectories(scenarioDir);
        writeJson(scenarioDir.resolve("base.metadata.json"), TableMetadataParser.toJson(base));
        writeJson(
            scenarioDir.resolve("java_evolved.metadata.json"),
            TableMetadataParser.toJson(javaEvolved));
        System.out.println("generated partition: " + name);
      }
      System.out.println(
          "generate (partition): wrote " + scenarios.size() + " scenarios to " + fixturesDir);
    }

    /** Returns the number of failed scenarios. */
    private static int verify(Path fixturesDir) throws IOException {
      Map<String, PartitionScenario> scenarios = scenarios();
      int failures = 0;
      for (Map.Entry<String, PartitionScenario> entry : scenarios.entrySet()) {
        String name = entry.getKey();
        PartitionScenario scenario = entry.getValue();
        Path scenarioDir = fixturesDir.resolve(name);
        Path rustEvolvedPath = scenarioDir.resolve("rust_evolved.metadata.json");

        if (!Files.exists(rustEvolvedPath)) {
          System.out.println(
              "FAIL partition/" + name + ": missing rust_evolved.metadata.json (run the Rust gen)");
          failures++;
          continue;
        }

        // Recompute Java's evolved metadata from the committed base (same op-sequence as generate).
        Path basePath = scenarioDir.resolve("base.metadata.json");
        TableMetadata base = TableMetadataParser.fromJson(basePath.toString(), readString(basePath));
        TableMetadata javaEvolved = scenario.evolve(base);

        TableMetadata rustEvolved;
        try {
          rustEvolved =
              TableMetadataParser.fromJson(rustEvolvedPath.toString(), readString(rustEvolvedPath));
        } catch (RuntimeException parseError) {
          System.out.println(
              "FAIL partition/" + name + ": Java could not parse rust_evolved: " + parseError);
          failures++;
          continue;
        }

        String mismatch = structuralSpecMismatch(javaEvolved, rustEvolved);
        if (mismatch != null) {
          System.out.println("FAIL partition/" + name + ": " + mismatch);
          failures++;
          continue;
        }
        System.out.println("PASS partition/" + name);
      }
      System.out.println(
          "verify (partition): "
              + (scenarios.size() - failures)
              + "/"
              + scenarios.size()
              + " scenarios passed");
      return failures;
    }

    /**
     * Compare the evolved DEFAULT partition specs structurally: spec id, field count, and each field's
     * source id / field id / name / transform (via {@link PartitionField#equals}, which covers all
     * four), plus the table's last-assigned-partition-id. Returns null when equal, else a message.
     */
    private static String structuralSpecMismatch(TableMetadata expected, TableMetadata actual) {
      PartitionSpec expectedSpec = expected.spec();
      PartitionSpec actualSpec = actual.spec();
      if (expectedSpec.specId() != actualSpec.specId()) {
        return "default spec-id mismatch: java="
            + expectedSpec.specId()
            + " rust="
            + actualSpec.specId();
      }
      // PartitionField.equals compares sourceId, fieldId, name, and transform — exactly the field-id
      // level identity we need. List equality compares element-wise in order.
      if (!expectedSpec.fields().equals(actualSpec.fields())) {
        return "default spec fields mismatch:\n  java= "
            + expectedSpec.fields()
            + "\n  rust= "
            + actualSpec.fields();
      }
      if (expected.lastAssignedPartitionId() != actual.lastAssignedPartitionId()) {
        return "last-partition-id mismatch: java="
            + expected.lastAssignedPartitionId()
            + " rust="
            + actual.lastAssignedPartitionId();
      }
      return null;
    }
  }

  /**
   * A partition-spec scenario: how to build the base {@link TableMetadata} and the UpdatePartitionSpec
   * op-sequence to apply. The op-sequence is applied through a real {@link BaseUpdatePartitionSpec}
   * (via {@link BaseTable#updateSpec()} over an {@link InMemoryTableOperations}) so the recycling path
   * is live; the evolved metadata is read back from {@code ops.current()} after {@code commit()}.
   */
  private static final class PartitionScenario {
    final Function<Void, TableMetadata> baseSupplier;
    final Function<UpdatePartitionSpec, UpdatePartitionSpec> ops;

    PartitionScenario(
        Function<Void, TableMetadata> baseSupplier,
        Function<UpdatePartitionSpec, UpdatePartitionSpec> ops) {
      this.baseSupplier = baseSupplier;
      this.ops = ops;
    }

    TableMetadata baseMetadata() {
      return baseSupplier.apply(null);
    }

    /** Drive the op-sequence through a real BaseUpdatePartitionSpec and return the evolved metadata. */
    TableMetadata evolve(TableMetadata base) {
      InMemoryTableOperations operations = new InMemoryTableOperations(base);
      BaseTable table = new BaseTable(operations, "interop");
      ops.apply(table.updateSpec()).commit();
      return operations.current();
    }

    /** An unpartitioned base at the given format version. */
    static PartitionScenario unpartitioned(
        Schema schema,
        int formatVersion,
        String name,
        Function<UpdatePartitionSpec, UpdatePartitionSpec> ops) {
      return new PartitionScenario(
          ignored -> newBase(schema, PartitionSpec.unpartitioned(), formatVersion, name), ops);
    }

    /** A base partitioned by a single (built) partition spec at the given format version. */
    static PartitionScenario partitioned(
        Schema schema,
        int formatVersion,
        int unusedSourceColumns,
        String name,
        Function<PartitionSpec.Builder, PartitionSpec.Builder> specBuilder,
        Function<UpdatePartitionSpec, UpdatePartitionSpec> ops) {
      return new PartitionScenario(
          ignored -> {
            PartitionSpec spec = specBuilder.apply(PartitionSpec.builderFor(schema)).build();
            return newBase(schema, spec, formatVersion, name);
          },
          ops);
    }

    /**
     * A V2 base carrying TWO historical specs for the recycling scenario: spec 0 (default) is
     * identity(category) @1000; spec 1 is bucket[8](id) under the CUSTOM name "id_shard" @1001;
     * last-partition-id = 1001. Built REALISTICALLY by evolving the identity(category) base through a
     * real {@link BaseUpdatePartitionSpec} (which assigns the bucket field the next sequential id, 1001,
     * advancing last-partition-id globally) as a NON-default spec. Driving it this way — rather than
     * building two independent specs that would BOTH start their field ids at 1000 — is what a real V2
     * table looks like and is required for recycling: a field id must be unique within a single spec, so
     * the re-add of bucket[8](id) recycles 1001 (not 1000, which collides with identity(category)).
     */
    static PartitionScenario forked(
        Schema schema,
        String name,
        Function<UpdatePartitionSpec, UpdatePartitionSpec> ops) {
      return new PartitionScenario(
          ignored -> {
            PartitionSpec defaultSpec =
                PartitionSpec.builderFor(schema).identity("category").build();
            TableMetadata base = newBase(schema, defaultSpec, 2, name);
            // Evolve in a historical (non-default) spec via the real action so the bucket field gets a
            // fresh sequential id (1001), exactly as a production V2 evolution would.
            InMemoryTableOperations operations = new InMemoryTableOperations(base);
            BaseTable table = new BaseTable(operations, "interop");
            table
                .updateSpec()
                .addNonDefaultSpec()
                .addField(
                    "id_shard", org.apache.iceberg.expressions.Expressions.bucket("id", 8))
                .commit();
            return operations.current();
          },
          ops);
    }

    private static TableMetadata newBase(
        Schema schema, PartitionSpec spec, int formatVersion, String name) {
      Map<String, String> props = new LinkedHashMap<>();
      props.put(TableProperties.FORMAT_VERSION, Integer.toString(formatVersion));
      return TableMetadata.newTableMetadata(
          schema, spec, SortOrder.unsorted(), PARTITION_LOCATION + "/" + name, props);
    }
  }

  // ===========================================================================================
  // ManageSnapshots oracle — the THIRD capability. Unlike schema/partition, ref operations act on the
  // snapshot graph + refs, so the base TableMetadata must carry a REAL snapshot history. Each scenario
  // shares one forked base (snapshots ROOT, CURRENT(child of ROOT), SIBLING(child of ROOT); refs
  // main->CURRENT, `dev` branch->CURRENT, `stable` tag->ROOT — mirroring the Rust action's
  // `forked_table()` fixture) and applies a `ManageSnapshots` op-sequence via a REAL `SnapshotManager`
  // (`new BaseTable(ops, name).manageSnapshots()…commit()`) over the same in-memory `TableOperations`.
  // The Rust test mirrors EACH op-sequence exactly against the same `base.metadata.json`. We compare the
  // evolved REFS map (snapshot-id + branch-vs-tag kind + retention) + the current-snapshot-id (main);
  // the snapshot list itself is unchanged by ref ops.
  // ===========================================================================================

  /**
   * The ManageSnapshots half of the oracle. The op-sequence is driven through a real
   * {@link SnapshotManager} (via {@link BaseTable#manageSnapshots()} over the in-memory
   * {@link TableOperations}) so the production ref-management + rollback paths are exercised end to end;
   * the evolved metadata is read back from {@code ops.current()} after {@code commit()}. Ref-only ops
   * never touch data files, so the no-op {@code io()} on {@link InMemoryTableOperations} is never reached
   * (the transaction's file-cleanup path returns early for an empty new-snapshot set).
   */
  static final class SnapshotOracle {
    private SnapshotOracle() {}

    // The forked base's snapshot graph. Distinct, increasing timestamps so `rollback_to_time` is
    // deterministic; increasing sequence numbers so V2 `addSnapshot` validation passes (a child snapshot
    // must carry a sequence number greater than the last). All three timestamps are far in the past, so
    // they trivially satisfy `ts <= lastUpdatedMillis` (buildFrom stamps last-updated-ms at build time).
    static final long ROOT_ID = 3051729675574597004L;
    static final long CURRENT_ID = 3055729675574597004L;
    static final long SIBLING_ID = 3060729675574597004L;
    static final long ROOT_TS_MS = 1515100955770L;
    static final long CURRENT_TS_MS = 1555100955770L;
    static final long SIBLING_TS_MS = 1600000000000L;

    private static Map<String, SnapshotScenario> scenarios() {
      Map<String, SnapshotScenario> scenarios = new LinkedHashMap<>();

      // create_branch_and_tag — create a branch @ROOT and a tag @CURRENT. Pins fresh-ref creation with
      // default (empty) retention and the branch-vs-tag kind.
      scenarios.put(
          "create_branch_and_tag",
          new SnapshotScenario(
              manage -> manage.createBranch("audit", ROOT_ID).createTag("release-1", CURRENT_ID)));

      // rollback_to_ancestor — main (CURRENT) -> ROOT, which IS an ancestor. Pins ancestry-checked
      // rollback (Java `SetSnapshotOperation.rollbackTo`).
      scenarios.put(
          "rollback_to_ancestor", new SnapshotScenario(manage -> manage.rollbackTo(ROOT_ID)));

      // rollback_to_time — a timestamp STRICTLY between ROOT and CURRENT resolves to ROOT (the newest
      // ancestor older than it; CURRENT is too new). Cross-checks the strict-`<` semantics
      // (`SetSnapshotOperation.findLatestAncestorOlderThan`: `timestampMillis() < timestampMillis`).
      scenarios.put(
          "rollback_to_time",
          new SnapshotScenario(manage -> manage.rollbackToTime(ROOT_TS_MS + 1)));

      // set_current_snapshot — main -> ROOT with NO ancestry requirement (Java `setCurrentSnapshot`).
      // Here ROOT happens to be an ancestor, but the op path does not check ancestry.
      scenarios.put(
          "set_current_snapshot",
          new SnapshotScenario(manage -> manage.setCurrentSnapshot(ROOT_ID)));

      // fast_forward — a branch @ROOT fast-forwarded to main (@CURRENT). ROOT is an ancestor of CURRENT,
      // so the branch advances to CURRENT (Java `fastForwardBranch` -> `replaceBranch(from, to, true)`).
      scenarios.put(
          "fast_forward",
          new SnapshotScenario(
              manage ->
                  manage
                      .createBranch("staging", ROOT_ID)
                      .fastForwardBranch("staging", SnapshotRef.MAIN_BRANCH)));

      // retention — set min_snapshots_to_keep + max_snapshot_age_ms on a BRANCH (`dev`), and
      // max_ref_age_ms on the `stable` TAG. Pins that branch-only retention fields land on a branch and
      // tag-legal retention (max_ref_age_ms) lands on a tag — the branch-vs-tag retention distinction.
      scenarios.put(
          "retention",
          new SnapshotScenario(
              manage ->
                  manage
                      .setMinSnapshotsToKeep("dev", 5)
                      .setMaxSnapshotAgeMs("dev", 86400000L)
                      .setMaxRefAgeMs("stable", 604800000L)));

      // remove_and_rename — remove the `stable` tag and rename the `dev` branch to `feature`. Pins ref
      // removal and branch rename (the renamed ref keeps the original snapshot id + retention).
      scenarios.put(
          "remove_and_rename",
          new SnapshotScenario(
              manage -> manage.removeTag("stable").renameBranch("dev", "feature")));

      return scenarios;
    }

    /**
     * Build the shared forked base {@link TableMetadata}: an unpartitioned V2 table with snapshots
     * {ROOT, CURRENT(child of ROOT), SIBLING(child of ROOT)} and refs {main->CURRENT, `dev`
     * branch->CURRENT, `stable` tag->ROOT}. SIBLING exists but is NOT in main's ancestry, so the
     * rollback/fast-forward scenarios have a valid-but-non-ancestor snapshot available (matching the Rust
     * `forked_table()` shape). The location embeds the scenario name so each fixture is self-describing.
     */
    private static TableMetadata buildBase(String name) {
      Schema schema =
          new Schema(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "data", Types.StringType.get()));
      Map<String, String> props = new LinkedHashMap<>();
      props.put(TableProperties.FORMAT_VERSION, "2");
      TableMetadata seed =
          TableMetadata.newTableMetadata(
              schema,
              PartitionSpec.unpartitioned(),
              SortOrder.unsorted(),
              SNAPSHOT_LOCATION + "/" + name,
              props);

      // V2 sequence numbers start at 1 (0 is reserved as INITIAL_SEQUENCE_NUMBER). Using 1/2/3 — rather
      // than 0/1/2 — means Java's SnapshotParser emits a `sequence-number` for EVERY snapshot (it omits
      // the field only when the value equals INITIAL_SEQUENCE_NUMBER), which keeps the fixture parseable
      // by the (spec-`required`) Rust V2 snapshot reader. (A snapshot with sequence-number 0 only arises
      // as a V1-carryover artifact; see the report for the latent Rust read-strictness note.)
      Snapshot root = snapshot(ROOT_ID, null, 1L, ROOT_TS_MS, seed.currentSchemaId());
      Snapshot current = snapshot(CURRENT_ID, ROOT_ID, 2L, CURRENT_TS_MS, seed.currentSchemaId());
      Snapshot sibling = snapshot(SIBLING_ID, ROOT_ID, 3L, SIBLING_TS_MS, seed.currentSchemaId());

      return TableMetadata.buildFrom(seed)
          // Add the full snapshot graph first, then attach refs. main is set LAST (and to CURRENT, the
          // newest snapshot) so the single snapshot-log entry is stamped at CURRENT's timestamp and the
          // build's last-updated-ms (System.currentTimeMillis) is never behind it.
          .addSnapshot(root)
          .addSnapshot(current)
          .addSnapshot(sibling)
          .setRef("dev", SnapshotRef.branchBuilder(CURRENT_ID).build())
          .setRef("stable", SnapshotRef.tagBuilder(ROOT_ID).build())
          .setBranchSnapshot(CURRENT_ID, SnapshotRef.MAIN_BRANCH)
          .build();
    }

    /** Construct a package-private {@link BaseSnapshot} with a fake (never-read) manifest-list path. */
    private static Snapshot snapshot(
        long snapshotId, Long parentId, long sequenceNumber, long timestampMs, int schemaId) {
      Map<String, String> summary = new LinkedHashMap<>();
      summary.put("operation", DataOperations.APPEND);
      return new BaseSnapshot(
          sequenceNumber,
          snapshotId,
          parentId,
          timestampMs,
          DataOperations.APPEND,
          summary,
          schemaId,
          SNAPSHOT_LOCATION + "/metadata/snap-" + snapshotId + ".avro",
          null,
          null,
          null);
    }

    private static void generate(Path fixturesDir) throws IOException {
      Map<String, SnapshotScenario> scenarios = scenarios();
      for (Map.Entry<String, SnapshotScenario> entry : scenarios.entrySet()) {
        String name = entry.getKey();
        SnapshotScenario scenario = entry.getValue();

        Path scenarioDir = fixturesDir.resolve(name);
        Files.createDirectories(scenarioDir);

        // Write the base FIRST, then re-parse it from disk before evolving. The freshly built
        // TableMetadata carries pending `AddSnapshot` changes from the history builder; `buildFrom`
        // copies those, so `isAddedSnapshot` would treat ROOT/CURRENT/SIBLING as just-added and stamp a
        // rollback's snapshot-log entry with the OLD snapshot timestamp (tripping the "before last
        // snapshot log entry" guard). A round-tripped TableMetadata has no pending changes — exactly the
        // clean base the Rust test and the Java `verify` step both load.
        TableMetadata base = buildBase(name);
        Path basePath = scenarioDir.resolve("base.metadata.json");
        writeJson(basePath, TableMetadataParser.toJson(base));
        TableMetadata cleanBase =
            TableMetadataParser.fromJson(basePath.toString(), readString(basePath));

        TableMetadata javaEvolved = scenario.evolve(cleanBase);
        writeJson(
            scenarioDir.resolve("java_evolved.metadata.json"),
            TableMetadataParser.toJson(javaEvolved));
        System.out.println("generated manage_snapshots: " + name);
      }
      System.out.println(
          "generate (manage_snapshots): wrote " + scenarios.size() + " scenarios to " + fixturesDir);
    }

    /** Returns the number of failed scenarios. */
    private static int verify(Path fixturesDir) throws IOException {
      Map<String, SnapshotScenario> scenarios = scenarios();
      int failures = 0;
      for (Map.Entry<String, SnapshotScenario> entry : scenarios.entrySet()) {
        String name = entry.getKey();
        SnapshotScenario scenario = entry.getValue();
        Path scenarioDir = fixturesDir.resolve(name);
        Path rustEvolvedPath = scenarioDir.resolve("rust_evolved.metadata.json");

        if (!Files.exists(rustEvolvedPath)) {
          System.out.println(
              "FAIL manage_snapshots/"
                  + name
                  + ": missing rust_evolved.metadata.json (run the Rust gen)");
          failures++;
          continue;
        }

        // Recompute Java's evolved metadata from the committed base (same op-sequence as generate).
        Path basePath = scenarioDir.resolve("base.metadata.json");
        TableMetadata base = TableMetadataParser.fromJson(basePath.toString(), readString(basePath));
        TableMetadata javaEvolved = scenario.evolve(base);

        TableMetadata rustEvolved;
        try {
          rustEvolved =
              TableMetadataParser.fromJson(rustEvolvedPath.toString(), readString(rustEvolvedPath));
        } catch (RuntimeException parseError) {
          System.out.println(
              "FAIL manage_snapshots/" + name + ": Java could not parse rust_evolved: " + parseError);
          failures++;
          continue;
        }

        String mismatch = structuralRefsMismatch(javaEvolved, rustEvolved);
        if (mismatch != null) {
          System.out.println("FAIL manage_snapshots/" + name + ": " + mismatch);
          failures++;
          continue;
        }
        System.out.println("PASS manage_snapshots/" + name);
      }
      System.out.println(
          "verify (manage_snapshots): "
              + (scenarios.size() - failures)
              + "/"
              + scenarios.size()
              + " scenarios passed");
      return failures;
    }

    /**
     * Compare the evolved REFS structurally: the ref names, and for each ref its snapshot-id, kind
     * (branch vs tag), and retention fields (min_snapshots_to_keep / max_snapshot_age_ms /
     * max_ref_age_ms) — all covered by {@link SnapshotRef#equals} — plus the current-snapshot-id (main).
     * Snapshots themselves are unchanged by ref ops, so they are not compared. Returns null when equal.
     */
    private static String structuralRefsMismatch(TableMetadata expected, TableMetadata actual) {
      Map<String, SnapshotRef> expectedRefs = expected.refs();
      Map<String, SnapshotRef> actualRefs = actual.refs();
      if (!expectedRefs.keySet().equals(actualRefs.keySet())) {
        return "ref-name-set mismatch:\n  java= " + expectedRefs.keySet() + "\n  rust= "
            + actualRefs.keySet();
      }
      for (Map.Entry<String, SnapshotRef> javaEntry : expectedRefs.entrySet()) {
        SnapshotRef javaRef = javaEntry.getValue();
        SnapshotRef rustRef = actualRefs.get(javaEntry.getKey());
        // SnapshotRef.equals compares snapshotId, type (branch/tag), and all three retention fields.
        if (!javaRef.equals(rustRef)) {
          return "ref `" + javaEntry.getKey() + "` mismatch:\n  java= " + javaRef + "\n  rust= "
              + rustRef;
        }
      }
      long expectedCurrent =
          expected.currentSnapshot() == null ? -1 : expected.currentSnapshot().snapshotId();
      long actualCurrent =
          actual.currentSnapshot() == null ? -1 : actual.currentSnapshot().snapshotId();
      if (expectedCurrent != actualCurrent) {
        return "current-snapshot-id mismatch: java=" + expectedCurrent + " rust=" + actualCurrent;
      }
      return null;
    }
  }

  /**
   * A ManageSnapshots scenario: an op-sequence applied through a real {@link SnapshotManager} over an
   * {@link InMemoryTableOperations} holding the shared forked base. The evolved metadata is read back
   * from {@code ops.current()} after {@code commit()}.
   */
  private static final class SnapshotScenario {
    final UnaryOperator<ManageSnapshots> ops;

    SnapshotScenario(UnaryOperator<ManageSnapshots> ops) {
      this.ops = ops;
    }

    /** Drive the op-sequence through a real SnapshotManager and return the evolved metadata. */
    TableMetadata evolve(TableMetadata base) {
      InMemoryTableOperations operations = new InMemoryTableOperations(base);
      BaseTable table = new BaseTable(operations, "interop");
      ops.apply(table.manageSnapshots()).commit();
      return operations.current();
    }
  }

  /**
   * A minimal in-memory {@link TableOperations} that just holds a {@link TableMetadata} and swaps it on
   * commit. This is the ONLY way to drive {@link BaseUpdatePartitionSpec} with {@code base != null} (so
   * the historical field-id recycling path is exercised) without a real catalog / object store; the
   * manage-snapshots oracle reuses it to drive {@link SnapshotManager} over a base with a real snapshot
   * history. Neither a partition-spec commit nor a ref-only ManageSnapshots commit reads or writes data
   * files, so {@code io()} / {@code locationProvider()} / {@code newSnapshotId()} stay no-op.
   */
  private static final class InMemoryTableOperations implements TableOperations {
    private TableMetadata current;

    InMemoryTableOperations(TableMetadata initial) {
      this.current = initial;
    }

    @Override
    public TableMetadata current() {
      return current;
    }

    @Override
    public TableMetadata refresh() {
      return current;
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      // In-memory, single-threaded oracle: trust the optimistic base check that BaseUpdatePartitionSpec
      // already performed and swap in the new metadata.
      this.current = metadata;
    }

    @Override
    public org.apache.iceberg.io.FileIO io() {
      throw new UnsupportedOperationException("interop oracle does not perform file IO");
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return current.location() + "/metadata/" + fileName;
    }

    @Override
    public org.apache.iceberg.io.LocationProvider locationProvider() {
      throw new UnsupportedOperationException("interop oracle does not provide data locations");
    }
  }

  // ===========================================================================================
  // Inspection oracle — the FOURTH capability. Unlike schema/partition/manage-snapshots (which evolve
  // metadata), the inspection metadata tables are READ-ONLY: they project a base TableMetadata into rows.
  // This oracle materializes the ACTUAL rows of Java's own SnapshotsTable / RefsTable (via
  // MetadataTableUtils + asDataTask().rows()) from a RE-PARSED base — the same bytes the Rust reader
  // consumes — and emits them as java_snapshots.json / java_refs.json. The Rust test asserts
  // `table.inspect().snapshots()/.refs().scan()` is field-for-field equal to those rows. There is only
  // ONE direction here (Rust reproduces Java's projection); the tables are not writable.
  //
  // CORRECTNESS NOTE: SnapshotsTable.snapshotToRow writes snap.summary() into the summary MAP column. On
  // the on-disk round-trip, SnapshotParser.fromJson splits the `operation` key OUT of the summary map
  // (operation becomes the typed `operation` column; the map keeps only the OTHER properties). Rust's
  // spec::Summary likewise hoists `operation` out and inspect/snapshots.rs emits only additional_properties
  // into the summary column. Materializing from a RE-PARSED base (step 3) is therefore what makes the
  // summary maps match with NO Rust change.
  // ===========================================================================================

  /**
   * The inspection half of the oracle. Builds a purpose-built V2 base that exercises the non-trivial
   * columns (multi-key summaries, branch/tag retention + NULLs), RE-PARSES it from disk (so the summary
   * maps are canonical and there is a non-null metadataFileLocation), and emits the rows of Java's REAL
   * {@link SnapshotsTable} / {@link RefsTable} — obtained via {@link MetadataTableUtils} and
   * {@code asDataTask().rows()} — as {@code java_snapshots.json} / {@code java_refs.json}.
   */
  static final class InspectionOracle {
    private InspectionOracle() {}

    // The base's snapshot graph. ROOT is the lone-key (operation-only) summary; CURRENT is the MULTI-KEY
    // summary (added-data-files / added-records / total-records survive the operation split); SIBLING is a
    // small two-key summary. Sequence numbers 1/2/3 so Java's SnapshotParser emits a sequence-number for
    // every snapshot (it omits it only for INITIAL_SEQUENCE_NUMBER 0), keeping the fixture parseable by the
    // spec-`required` Rust V2 snapshot reader.
    static final long ROOT_ID = 3051729675574597004L;
    static final long CURRENT_ID = 3055729675574597004L;
    static final long SIBLING_ID = 3060729675574597004L;
    static final long ROOT_TS_MS = 1515100955770L;
    static final long CURRENT_TS_MS = 1555100955770L;
    static final long SIBLING_TS_MS = 1600000000000L;

    static void generate(Path dir) throws IOException {
      // 1. Build a purpose-built base TableMetadata that exercises the non-trivial columns.
      TableMetadata base = buildBase();

      // 2. Write the base, then RE-PARSE it from disk. Re-parsing is what makes the summary maps canonical
      //    (operation split out by SnapshotParser.fromJson) AND gives a non-null metadataFileLocation that
      //    SnapshotsTable.task / RefsTable.task hand to io().newInputFile(...).
      Files.createDirectories(dir);
      Path basePath = dir.resolve("base.metadata.json");
      writeJson(basePath, TableMetadataParser.toJson(base));
      TableMetadata reparsed =
          TableMetadataParser.fromJson(basePath.toString(), readString(basePath));

      // 3. Build a BaseTable over in-memory ops whose io() is an InMemoryFileIO that has the metadata file
      //    PRE-ADDED, so SnapshotsTable.task / RefsTable.task's io().newInputFile(metadataFileLocation())
      //    does not throw. (The InputFile is only HELD by StaticDataTask, never read for these pure-metadata
      //    tables.)
      InMemoryFileIO io = new InMemoryFileIO();
      io.addFile(reparsed.metadataFileLocation(), Files.readAllBytes(basePath));
      BaseTable baseTable =
          new BaseTable(new InMemoryInspectionOperations(reparsed, io), "interop_inspection");

      // 4. Materialize + emit the rows of Java's REAL SnapshotsTable and RefsTable.
      writeJson(
          dir.resolve("java_snapshots.json"),
          rowsToJson(baseTable, MetadataTableType.SNAPSHOTS, InspectionOracle::snapshotRowToJson));
      writeJson(
          dir.resolve("java_refs.json"),
          rowsToJson(baseTable, MetadataTableType.REFS, InspectionOracle::refRowToJson));
      System.out.println("generated inspection fixtures to " + dir);
    }

    /**
     * Build the purpose-built base: an unpartitioned V2 table (id long required, data string optional) with
     * snapshots {ROOT, CURRENT(child of ROOT), SIBLING(child of ROOT)} and refs {main->CURRENT branch,
     * dev->CURRENT branch with FULL retention, stable->ROOT tag with ONLY maxRefAgeMs}. Mirrors
     * SnapshotOracle.buildBase, but with the multi-key summaries + the full-retention refs the inspection
     * columns need. The branch/tag retention shapes exercise every retention column + its NULL pattern.
     */
    private static TableMetadata buildBase() {
      Schema schema =
          new Schema(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "data", Types.StringType.get()));
      Map<String, String> props = new LinkedHashMap<>();
      props.put(TableProperties.FORMAT_VERSION, "2");
      TableMetadata seed =
          TableMetadata.newTableMetadata(
              schema,
              PartitionSpec.unpartitioned(),
              SortOrder.unsorted(),
              INSPECTION_LOCATION,
              props);

      // ROOT: lone-key (operation-only) summary.
      Map<String, String> rootSummary = new LinkedHashMap<>();
      rootSummary.put("operation", DataOperations.APPEND);

      // CURRENT: MULTI-KEY summary. After the operation split, the summary MAP column retains
      // added-data-files / added-records / total-records.
      Map<String, String> currentSummary = new LinkedHashMap<>();
      currentSummary.put("operation", DataOperations.APPEND);
      currentSummary.put("added-data-files", "3");
      currentSummary.put("added-records", "100");
      currentSummary.put("total-records", "100");

      // SIBLING: a small two-key summary (operation + one surviving property).
      Map<String, String> siblingSummary = new LinkedHashMap<>();
      siblingSummary.put("operation", DataOperations.APPEND);
      siblingSummary.put("added-data-files", "1");

      Snapshot root = snapshot(ROOT_ID, null, 1L, ROOT_TS_MS, rootSummary, seed.currentSchemaId());
      Snapshot current =
          snapshot(CURRENT_ID, ROOT_ID, 2L, CURRENT_TS_MS, currentSummary, seed.currentSchemaId());
      Snapshot sibling =
          snapshot(SIBLING_ID, ROOT_ID, 3L, SIBLING_TS_MS, siblingSummary, seed.currentSchemaId());

      return TableMetadata.buildFrom(seed)
          .addSnapshot(root)
          .addSnapshot(current)
          .addSnapshot(sibling)
          // dev branch @CURRENT with FULL retention; stable tag @ROOT with ONLY maxRefAgeMs. main is set
          // LAST (and to CURRENT, the newest snapshot) so the snapshot-log entry is stamped at CURRENT's
          // timestamp and last-updated-ms is never behind it.
          .setRef(
              "dev",
              SnapshotRef.branchBuilder(CURRENT_ID)
                  .minSnapshotsToKeep(2)
                  .maxSnapshotAgeMs(86400000L)
                  .maxRefAgeMs(604800000L)
                  .build())
          .setRef("stable", SnapshotRef.tagBuilder(ROOT_ID).maxRefAgeMs(259200000L).build())
          .setBranchSnapshot(CURRENT_ID, SnapshotRef.MAIN_BRANCH)
          .build();
    }

    /** Construct a package-private {@link BaseSnapshot} with a fake (never-read) manifest-list path. */
    private static Snapshot snapshot(
        long snapshotId,
        Long parentId,
        long sequenceNumber,
        long timestampMs,
        Map<String, String> summary,
        int schemaId) {
      return new BaseSnapshot(
          sequenceNumber,
          snapshotId,
          parentId,
          timestampMs,
          DataOperations.APPEND,
          summary,
          schemaId,
          INSPECTION_LOCATION + "/metadata/snap-" + snapshotId + ".avro",
          null,
          null,
          null);
    }

    /**
     * Materialize the rows of Java's REAL metadata table of {@code type} (SnapshotsTable / RefsTable) via
     * {@link MetadataTableUtils#createMetadataTableInstance} + {@code task.asDataTask().rows()} and serialize
     * each {@link StructLike} row with {@code rowToJson}. Columns are read BY POSITION per the metadata
     * table's own schema — exactly the rows Java's planner would feed a scan engine.
     *
     * <p>IMPORTANT: each row MUST be serialized EAGERLY inside the iteration. {@code StaticDataTask.rows()}
     * is a lazy {@code Iterables.transform} over a SINGLE mutable {@link org.apache.iceberg.util.StructProjection}
     * that {@code wrap}s each underlying row in turn — accumulating the {@link StructLike} references into a
     * list and reading them afterwards would yield the LAST row repeated N times (every reference aliases
     * the same re-wrapped projection). Writing JSON per row during the loop captures each row's values
     * while the projection still points at it.
     */
    private static String rowsToJson(
        BaseTable baseTable, MetadataTableType type, RowWriter rowToJson) {
      Table mt = MetadataTableUtils.createMetadataTableInstance(baseTable, type);
      return JsonUtil.generate(
          gen -> {
            gen.writeStartArray();
            try (CloseableIterable<FileScanTask> tasks = mt.newScan().planFiles()) {
              for (FileScanTask task : tasks) {
                try (CloseableIterable<StructLike> taskRows = task.asDataTask().rows()) {
                  for (StructLike row : taskRows) {
                    rowToJson.write(row, gen);
                  }
                }
              }
            }
            gen.writeEndArray();
          },
          true);
    }

    /**
     * Serialize one SnapshotsTable row by position: 0=committed_at micros Long, 1=snapshot_id Long,
     * 2=parent_id Long-or-null, 3=operation String-or-null, 4=manifest_list String-or-null, 5=summary
     * Map<String,String>. committed_at is emitted as the raw micros long; nulls as JSON null; summary as a
     * JSON object {string:string}.
     */
    @SuppressWarnings("unchecked")
    private static void snapshotRowToJson(StructLike row, JsonGenerator gen) throws IOException {
      gen.writeStartObject();
      gen.writeNumberField("committed_at", row.get(0, Long.class));
      gen.writeNumberField("snapshot_id", row.get(1, Long.class));
      writeLongOrNull(gen, "parent_id", row.get(2, Long.class));
      writeStringOrNull(gen, "operation", row.get(3, String.class));
      writeStringOrNull(gen, "manifest_list", row.get(4, String.class));
      Map<String, String> summary = row.get(5, Map.class);
      gen.writeObjectFieldStart("summary");
      if (summary != null) {
        for (Map.Entry<String, String> entry : summary.entrySet()) {
          gen.writeStringField(entry.getKey(), entry.getValue());
        }
      }
      gen.writeEndObject();
      gen.writeEndObject();
    }

    /**
     * Serialize one RefsTable row by position: 0=name, 1=type, 2=snapshot_id Long,
     * 3=max_reference_age_in_ms Long-or-null, 4=min_snapshots_to_keep Integer-or-null,
     * 5=max_snapshot_age_in_ms Long-or-null.
     */
    private static void refRowToJson(StructLike row, JsonGenerator gen) throws IOException {
      gen.writeStartObject();
      gen.writeStringField("name", row.get(0, String.class));
      gen.writeStringField("type", row.get(1, String.class));
      gen.writeNumberField("snapshot_id", row.get(2, Long.class));
      writeLongOrNull(gen, "max_reference_age_in_ms", row.get(3, Long.class));
      writeIntOrNull(gen, "min_snapshots_to_keep", row.get(4, Integer.class));
      writeLongOrNull(gen, "max_snapshot_age_in_ms", row.get(5, Long.class));
      gen.writeEndObject();
    }

    private static void writeLongOrNull(JsonGenerator gen, String field, Long value)
        throws IOException {
      if (value == null) {
        gen.writeNullField(field);
      } else {
        gen.writeNumberField(field, value);
      }
    }

    private static void writeIntOrNull(JsonGenerator gen, String field, Integer value)
        throws IOException {
      if (value == null) {
        gen.writeNullField(field);
      } else {
        gen.writeNumberField(field, value.intValue());
      }
    }

    private static void writeStringOrNull(JsonGenerator gen, String field, String value)
        throws IOException {
      if (value == null) {
        gen.writeNullField(field);
      } else {
        gen.writeStringField(field, value);
      }
    }

    /** Serializes one {@link StructLike} metadata-table row to JSON. */
    @FunctionalInterface
    private interface RowWriter {
      void write(StructLike row, JsonGenerator gen) throws IOException;
    }
  }

  // ===========================================================================================
  // Inspection-LOG oracle — the two REMAINING pure-metadata inspection tables: `history` and
  // `metadata_log_entries`. Like InspectionOracle these are READ-ONLY projections; this oracle
  // materializes the rows of Java's REAL HistoryTable / MetadataLogEntriesTable (via MetadataTableUtils +
  // asDataTask().rows()) and emits them as java_history.json / java_metadata_log_entries.json. The Rust
  // test asserts `table.inspect().history()/.metadata_log_entries().scan()` is field-for-field equal.
  //
  // The two NON-TRIVIAL derived columns drive the whole fixture:
  //   * history.is_current_ancestor — true iff a snapshot-log entry's snapshot is in the parent chain
  //     walked from the CURRENT snapshot (Java SnapshotUtil.currentAncestorIds). To make it FALSE for a
  //     row, the snapshot LOG must contain a snapshot off the current ancestry — a FORKED log entry.
  //   * metadata_log_entries.latest_{snapshot_id,schema_id,sequence_number} — resolve to the snapshot that
  //     was current AT each metadata-log entry's timestamp = the last snapshot-log entry with
  //     made_current_at <= ts (Java SnapshotUtil.snapshotIdAsOfTime); NULL when no snapshot-log entry is
  //     at/older than the timestamp (Java throws IllegalArgumentException, caught). To exercise
  //     NULL / a-middle-snapshot / CURRENT the metadata-log timestamps must STRADDLE the snapshot-log
  //     timestamps.
  //
  // FORKED snapshot-log construction. A single build that adds ROOT/SIBLING/CURRENT and points main at
  // each would treat ROOT/SIBLING as INTERMEDIATE (added AND set-to-main AND no-longer-current within one
  // build's `changes`) and prune them from the log. So the log is built via THREE SEPARATE builds,
  // RE-PARSING between each (re-parsing clears `changes`, so a prior snapshot counts as already-persisted
  // and is not intermediate). Because each snapshot is added in the SAME build that points main at it,
  // `isAddedSnapshot` is true and the snapshot-LOG entry timestamp is the snapshot's OWN timestampMillis
  // (deterministic — no rollback, so no System.currentTimeMillis leaks into the log). The snapshot
  // timestamps are ascending (ROOT 2018-01, SIBLING 2018-08, CURRENT 2019-04) so the log is ascending and
  // the build's last-entry-is-current invariant holds. Result log =
  // [(2018-01,ROOT),(2018-08,SIBLING),(2019-04,CURRENT)], current=CURRENT, ancestry={CURRENT,ROOT} =>
  // is_current_ancestor: ROOT true, SIBLING FALSE, CURRENT true.
  // ===========================================================================================

  /**
   * The inspection-log half of the oracle. Builds the shared forked base via the 3-commit re-parse recipe,
   * INJECTS a deterministic metadata-log that straddles the snapshot timestamps (real commits would stamp
   * ~now timestamps; injection is the Java analog of the Rust unit test's `meta.metadata_log = vec![...]`,
   * and Java's REAL MetadataLogEntriesTable still computes latest_* over it), re-parses with a STABLE
   * LOGICAL LOCATION (so the synthetic current entry's `file` column is portable), and emits the rows of
   * Java's REAL {@link HistoryTable} / {@link MetadataLogEntriesTable}.
   */
  static final class InspectionLogOracle {
    private InspectionLogOracle() {}

    private static final String LOCATION = "s3://interop-bucket/inspection_history";
    // The stable LOGICAL metadata location the base is re-parsed with — NOT the on-disk path. The
    // synthetic metadata-log entry's `file` column = metadataFileLocation(), so it must be portable; the
    // Rust test builds its Table with exactly this `.metadata_location(...)`.
    private static final String STABLE_LOCATION = LOCATION + "/metadata/v1.metadata.json";

    // Snapshot graph (reuse the prior ids). NOTE SIBLING's ts sits BETWEEN root and current so the
    // snapshot log is ascending and the forked SIBLING is genuinely off main's ancestry.
    static final long ROOT_ID = 3051729675574597004L;
    static final long CURRENT_ID = 3055729675574597004L;
    static final long SIBLING_ID = 3060729675574597004L;
    static final long ROOT_TS_MS = 1515100955770L; // 2018-01
    static final long SIBLING_TS_MS = 1535000000000L; // 2018-08 (between ROOT and CURRENT)
    static final long CURRENT_TS_MS = 1555100955770L; // 2019-04

    static void generate(Path dir) throws IOException {
      Files.createDirectories(dir);

      // 1. Build the FORKED snapshot-log base via THREE separate builds, RE-PARSING between each so a
      //    prior snapshot is NOT treated as intermediate (which would prune it from the log).
      Schema schema =
          new Schema(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "data", Types.StringType.get()));
      Map<String, String> props = new LinkedHashMap<>();
      props.put(TableProperties.FORMAT_VERSION, "2");
      TableMetadata seed =
          TableMetadata.newTableMetadata(
              schema, PartitionSpec.unpartitioned(), SortOrder.unsorted(), LOCATION, props);
      int schemaId = seed.currentSchemaId();

      Snapshot root = snapshot(ROOT_ID, null, 1L, ROOT_TS_MS, schemaId);
      Snapshot sibling = snapshot(SIBLING_ID, ROOT_ID, 2L, SIBLING_TS_MS, schemaId);
      Snapshot current = snapshot(CURRENT_ID, ROOT_ID, 3L, CURRENT_TS_MS, schemaId);

      // B0: add ROOT, point main at ROOT -> reparse.
      TableMetadata b0 =
          reparse(
              TableMetadata.buildFrom(seed)
                  .addSnapshot(root)
                  .setBranchSnapshot(ROOT_ID, SnapshotRef.MAIN_BRANCH)
                  .build());
      // B1: add SIBLING, point main at SIBLING -> reparse.
      TableMetadata b1 =
          reparse(
              TableMetadata.buildFrom(b0)
                  .addSnapshot(sibling)
                  .setBranchSnapshot(SIBLING_ID, SnapshotRef.MAIN_BRANCH)
                  .build());
      // B2: add CURRENT, point main at CURRENT -> reparse. Final log = [ROOT, SIBLING, CURRENT].
      TableMetadata b2 =
          reparse(
              TableMetadata.buildFrom(b1)
                  .addSnapshot(current)
                  .setBranchSnapshot(CURRENT_ID, SnapshotRef.MAIN_BRANCH)
                  .build());

      // 2. INJECT the deterministic metadata-log (three entries that STRADDLE the snapshot timestamps).
      //    Real commits would stamp ~now timestamps; injecting straight into the metadata JSON is the
      //    Java analog of the Rust unit test setting `meta.metadata_log` directly.
      String json = TableMetadataParser.toJson(b2);
      ObjectNode node = (ObjectNode) JsonUtil.mapper().readTree(json);
      ArrayNode metadataLog = JsonUtil.mapper().createArrayNode();
      metadataLog.add(metadataLogEntry(ROOT_TS_MS - 1000, "00000-creation.metadata.json"));
      metadataLog.add(metadataLogEntry(ROOT_TS_MS + 1000, "00001-after-root.metadata.json"));
      metadataLog.add(metadataLogEntry(SIBLING_TS_MS + 1000, "00002-after-sibling.metadata.json"));
      node.set("metadata-log", metadataLog);
      String injected = JsonUtil.mapper().writerWithDefaultPrettyPrinter().writeValueAsString(node);

      // Write the injected base to disk (this is the byte-for-byte fixture the Rust test loads).
      Path basePath = dir.resolve("base.metadata.json");
      writeJson(basePath, injected);

      // 3. RE-PARSE with the STABLE LOGICAL location (NOT basePath) so metadataFileLocation() — the
      //    synthetic current entry's `file` column — is portable and equals what the Rust test builds.
      TableMetadata reparsed = TableMetadataParser.fromJson(STABLE_LOCATION, injected);

      // 4. Build a BaseTable over in-memory ops whose io() resolves the STABLE location to the injected
      //    bytes (HistoryTable.task / MetadataLogEntriesTable.task call io().newInputFile(...) — the file is
      //    only HELD by StaticDataTask, never read for these pure-metadata tables).
      InMemoryFileIO io = new InMemoryFileIO();
      io.addFile(STABLE_LOCATION, injected.getBytes(StandardCharsets.UTF_8));
      BaseTable baseTable =
          new BaseTable(new InMemoryInspectionOperations(reparsed, io), "interop_inspection_history");

      // 5. Materialize + emit the rows of Java's REAL HistoryTable and MetadataLogEntriesTable.
      writeJson(
          dir.resolve("java_history.json"),
          rowsToJson(baseTable, MetadataTableType.HISTORY, InspectionLogOracle::historyRowToJson));
      writeJson(
          dir.resolve("java_metadata_log_entries.json"),
          rowsToJson(
              baseTable,
              MetadataTableType.METADATA_LOG_ENTRIES,
              InspectionLogOracle::metadataLogRowToJson));
      System.out.println("generated inspection-log fixtures to " + dir);
    }

    /** Round-trip a built {@link TableMetadata} through JSON to clear pending `changes` (re-parse). */
    private static TableMetadata reparse(TableMetadata metadata) {
      return TableMetadataParser.fromJson(STABLE_LOCATION, TableMetadataParser.toJson(metadata));
    }

    /** One injected metadata-log entry — keys EXACTLY `timestamp-ms` + `metadata-file`. */
    private static ObjectNode metadataLogEntry(long timestampMs, String fileName) {
      ObjectNode entry = JsonUtil.mapper().createObjectNode();
      entry.put("timestamp-ms", timestampMs);
      entry.put("metadata-file", LOCATION + "/metadata/" + fileName);
      return entry;
    }

    /** Construct a package-private {@link BaseSnapshot} with a fake (never-read) manifest-list path. */
    private static Snapshot snapshot(
        long snapshotId, Long parentId, long sequenceNumber, long timestampMs, int schemaId) {
      Map<String, String> summary = new LinkedHashMap<>();
      summary.put("operation", DataOperations.APPEND);
      return new BaseSnapshot(
          sequenceNumber,
          snapshotId,
          parentId,
          timestampMs,
          DataOperations.APPEND,
          summary,
          schemaId,
          LOCATION + "/metadata/snap-" + snapshotId + ".avro",
          null,
          null,
          null);
    }

    /**
     * Materialize the rows of Java's REAL metadata table of {@code type} (HistoryTable /
     * MetadataLogEntriesTable) via {@link MetadataTableUtils#createMetadataTableInstance} +
     * {@code task.asDataTask().rows()} and serialize each {@link StructLike} row with {@code rowToJson}.
     * Columns are read BY POSITION per the metadata table's own schema.
     *
     * <p>IMPORTANT: each row MUST be serialized EAGERLY inside the iteration — {@code StaticDataTask.rows()}
     * is a lazy transform over ONE mutable projection, so stashing the {@link StructLike} references would
     * yield the LAST row repeated N times (this bit the prior increment).
     */
    private static String rowsToJson(
        BaseTable baseTable, MetadataTableType type, InspectionOracle.RowWriter rowToJson) {
      Table mt = MetadataTableUtils.createMetadataTableInstance(baseTable, type);
      return JsonUtil.generate(
          gen -> {
            gen.writeStartArray();
            try (CloseableIterable<FileScanTask> tasks = mt.newScan().planFiles()) {
              for (FileScanTask task : tasks) {
                try (CloseableIterable<StructLike> taskRows = task.asDataTask().rows()) {
                  for (StructLike row : taskRows) {
                    rowToJson.write(row, gen);
                  }
                }
              }
            }
            gen.writeEndArray();
          },
          true);
    }

    /**
     * Serialize one HistoryTable row by position (HISTORY_SCHEMA): 0=made_current_at micros Long,
     * 1=snapshot_id Long, 2=parent_id Long-or-null, 3=is_current_ancestor Boolean.
     */
    private static void historyRowToJson(StructLike row, JsonGenerator gen) throws IOException {
      gen.writeStartObject();
      gen.writeNumberField("made_current_at", row.get(0, Long.class));
      gen.writeNumberField("snapshot_id", row.get(1, Long.class));
      writeLongOrNull(gen, "parent_id", row.get(2, Long.class));
      gen.writeBooleanField("is_current_ancestor", row.get(3, Boolean.class));
      gen.writeEndObject();
    }

    /**
     * Serialize one MetadataLogEntriesTable row by position (METADATA_LOG_ENTRIES_SCHEMA): 0=timestamp
     * micros Long, 1=file String, 2=latest_snapshot_id Long-or-null, 3=latest_schema_id Integer-or-null,
     * 4=latest_sequence_number Long-or-null.
     */
    private static void metadataLogRowToJson(StructLike row, JsonGenerator gen) throws IOException {
      gen.writeStartObject();
      gen.writeNumberField("timestamp", row.get(0, Long.class));
      gen.writeStringField("file", row.get(1, String.class));
      writeLongOrNull(gen, "latest_snapshot_id", row.get(2, Long.class));
      writeIntOrNull(gen, "latest_schema_id", row.get(3, Integer.class));
      writeLongOrNull(gen, "latest_sequence_number", row.get(4, Long.class));
      gen.writeEndObject();
    }

    private static void writeLongOrNull(JsonGenerator gen, String field, Long value)
        throws IOException {
      if (value == null) {
        gen.writeNullField(field);
      } else {
        gen.writeNumberField(field, value);
      }
    }

    private static void writeIntOrNull(JsonGenerator gen, String field, Integer value)
        throws IOException {
      if (value == null) {
        gen.writeNullField(field);
      } else {
        gen.writeNumberField(field, value.intValue());
      }
    }
  }

  /**
   * A minimal in-memory {@link TableOperations} for the inspection oracle. Unlike the manage-snapshots
   * {@link InMemoryTableOperations}, this one's {@code io()} returns a real {@link InMemoryFileIO} (with the
   * metadata file pre-added) because {@link SnapshotsTable#task}/{@link RefsTable#task} call
   * {@code io().newInputFile(metadataFileLocation())} to build the {@link org.apache.iceberg.io.InputFile}
   * that {@code StaticDataTask} merely HOLDS. The metadata is read-only here (no commit path is exercised).
   */
  private static final class InMemoryInspectionOperations implements TableOperations {
    private final TableMetadata current;
    private final FileIO io;

    InMemoryInspectionOperations(TableMetadata current, FileIO io) {
      this.current = current;
      this.io = io;
    }

    @Override
    public TableMetadata current() {
      return current;
    }

    @Override
    public TableMetadata refresh() {
      return current;
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      throw new UnsupportedOperationException("inspection oracle is read-only");
    }

    @Override
    public FileIO io() {
      return io;
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return current.location() + "/metadata/" + fileName;
    }

    @Override
    public LocationProvider locationProvider() {
      throw new UnsupportedOperationException("inspection oracle does not provide data locations");
    }
  }

  // ===========================================================================================
  // Inspection-MANIFESTS oracle — the FIRST manifest-READING inspection increment. Unlike the three
  // pure-metadata oracles above (which read rows out of an InMemoryFileIO holding only the metadata JSON),
  // this oracle WRITES A REAL TABLE to LOCAL DISK: real commits (newAppend + newRowDelta) write AVRO data /
  // delete manifests + a manifest-list + a metadata.json under <dir>/table/metadata via
  // org.apache.iceberg.Files.localOutput. The metadata-table rows are then materialized the SAME way as the
  // pure-metadata tables — MetadataTableUtils.createMetadataTableInstance(...) + asDataTask().rows() — but
  // each ManifestReadTask now opens the ON-DISK manifest via the LocalFileIO, so the emitted rows are read
  // from the real AVRO the Rust test also reads.
  //
  // SCOPE (A1): the content-filtered FILES / DATA_FILES / DELETE_FILES tables. Every column is covered
  // EXCEPT the trailing virtual `readable_metrics` STRUCT — it is DERIVED (per-leaf-column human-readable
  // min/max/counts) and its interior field ordering depends on a JVM HashMap iteration order, a documented
  // divergence that is OUT OF SCOPE for A1 (the raw metric MAPS + bound MAPS this oracle emits are the
  // load-bearing source those readable values are derived from). `entries` / `manifests` / `partitions` /
  // `all_*` and scan interop are deferred to later increments.
  //
  // The referenced .parquet data/delete paths need NOT exist on disk: the files tables read the MANIFEST
  // entries, never the parquet — so PURE-METADATA DataFiles / FileMetadata builders are enough.
  // ===========================================================================================

  /**
   * The inspection-manifests half of the oracle. Builds a partitioned V2 table on local disk via real
   * commits (so real AVRO manifests + a manifest-list land under {@code <dir>/table/metadata}), writes the
   * final metadata to a deterministic {@code <dir>/table/metadata/final.metadata.json}, and emits the rows
   * of Java's REAL {@link FilesTable} / {@link DataFilesTable} / {@link DeleteFilesTable} (via {@link
   * MetadataTableUtils} + {@code asDataTask().rows()} reading those on-disk manifests) as
   * {@code java_files.json} / {@code java_data_files.json} / {@code java_delete_files.json}.
   */
  static final class InspectionManifestsOracle {
    private InspectionManifestsOracle() {}

    static void generate(Path dir) throws IOException {
      Files.createDirectories(dir);

      // 1. Build a partitioned V2 table on local disk. Location is the BARE absolute path of <dir>/table
      //    (org.apache.iceberg.Files.localOutput.location() returns a bare path, no `file:` scheme, so the
      //    manifest/manifest-list paths the commits write are bare absolute paths the Rust FileIO resolves
      //    directly).
      File tableDir = dir.resolve("table").toFile();
      File metadataDir = new File(tableDir, "metadata");
      if (!metadataDir.isDirectory() && !metadataDir.mkdirs()) {
        throw new IOException("failed to create metadata dir at " + metadataDir);
      }

      Schema schema =
          new Schema(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "category", Types.StringType.get()),
              Types.NestedField.optional(3, "value", Types.DoubleType.get()));
      PartitionSpec spec = PartitionSpec.builderFor(schema).identity("category").build();

      Map<String, String> props = new LinkedHashMap<>();
      props.put(TableProperties.FORMAT_VERSION, "2");
      TableMetadata seed =
          TableMetadata.newTableMetadata(
              schema,
              spec,
              SortOrder.unsorted(),
              tableDir.getAbsolutePath(),
              props);

      LocalTableOperations ops = new LocalTableOperations(tableDir, metadataDir);
      ops.commit(null, seed); // persist v0 metadata so the BaseTable has a current metadata to evolve.
      BaseTable table = new BaseTable(ops, "interop_inspection_manifests");

      // 2. Two DATA files (one per partition), WITH metrics (column sizes + value/null counts for ids 1/2/3
      //    and lower/upper bounds for id (long) and value (double)). The referenced .parquet paths are pure
      //    metadata — they need not exist; the files tables read only the manifest entries.
      DataFile dataFileA =
          DataFiles.builder(spec)
              .withPath(tableDir.getAbsolutePath() + "/data/category=a/00000-a.parquet")
              .withFileSizeInBytes(1100L)
              .withRecordCount(3L)
              .withPartitionPath("category=a")
              .withMetrics(metricsFor(3L, 1L, 3L, 10.5d, 30.5d))
              .build();
      DataFile dataFileB =
          DataFiles.builder(spec)
              .withPath(tableDir.getAbsolutePath() + "/data/category=b/00000-b.parquet")
              .withFileSizeInBytes(900L)
              .withRecordCount(2L)
              .withPartitionPath("category=b")
              .withMetrics(metricsFor(2L, 4L, 5L, 40.5d, 50.5d))
              .build();

      // 3. One POSITION-DELETE file in category=a (record_count 1).
      DeleteFile deleteFileA =
          FileMetadata.deleteFileBuilder(spec)
              .ofPositionDeletes()
              .withPath(tableDir.getAbsolutePath() + "/data/category=a/00000-a-deletes.parquet")
              .withFileSizeInBytes(150L)
              .withRecordCount(1L)
              .withPartitionPath("category=a")
              .build();

      // 4. Real commits: the two data files via newAppend (writes a DATA manifest + manifest-list), then the
      //    delete via newRowDelta (writes a DELETE manifest).
      table.newAppend().appendFile(dataFileA).appendFile(dataFileB).commit();
      table.newRowDelta().addDeletes(deleteFileA).commit();

      // 5. Write the FINAL metadata to a KNOWN path so the Rust test loads it deterministically. (The real
      //    on-disk manifest-list + manifests already live under <dir>/table/metadata/.)
      Path finalMetadata = metadataDir.toPath().resolve("final.metadata.json");
      OutputFile finalOut =
          new LocalFileIO().newOutputFile(finalMetadata.toAbsolutePath().toString());
      TableMetadataParser.write(ops.current(), finalOut);

      // 6. Materialize + emit the rows of Java's REAL FilesTable / DataFilesTable / DeleteFilesTable.
      writeJson(dir.resolve("java_files.json"), rowsToJson(table, MetadataTableType.FILES));
      writeJson(dir.resolve("java_data_files.json"), rowsToJson(table, MetadataTableType.DATA_FILES));
      writeJson(
          dir.resolve("java_delete_files.json"), rowsToJson(table, MetadataTableType.DELETE_FILES));
      System.out.println("generated inspection-manifests table + fixtures to " + dir);
    }

    /**
     * Build a pure-metadata {@link Metrics} for the table schema {1 id long, 2 category string, 3 value
     * double}: column_sizes + value_counts + null_value_counts for ids 1/2/3, and lower/upper bounds for id
     * 1 (long) and id 3 (value, double). category (id 2) is excluded from bounds on purpose (a string
     * column with no bound here), so the bound MAPS are non-trivial subsets of the count MAPS.
     */
    private static Metrics metricsFor(
        long recordCount, long idLower, long idUpper, double valueLower, double valueUpper) {
      Map<Integer, Long> columnSizes = new LinkedHashMap<>();
      columnSizes.put(1, 40L);
      columnSizes.put(2, 24L);
      columnSizes.put(3, 32L);
      Map<Integer, Long> valueCounts = new LinkedHashMap<>();
      valueCounts.put(1, recordCount);
      valueCounts.put(2, recordCount);
      valueCounts.put(3, recordCount);
      Map<Integer, Long> nullValueCounts = new LinkedHashMap<>();
      nullValueCounts.put(1, 0L);
      nullValueCounts.put(2, 0L);
      nullValueCounts.put(3, 0L);
      Map<Integer, ByteBuffer> lowerBounds = new LinkedHashMap<>();
      lowerBounds.put(1, Conversions.toByteBuffer(Types.LongType.get(), idLower));
      lowerBounds.put(3, Conversions.toByteBuffer(Types.DoubleType.get(), valueLower));
      Map<Integer, ByteBuffer> upperBounds = new LinkedHashMap<>();
      upperBounds.put(1, Conversions.toByteBuffer(Types.LongType.get(), idUpper));
      upperBounds.put(3, Conversions.toByteBuffer(Types.DoubleType.get(), valueUpper));
      return new Metrics(
          recordCount,
          columnSizes,
          valueCounts,
          nullValueCounts,
          null, // nan_value_counts — none (exercises a NULL/absent map column)
          lowerBounds,
          upperBounds);
    }

    /**
     * Materialize the rows of Java's REAL files metadata table of {@code type} (FilesTable / DataFilesTable
     * / DeleteFilesTable) via {@link MetadataTableUtils#createMetadataTableInstance} +
     * {@code task.asDataTask().rows()} — each {@link BaseFilesTable.ManifestReadTask} opens the ON-DISK
     * AVRO manifest through the table's LocalFileIO — and serialize each row keyed BY COLUMN NAME (derived
     * from {@code mt.schema().columns()}; NEVER hardcode positions). EVERY column is emitted EXCEPT the
     * trailing virtual {@code readable_metrics} struct (deferred for A1).
     *
     * <p>IMPORTANT: each row MUST be serialized EAGERLY inside the iteration — {@code StaticDataTask}-style
     * {@code rows()} reuse a single mutable projection per task, so stashing the {@link StructLike}
     * references would yield the LAST row repeated. (Here the files tables iterate real manifest entries,
     * but the eager-serialize discipline is kept identical to the pure-metadata oracles.)
     */
    private static String rowsToJson(BaseTable baseTable, MetadataTableType type) {
      Table mt = MetadataTableUtils.createMetadataTableInstance(baseTable, type);
      List<Types.NestedField> columns = mt.schema().columns();
      return JsonUtil.generate(
          gen -> {
            gen.writeStartArray();
            try (CloseableIterable<FileScanTask> tasks = mt.newScan().planFiles()) {
              for (FileScanTask task : tasks) {
                try (CloseableIterable<StructLike> taskRows = task.asDataTask().rows()) {
                  for (StructLike row : taskRows) {
                    fileRowToJson(row, columns, gen);
                  }
                }
              }
            }
            gen.writeEndArray();
          },
          true);
    }

    /**
     * Serialize one files-table row keyed by COLUMN NAME, covering every column EXCEPT
     * {@code readable_metrics}. Scalars verbatim; {@code partition} as a nested object of its sub-field
     * values; the count MAPS ({@code column_sizes} / {@code value_counts} / {@code null_value_counts} /
     * {@code nan_value_counts}) as {field_id: long}; {@code lower_bounds} / {@code upper_bounds} as
     * {field_id: hex-of-bytes}; list columns ({@code split_offsets} / {@code equality_ids}) as JSON arrays;
     * nulls as JSON null.
     */
    @SuppressWarnings("unchecked")
    private static void fileRowToJson(
        StructLike row, List<Types.NestedField> columns, JsonGenerator gen) throws IOException {
      gen.writeStartObject();
      for (int i = 0; i < columns.size(); i++) {
        Types.NestedField column = columns.get(i);
        String name = column.name();
        if (MetricsUtil.READABLE_METRICS.equals(name)) {
          continue; // DEFER readable_metrics for A1 (see class comment).
        }
        Object value = row.get(i, Object.class);
        if (value == null) {
          gen.writeNullField(name);
          continue;
        }
        switch (name) {
          case "partition":
            writePartition(gen, name, (StructLike) value, column.type().asStructType());
            break;
          case "column_sizes":
          case "value_counts":
          case "null_value_counts":
          case "nan_value_counts":
            writeLongMap(gen, name, (Map<Integer, Long>) value);
            break;
          case "lower_bounds":
          case "upper_bounds":
            writeBytesMap(gen, name, (Map<Integer, ByteBuffer>) value);
            break;
          case "split_offsets":
            writeLongList(gen, name, (List<Long>) value);
            break;
          case "equality_ids":
            writeIntList(gen, name, (List<Integer>) value);
            break;
          default:
            writeScalar(gen, name, value);
        }
      }
      gen.writeEndObject();
    }

    /** A scalar column — number / boolean / string / CharSequence — written verbatim. */
    private static void writeScalar(JsonGenerator gen, String name, Object value) throws IOException {
      if (value instanceof Integer) {
        gen.writeNumberField(name, (Integer) value);
      } else if (value instanceof Long) {
        gen.writeNumberField(name, (Long) value);
      } else if (value instanceof Boolean) {
        gen.writeBooleanField(name, (Boolean) value);
      } else if (value instanceof ByteBuffer) {
        gen.writeStringField(name, toHex((ByteBuffer) value));
      } else {
        // file_path / file_format are CharSequence-y; toString() yields the underlying string.
        gen.writeStringField(name, value.toString());
      }
    }

    /** The {@code partition} struct as a nested object keyed by sub-field name (identity(category)). */
    private static void writePartition(
        JsonGenerator gen, String name, StructLike partition, Types.StructType type)
        throws IOException {
      gen.writeObjectFieldStart(name);
      List<Types.NestedField> fields = type.fields();
      for (int i = 0; i < fields.size(); i++) {
        Types.NestedField field = fields.get(i);
        Object value = partition.get(i, Object.class);
        if (value == null) {
          gen.writeNullField(field.name());
        } else {
          writeScalar(gen, field.name(), value);
        }
      }
      gen.writeEndObject();
    }

    /** A metric count MAP {field_id: long}. */
    private static void writeLongMap(JsonGenerator gen, String name, Map<Integer, Long> map)
        throws IOException {
      gen.writeObjectFieldStart(name);
      for (Map.Entry<Integer, Long> entry : map.entrySet()) {
        gen.writeNumberField(Integer.toString(entry.getKey()), entry.getValue());
      }
      gen.writeEndObject();
    }

    /** A bound MAP {field_id: hex-of-bytes} (lowercase hex of the raw ByteBuffer single-value bytes). */
    private static void writeBytesMap(JsonGenerator gen, String name, Map<Integer, ByteBuffer> map)
        throws IOException {
      gen.writeObjectFieldStart(name);
      for (Map.Entry<Integer, ByteBuffer> entry : map.entrySet()) {
        gen.writeStringField(Integer.toString(entry.getKey()), toHex(entry.getValue()));
      }
      gen.writeEndObject();
    }

    /** A {@code split_offsets} list as a JSON array of longs. */
    private static void writeLongList(JsonGenerator gen, String name, List<Long> list)
        throws IOException {
      gen.writeArrayFieldStart(name);
      for (Long element : list) {
        gen.writeNumber(element);
      }
      gen.writeEndArray();
    }

    /** An {@code equality_ids} list as a JSON array of ints. */
    private static void writeIntList(JsonGenerator gen, String name, List<Integer> list)
        throws IOException {
      gen.writeArrayFieldStart(name);
      for (Integer element : list) {
        gen.writeNumber(element);
      }
      gen.writeEndArray();
    }

    /** Lowercase hex of a {@link ByteBuffer}'s remaining bytes (does NOT consume the buffer). */
    private static String toHex(ByteBuffer buffer) {
      ByteBuffer dup = buffer.duplicate();
      StringBuilder hex = new StringBuilder(dup.remaining() * 2);
      while (dup.hasRemaining()) {
        hex.append(String.format("%02x", dup.get() & 0xff));
      }
      return hex.toString();
    }
  }

  // ===========================================================================================
  // Inspection-MANIFESTS A2 oracle — the SECOND manifest-READING inspection increment, building DIRECTLY
  // on A1's harness (it reuses A1's LocalTableOperations + LocalFileIO + writeJson + the same
  // MetadataTableUtils + planFiles() + asDataTask().rows() materializer). A1 proved the content-filtered
  // FILES / DATA_FILES / DELETE_FILES tables; A2 proves the THREE manifest-reading tables that need a
  // RICHER table than A1's:
  //
  //   * ENTRIES (Java ManifestEntriesTable) — one row per manifest ENTRY of the current snapshot's
  //     manifests, INCLUDING DELETED tombstones (status 2) that the `files` family excludes. Columns:
  //     status(int), snapshot_id, sequence_number, file_sequence_number, data_file(NESTED struct = the SAME
  //     DataFile projection A1 used). REQUIRES a DELETED tombstone in the current snapshot's manifests → the
  //     A2 table deletes a data file (newDelete) as its last commit.
  //   * MANIFESTS (Java ManifestsTable) — one row per manifest in the CURRENT snapshot's manifest list:
  //     content, path, length, partition_spec_id, added_snapshot_id, the six *_data/delete_files_count
  //     (CONTENT-GATED: a DATA manifest carries data counts + 0 delete counts, a DELETE manifest the
  //     reverse), partition_summaries (list<struct: contains_null, contains_nan, lower_bound STRING,
  //     upper_bound STRING>). REQUIRES ≥1 DATA manifest AND ≥1 DELETE manifest, and a PARTITIONED spec so
  //     the summaries are non-empty.
  //   * PARTITIONS (Java PartitionsTable) — one row per partition value over the CURRENT snapshot's LIVE
  //     entries: partition(struct), spec_id, record_count, file_count, total_data_file_size_in_bytes, the
  //     position/equality delete-count columns, last_updated_at(micros), last_updated_snapshot_id. REQUIRES
  //     ≥2 partitions, one carrying BOTH data files and a position-delete (so the delete-count columns are
  //     non-zero).
  //
  // THE A2 TABLE (its OWN subdir <dir>/table_a2 + its OWN final.metadata.json — A1's <dir>/table is
  // untouched). Partition by identity(category), V2:
  //   snapshot 1 (newAppend): data A(category=a, metrics+bounds), B(category=b), C(category=a), D(category=b)
  //     -> 2 partitions; cat=a has 2 data files (A, C), cat=b has 2 (B, D).
  //   snapshot 2 (newRowDelta): add a POSITION-DELETE for category=a
  //     -> a DELETE manifest in the manifest list; cat=a gets non-zero position_delete_* counts.
  //   snapshot 3 (newDelete): deleteFile(B) (category=b)
  //     -> B becomes a DELETED tombstone (status 2) in the rewritten DATA manifest, for `entries`. D KEEPS
  //        category=b ALIVE in `partitions` (Java's PartitionsTable only lists partitions with live
  //        entries — deleting the ONLY cat=b file would drop the partition, so D is the live survivor that
  //        guarantees the required ≥2 partition rows).
  // The EXACT row values are whatever Java materializes (Java is the oracle; Rust must match) — the table
  // only needs to HIT these cases, which the design above guarantees.
  //
  // The referenced .parquet data/delete paths need NOT exist on disk: the three tables read the MANIFEST
  // entries (and, for `manifests`, the manifest-list), never the parquet — so PURE-METADATA DataFiles /
  // FileMetadata builders are enough, exactly as A1.
  // ===========================================================================================

  /**
   * The A2 (+ A3) half of the inspection-manifests oracle. Builds the richer partitioned V2 table on local
   * disk under {@code <dir>/table_a2} via THREE real commits (newAppend + newRowDelta + newDelete), writes
   * {@code <dir>/table_a2/metadata/final.metadata.json}, and emits the rows of Java's REAL {@link
   * ManifestEntriesTable} / {@link ManifestsTable} / {@link PartitionsTable} (A2) as
   * {@code java_entries.json} / {@code java_manifests.json} / {@code java_partitions.json}, AND — over the
   * SAME table — Java's REAL {@code AllDataFilesTable} / {@code AllDeleteFilesTable} / {@code AllFilesTable}
   * / {@code AllEntriesTable} / {@code AllManifestsTable} (A3, the cross-snapshot {@code all_*} tables) as
   * {@code java_all_data_files.json} / {@code java_all_delete_files.json} / {@code java_all_files.json} /
   * {@code java_all_entries.json} / {@code java_all_manifests.json}. All materialize via {@link
   * MetadataTableUtils} + {@code asDataTask().rows()} reading the same on-disk manifests.
   */
  static final class InspectionManifestsA2Oracle {
    private InspectionManifestsA2Oracle() {}

    static void generate(Path dir) throws IOException {
      Files.createDirectories(dir);

      // 1. Build the richer partitioned V2 table on local disk under <dir>/table_a2 (A1's <dir>/table is
      //    untouched). The location is the BARE absolute path so the manifest/manifest-list paths the
      //    commits write are bare absolute paths the Rust FileIO resolves directly (same as A1).
      File tableDir = dir.resolve("table_a2").toFile();
      File metadataDir = new File(tableDir, "metadata");
      if (!metadataDir.isDirectory() && !metadataDir.mkdirs()) {
        throw new IOException("failed to create metadata dir at " + metadataDir);
      }

      Schema schema =
          new Schema(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "category", Types.StringType.get()),
              Types.NestedField.optional(3, "value", Types.DoubleType.get()));
      PartitionSpec spec = PartitionSpec.builderFor(schema).identity("category").build();

      Map<String, String> props = new LinkedHashMap<>();
      props.put(TableProperties.FORMAT_VERSION, "2");
      TableMetadata seed =
          TableMetadata.newTableMetadata(
              schema, spec, SortOrder.unsorted(), tableDir.getAbsolutePath(), props);

      LocalTableOperations ops = new LocalTableOperations(tableDir, metadataDir);
      ops.commit(null, seed); // persist v0 metadata so the BaseTable has a current metadata to evolve.
      BaseTable table = new BaseTable(ops, "interop_inspection_manifests_a2");

      // 2. FOUR DATA files: A + C in category=a, B + D in category=b. Each WITH metrics (column sizes +
      //    value/null counts for ids 1/2/3 and lower/upper bounds for id (long) and value (double)) so the
      //    `partitions` size/record rollups and the `manifests` partition_summaries are non-trivial.
      DataFile dataFileA =
          DataFiles.builder(spec)
              .withPath(tableDir.getAbsolutePath() + "/data/category=a/00000-a.parquet")
              .withFileSizeInBytes(1100L)
              .withRecordCount(3L)
              .withPartitionPath("category=a")
              .withMetrics(metricsFor(3L, 1L, 3L, 10.5d, 30.5d))
              .build();
      DataFile dataFileB =
          DataFiles.builder(spec)
              .withPath(tableDir.getAbsolutePath() + "/data/category=b/00000-b.parquet")
              .withFileSizeInBytes(900L)
              .withRecordCount(2L)
              .withPartitionPath("category=b")
              .withMetrics(metricsFor(2L, 4L, 5L, 40.5d, 50.5d))
              .build();
      DataFile dataFileC =
          DataFiles.builder(spec)
              .withPath(tableDir.getAbsolutePath() + "/data/category=a/00001-c.parquet")
              .withFileSizeInBytes(1300L)
              .withRecordCount(4L)
              .withPartitionPath("category=a")
              .withMetrics(metricsFor(4L, 6L, 9L, 60.5d, 90.5d))
              .build();
      // D — the live cat=b survivor (B is deleted in s3; without D, cat=b would vanish from `partitions`).
      DataFile dataFileD =
          DataFiles.builder(spec)
              .withPath(tableDir.getAbsolutePath() + "/data/category=b/00001-d.parquet")
              .withFileSizeInBytes(700L)
              .withRecordCount(1L)
              .withPartitionPath("category=b")
              .withMetrics(metricsFor(1L, 7L, 7L, 70.5d, 70.5d))
              .build();

      // 3. One POSITION-DELETE file in category=a (record_count 2) — so cat=a carries BOTH data files and a
      //    position-delete, exercising the `partitions` delete-count columns + a DELETE manifest for
      //    `manifests`.
      DeleteFile deleteFileA =
          FileMetadata.deleteFileBuilder(spec)
              .ofPositionDeletes()
              .withPath(tableDir.getAbsolutePath() + "/data/category=a/00000-a-deletes.parquet")
              .withFileSizeInBytes(150L)
              .withRecordCount(2L)
              .withPartitionPath("category=a")
              .build();

      // 4. THREE real commits, each producing a snapshot:
      //    s1 newAppend(A, B, C, D)  -> a DATA manifest + manifest-list.
      //    s2 newRowDelta(+deleteFileA) -> a DELETE manifest (the manifest list now has a DATA + a DELETE
      //       manifest, so `manifests` is content-gated and `partitions` cat=a has position-delete counts).
      //    s3 newDelete(B) -> rewrites the DATA manifest, marking B as a DELETED tombstone (status 2) in
      //       the CURRENT snapshot's manifests, which `entries` surfaces (and `files`/`partitions` drop).
      //       D survives so cat=b is still a live partition.
      table
          .newAppend()
          .appendFile(dataFileA)
          .appendFile(dataFileB)
          .appendFile(dataFileC)
          .appendFile(dataFileD)
          .commit();
      table.newRowDelta().addDeletes(deleteFileA).commit();
      table.newDelete().deleteFile(dataFileB).commit();

      // 5. Write the FINAL metadata to a KNOWN path so the Rust test loads it deterministically. (The real
      //    on-disk manifest-list + manifests already live under <dir>/table_a2/metadata/.)
      Path finalMetadata = metadataDir.toPath().resolve("final.metadata.json");
      OutputFile finalOut =
          new LocalFileIO().newOutputFile(finalMetadata.toAbsolutePath().toString());
      TableMetadataParser.write(ops.current(), finalOut);

      // 6. Materialize + emit the rows of Java's REAL ManifestEntriesTable / ManifestsTable /
      //    PartitionsTable. Same materializer as A1: MetadataTableUtils + planFiles() + asDataTask().rows(),
      //    columns keyed BY NAME from mt.schema().columns(), eager per-row serialize.
      writeJson(dir.resolve("java_entries.json"), rowsToJson(table, MetadataTableType.ENTRIES));
      writeJson(dir.resolve("java_manifests.json"), rowsToJson(table, MetadataTableType.MANIFESTS));
      writeJson(dir.resolve("java_partitions.json"), rowsToJson(table, MetadataTableType.PARTITIONS));

      // 7. A3 — the FIVE cross-snapshot `all_*` inspection tables over the SAME table_a2 (its A2 fixtures
      //    above are UNTOUCHED). table_a2's three commits (s1 newAppend A,B,C,D -> manifest M1; s2 newRowDelta
      //    +pos-delete cat=a -> delete manifest MD, M1 CARRIED into s2's list; s3 newDelete B -> rewritten M1'
      //    where B is a DELETED tombstone) give the cross-snapshot shape these tables read:
      //      * ALL_DATA_FILES / ALL_FILES — the manifest SOURCE is the dedup-by-PATH union of manifests
      //        reachable from ALL snapshots (Java BaseAllMetadataTableScan.reachableManifests), so they
      //        INCLUDE B (live in s1's M1) which the CURRENT files/data_files tables EXCLUDE (current sees
      //        only M1' where B is deleted). Manifests are dedup'd by path but the FILES inside are NOT — a
      //        file present in two distinct reachable manifests (A in M1 and M1') appears MULTIPLE times
      //        (Java javadoc "may return duplicate rows"). Same flat files schema as A1, so the Rust test
      //        reuses the A1 FileRow extraction; the comparison is an order-independent MULTISET (no dedup).
      //      * ALL_ENTRIES — every manifest entry across all reachable manifests, incl. tombstones. Same
      //        nested-data_file schema as A2 `entries`.
      //      * ALL_MANIFESTS — one row per (manifest × referencing snapshot), NOT dedup'd: M1 referenced by
      //        BOTH s1 and s2 yields TWO rows with distinct reference_snapshot_id (its added_snapshot_id stays
      //        s1, so for the s2-referencing carried row reference_snapshot_id != added_snapshot_id). Its
      //        schema is the regular `manifests` schema PLUS a `reference_snapshot_id` Long column; the
      //        partition_summaries bounds render via Transform.toHumanString as bare strings, same as A2.
      //    All five materialize the SAME way (their planFiles tasks are DataTasks) via the shared rowsToJson.
      writeJson(
          dir.resolve("java_all_data_files.json"),
          rowsToJson(table, MetadataTableType.ALL_DATA_FILES));
      writeJson(
          dir.resolve("java_all_delete_files.json"),
          rowsToJson(table, MetadataTableType.ALL_DELETE_FILES));
      writeJson(dir.resolve("java_all_files.json"), rowsToJson(table, MetadataTableType.ALL_FILES));
      writeJson(
          dir.resolve("java_all_entries.json"), rowsToJson(table, MetadataTableType.ALL_ENTRIES));
      writeJson(
          dir.resolve("java_all_manifests.json"),
          rowsToJson(table, MetadataTableType.ALL_MANIFESTS));
      System.out.println("generated inspection-manifests A2 + A3 table + fixtures to " + dir);
    }

    /**
     * Build a pure-metadata {@link Metrics} for the table schema {1 id long, 2 category string, 3 value
     * double}: column_sizes + value_counts + null_value_counts for ids 1/2/3, and lower/upper bounds for id
     * 1 (long) and id 3 (value, double). category (id 2) is excluded from bounds on purpose. Identical to
     * A1's {@code metricsFor}.
     */
    private static Metrics metricsFor(
        long recordCount, long idLower, long idUpper, double valueLower, double valueUpper) {
      Map<Integer, Long> columnSizes = new LinkedHashMap<>();
      columnSizes.put(1, 40L);
      columnSizes.put(2, 24L);
      columnSizes.put(3, 32L);
      Map<Integer, Long> valueCounts = new LinkedHashMap<>();
      valueCounts.put(1, recordCount);
      valueCounts.put(2, recordCount);
      valueCounts.put(3, recordCount);
      Map<Integer, Long> nullValueCounts = new LinkedHashMap<>();
      nullValueCounts.put(1, 0L);
      nullValueCounts.put(2, 0L);
      nullValueCounts.put(3, 0L);
      Map<Integer, ByteBuffer> lowerBounds = new LinkedHashMap<>();
      lowerBounds.put(1, Conversions.toByteBuffer(Types.LongType.get(), idLower));
      lowerBounds.put(3, Conversions.toByteBuffer(Types.DoubleType.get(), valueLower));
      Map<Integer, ByteBuffer> upperBounds = new LinkedHashMap<>();
      upperBounds.put(1, Conversions.toByteBuffer(Types.LongType.get(), idUpper));
      upperBounds.put(3, Conversions.toByteBuffer(Types.DoubleType.get(), valueUpper));
      return new Metrics(
          recordCount,
          columnSizes,
          valueCounts,
          nullValueCounts,
          null, // nan_value_counts — none (exercises a NULL/absent map column)
          lowerBounds,
          upperBounds);
    }

    /**
     * Materialize the rows of Java's REAL metadata table of {@code type} (ManifestEntriesTable /
     * ManifestsTable / PartitionsTable) via {@link MetadataTableUtils#createMetadataTableInstance} +
     * {@code task.asDataTask().rows()} and serialize each row keyed BY COLUMN NAME (derived from
     * {@code mt.schema().columns()}; NEVER hardcode positions). Each {@code asDataTask().rows()} for the
     * entries/manifests/partitions tables reads the ON-DISK AVRO manifests + manifest-list through the
     * table's LocalFileIO.
     *
     * <p>IMPORTANT: each row MUST be serialized EAGERLY inside the iteration — {@code rows()} reuses a
     * single mutable projection per task, so stashing the {@link StructLike} references would yield the LAST
     * row repeated. (Same eager-serialize discipline as A1.)
     */
    private static String rowsToJson(BaseTable baseTable, MetadataTableType type) {
      Table mt = MetadataTableUtils.createMetadataTableInstance(baseTable, type);
      List<Types.NestedField> columns = mt.schema().columns();
      return JsonUtil.generate(
          gen -> {
            gen.writeStartArray();
            try (CloseableIterable<FileScanTask> tasks = mt.newScan().planFiles()) {
              for (FileScanTask task : tasks) {
                try (CloseableIterable<StructLike> taskRows = task.asDataTask().rows()) {
                  for (StructLike row : taskRows) {
                    gen.writeStartObject();
                    for (int i = 0; i < columns.size(); i++) {
                      Types.NestedField column = columns.get(i);
                      writeField(gen, column.name(), column.type(), row.get(i, Object.class));
                    }
                    gen.writeEndObject();
                  }
                }
              }
            }
            gen.writeEndArray();
          },
          true);
    }

    /**
     * Serialize ONE named field of an arbitrary Iceberg type to JSON — the generic counterpart to A1's
     * {@code fileRowToJson} (which special-cased the files table's flat columns). This walks the column's
     * Iceberg {@link Type} so it serializes the entries table's NESTED {@code data_file} STRUCT (the SAME
     * DataFile projection A1's files table flattened) AND the manifests table's
     * {@code partition_summaries} LIST&lt;STRUCT&gt; AND the partitions table's {@code partition} STRUCT —
     * all keyed by sub-field NAME, recursively. {@code readable_metrics} is DEFERRED exactly as A1 (its
     * interior ordering depends on a JVM HashMap iteration order).
     */
    private static void writeField(JsonGenerator gen, String name, Type type, Object value)
        throws IOException {
      if (MetricsUtil.READABLE_METRICS.equals(name)) {
        return; // DEFER readable_metrics (the entries table joins it as a top-level struct).
      }
      gen.writeFieldName(name);
      writeValue(gen, name, type, value);
    }

    /** Serialize a value of the given Iceberg {@link Type}, dispatching struct / list / map / scalar. */
    @SuppressWarnings("unchecked")
    private static void writeValue(JsonGenerator gen, String name, Type type, Object value)
        throws IOException {
      if (value == null) {
        gen.writeNull();
        return;
      }
      if (type.isStructType()) {
        writeStruct(gen, type.asStructType(), (StructLike) value);
      } else if (type.isListType()) {
        writeList(gen, type.asListType(), (List<?>) value);
      } else if (type.isMapType()) {
        // The metric/bound maps: {field_id: long} for the count maps, {field_id: hex} for the bound maps.
        // Java keys them by Integer field id; bound VALUES are ByteBuffer (hex), count VALUES are Long.
        gen.writeStartObject();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
          String key = entry.getKey().toString();
          Object element = entry.getValue();
          if (element instanceof ByteBuffer) {
            gen.writeStringField(key, toHex((ByteBuffer) element));
          } else if (element instanceof Long) {
            gen.writeNumberField(key, (Long) element);
          } else if (element instanceof Integer) {
            gen.writeNumberField(key, (Integer) element);
          } else {
            gen.writeStringField(key, element.toString());
          }
        }
        gen.writeEndObject();
      } else {
        writeScalarValue(gen, name, value);
      }
    }

    /** A STRUCT as a nested object keyed by sub-field NAME (recurses for nested structs/lists). */
    private static void writeStruct(JsonGenerator gen, Types.StructType type, StructLike struct)
        throws IOException {
      gen.writeStartObject();
      List<Types.NestedField> fields = type.fields();
      for (int i = 0; i < fields.size(); i++) {
        Types.NestedField field = fields.get(i);
        writeField(gen, field.name(), field.type(), struct.get(i, Object.class));
      }
      gen.writeEndObject();
    }

    /** A LIST as a JSON array, each element serialized per the element type (e.g. partition_summaries). */
    private static void writeList(JsonGenerator gen, Types.ListType type, List<?> list)
        throws IOException {
      gen.writeStartArray();
      Type elementType = type.elementType();
      for (Object element : list) {
        writeValue(gen, "element", elementType, element);
      }
      gen.writeEndArray();
    }

    /** A scalar — number / boolean / ByteBuffer(hex) / CharSequence — written verbatim (no field name). */
    private static void writeScalarValue(JsonGenerator gen, String name, Object value)
        throws IOException {
      if (value instanceof Integer) {
        gen.writeNumber((Integer) value);
      } else if (value instanceof Long) {
        gen.writeNumber((Long) value);
      } else if (value instanceof Float) {
        gen.writeNumber((Float) value);
      } else if (value instanceof Double) {
        gen.writeNumber((Double) value);
      } else if (value instanceof Boolean) {
        gen.writeBoolean((Boolean) value);
      } else if (value instanceof ByteBuffer) {
        gen.writeString(toHex((ByteBuffer) value));
      } else {
        // file_path / file_format / lower_bound / upper_bound are CharSequence-y; toString() yields the
        // underlying string. (manifests partition_summaries' lower/upper_bound are String in Java.)
        gen.writeString(value.toString());
      }
    }

    /** Lowercase hex of a {@link ByteBuffer}'s remaining bytes (does NOT consume the buffer). */
    private static String toHex(ByteBuffer buffer) {
      ByteBuffer dup = buffer.duplicate();
      StringBuilder hex = new StringBuilder(dup.remaining() * 2);
      while (dup.hasRemaining()) {
        hex.append(String.format("%02x", dup.get() & 0xff));
      }
      return hex.toString();
    }
  }

  // ===========================================================================================
  // Inspection-SCAN A4 oracle — the FIRST scan-PLANNING interop increment, building DIRECTLY on A1's
  // table-writing harness (it reuses A1/A2's LocalTableOperations + LocalFileIO + writeJson + real
  // newAppend / newRowDelta commits). A1-A3 proved the metadata TABLES (files / entries / manifests /
  // all_*); A4 proves SCAN PLANNING: for a given filter, does Rust plan the SAME data files Java does?
  //
  // Scan planning reads the AVRO manifests + applies the filter to PRUNE files via (a) partition
  // predicates and (b) column-metric (lower/upper bound) ranges, ASSOCIATES delete files with the surviving
  // data files, and computes the per-file RESIDUAL (the leftover row filter after the partition-implied
  // conditions are removed). It does NOT read parquet — so the SAME env-gated, no-parquet methodology as
  // A1-A3 applies (the referenced .parquet paths need not exist; planning reads manifests only).
  //
  // THE A4 TABLE (its OWN subdir <dir>/table_a4 + its OWN final.metadata.json — A1/A2's tables are
  // untouched). Partition by identity(category), V2, schema {1 id long, 2 category string, 3 value double}.
  // THREE DATA files with DISTINCT id metric bounds so METRIC pruning (not just partition pruning) is
  // exercised, then a POSITION-DELETE for F1:
  //   F1: category=a, id lower=1  upper=10, record_count 3  -> carries the position-delete
  //   F2: category=b, id lower=11 upper=20, record_count 2
  //   F3: category=a, id lower=21 upper=30, record_count 4
  //
  // THE SCENARIOS (a stable ordered list shared with the Rust test BY NAME). For each, Java plans via the
  // REAL table.newScan().filter(expr).planFiles() and emits java_scan_<name>.json = a list of
  // { data_file_path, delete_file_paths:[...], residual_always_true } per planned data file:
  //   s0 "no_filter"       : no filter            -> plans F1,F2,F3 ; F1 carries the delete ; residual TRUE
  //   s1 "partition_a"     : category = 'a'        -> plans F1,F3 (partition prune drops the cat=b F2) ;
  //                                                   residual always-true (the filter IS the partition) ;
  //                                                   F1 still carries the delete
  //   s2 "metric_id_gt_15" : id > 15               -> plans F2,F3 (F1 upper=10 < 15 pruned by METRICS) ;
  //                                                   residual NOT always-true (id is not a partition col)
  //   s3 "combined"        : category='a' AND id>25 -> plans F3 only (partition drops F2 ; metrics drop F1,
  //                                                   whose id upper=10 < 25) ; residual NOT always-true on F3
  //
  // RESIDUAL SCOPE. A4 compares only a BOOLEAN "residual is fully covered by partitioning" per planned file
  // (Java: residual().op() == Operation.TRUE; Rust: the task predicate is None or AlwaysTrue) — NOT the full
  // residual EXPRESSION string (that needs a cross-language expression-normalization design and is DEFERRED;
  // Rust residuals are already unit-tested). This proves the partition-filter-removal SPLIT matches without a
  // fragile cross-language expression-string comparison.
  // ===========================================================================================

  /**
   * The A4 half of the inspection oracle — SCAN PLANNING. Builds a dedicated partitioned V2 table on local
   * disk under {@code <dir>/table_a4} via real commits (one {@code newAppend} of F1/F2/F3 with distinct id
   * metric bounds + one {@code newRowDelta} adding a position-delete for F1), writes
   * {@code <dir>/table_a4/metadata/final.metadata.json}, and for each named filter scenario emits
   * {@code java_scan_<name>.json} — the rows of Java's REAL {@code table.newScan().filter(expr).planFiles()}
   * projected to {@code { data_file_path, delete_file_paths, residual_always_true }}.
   */
  static final class InspectionScanA4Oracle {
    private InspectionScanA4Oracle() {}

    static void generate(Path dir) throws IOException {
      Files.createDirectories(dir);

      // 1. Build the dedicated partitioned V2 table on local disk under <dir>/table_a4 (A1/A2 untouched).
      //    Bare absolute location so the manifest/manifest-list paths the commits write are bare absolute
      //    paths the Rust FileIO resolves directly (same as A1/A2).
      File tableDir = dir.resolve("table_a4").toFile();
      File metadataDir = new File(tableDir, "metadata");
      if (!metadataDir.isDirectory() && !metadataDir.mkdirs()) {
        throw new IOException("failed to create metadata dir at " + metadataDir);
      }

      Schema schema =
          new Schema(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "category", Types.StringType.get()),
              Types.NestedField.optional(3, "value", Types.DoubleType.get()));
      PartitionSpec spec = PartitionSpec.builderFor(schema).identity("category").build();

      Map<String, String> props = new LinkedHashMap<>();
      props.put(TableProperties.FORMAT_VERSION, "2");
      TableMetadata seed =
          TableMetadata.newTableMetadata(
              schema, spec, SortOrder.unsorted(), tableDir.getAbsolutePath(), props);

      LocalTableOperations ops = new LocalTableOperations(tableDir, metadataDir);
      ops.commit(null, seed); // persist v0 metadata so the BaseTable has a current metadata to evolve.
      BaseTable table = new BaseTable(ops, "interop_inspection_scan_a4");

      // 2. THREE DATA files with DISTINCT id metric bounds so METRIC pruning is exercised (id is NOT a
      //    partition column, so id-range pruning can only come from the lower/upper bound metrics):
      //      F1 cat=a id[1,10] rc=3 ; F2 cat=b id[11,20] rc=2 ; F3 cat=a id[21,30] rc=4.
      DataFile fileF1 =
          DataFiles.builder(spec)
              .withPath(tableDir.getAbsolutePath() + "/data/category=a/00000-f1.parquet")
              .withFileSizeInBytes(1100L)
              .withRecordCount(3L)
              .withPartitionPath("category=a")
              .withMetrics(metricsFor(3L, 1L, 10L, 10.5d, 100.5d))
              .build();
      DataFile fileF2 =
          DataFiles.builder(spec)
              .withPath(tableDir.getAbsolutePath() + "/data/category=b/00000-f2.parquet")
              .withFileSizeInBytes(900L)
              .withRecordCount(2L)
              .withPartitionPath("category=b")
              .withMetrics(metricsFor(2L, 11L, 20L, 110.5d, 200.5d))
              .build();
      DataFile fileF3 =
          DataFiles.builder(spec)
              .withPath(tableDir.getAbsolutePath() + "/data/category=a/00001-f3.parquet")
              .withFileSizeInBytes(1300L)
              .withRecordCount(4L)
              .withPartitionPath("category=a")
              .withMetrics(metricsFor(4L, 21L, 30L, 210.5d, 300.5d))
              .build();

      // 3. One POSITION-DELETE file in category=a — it applies to F1 (same partition, sequence number after
      //    F1's append), so F1 carries the delete in the no-filter and partition=a scans.
      DeleteFile deleteForF1 =
          FileMetadata.deleteFileBuilder(spec)
              .ofPositionDeletes()
              .withPath(tableDir.getAbsolutePath() + "/data/category=a/00000-f1-deletes.parquet")
              .withFileSizeInBytes(150L)
              .withRecordCount(1L)
              .withPartitionPath("category=a")
              .build();

      // 4. Real commits: the three data files via newAppend (writes a DATA manifest + manifest-list), then
      //    the delete via newRowDelta (writes a DELETE manifest).
      table.newAppend().appendFile(fileF1).appendFile(fileF2).appendFile(fileF3).commit();
      table.newRowDelta().addDeletes(deleteForF1).commit();

      // 5. Write the FINAL metadata to a KNOWN path so the Rust test loads it deterministically.
      Path finalMetadata = metadataDir.toPath().resolve("final.metadata.json");
      OutputFile finalOut =
          new LocalFileIO().newOutputFile(finalMetadata.toAbsolutePath().toString());
      TableMetadataParser.write(ops.current(), finalOut);

      // 6. The ordered, named scenarios (shared with the Rust test by name). Each plans via the REAL
      //    table.newScan().filter(expr).planFiles().
      Map<String, Expression> scenarios = new LinkedHashMap<>();
      scenarios.put("no_filter", Expressions.alwaysTrue());
      scenarios.put("partition_a", Expressions.equal("category", "a"));
      scenarios.put("metric_id_gt_15", Expressions.greaterThan("id", 15L));
      scenarios.put(
          "combined",
          Expressions.and(Expressions.equal("category", "a"), Expressions.greaterThan("id", 25L)));

      for (Map.Entry<String, Expression> entry : scenarios.entrySet()) {
        String name = entry.getKey();
        writeJson(
            dir.resolve("java_scan_" + name + ".json"), planScanToJson(table, entry.getValue()));
        System.out.println("generated scan plan: " + name);
      }
      System.out.println("generated inspection-scan A4 table + fixtures to " + dir);
    }

    /**
     * Build a pure-metadata {@link Metrics} for the schema {1 id long, 2 category string, 3 value double}:
     * column_sizes + value_counts + null_value_counts for ids 1/2/3, and lower/upper bounds for id 1 (long,
     * the METRIC-pruning column) and id 3 (value, double). category (id 2) is excluded from bounds (it is the
     * identity-partition column — pruned by the partition predicate, not by metrics). Identical shape to A1/A2.
     */
    private static Metrics metricsFor(
        long recordCount, long idLower, long idUpper, double valueLower, double valueUpper) {
      Map<Integer, Long> columnSizes = new LinkedHashMap<>();
      columnSizes.put(1, 40L);
      columnSizes.put(2, 24L);
      columnSizes.put(3, 32L);
      Map<Integer, Long> valueCounts = new LinkedHashMap<>();
      valueCounts.put(1, recordCount);
      valueCounts.put(2, recordCount);
      valueCounts.put(3, recordCount);
      Map<Integer, Long> nullValueCounts = new LinkedHashMap<>();
      nullValueCounts.put(1, 0L);
      nullValueCounts.put(2, 0L);
      nullValueCounts.put(3, 0L);
      Map<Integer, ByteBuffer> lowerBounds = new LinkedHashMap<>();
      lowerBounds.put(1, Conversions.toByteBuffer(Types.LongType.get(), idLower));
      lowerBounds.put(3, Conversions.toByteBuffer(Types.DoubleType.get(), valueLower));
      Map<Integer, ByteBuffer> upperBounds = new LinkedHashMap<>();
      upperBounds.put(1, Conversions.toByteBuffer(Types.LongType.get(), idUpper));
      upperBounds.put(3, Conversions.toByteBuffer(Types.DoubleType.get(), valueUpper));
      return new Metrics(
          recordCount,
          columnSizes,
          valueCounts,
          nullValueCounts,
          null, // nan_value_counts — none
          lowerBounds,
          upperBounds);
    }

    /**
     * Plan a scan with {@code filter} via Java's REAL {@link Table#newScan()}.{@code filter(expr)}.{@code
     * planFiles()} and serialize the planned tasks to a JSON array of
     * {@code { data_file_path, delete_file_paths:[...], residual_always_true }}. For each {@link
     * FileScanTask}: {@code file().path()} is the planned data-file path; {@code deletes()} are the applicable
     * delete files (sorted by path for a stable comparison); {@code residual()} is the per-file leftover row
     * filter, whose {@code op() == TRUE} means it is FULLY covered by partitioning (no per-row filtering).
     *
     * <p>IMPORTANT: serialize EAGERLY inside the {@code try-with-resources} iteration — {@code planFiles()}
     * returns a lazy {@link CloseableIterable}, and the tasks/files must be read while the iterable is open.
     */
    private static String planScanToJson(Table table, Expression filter) {
      return JsonUtil.generate(
          gen -> {
            gen.writeStartArray();
            try (CloseableIterable<FileScanTask> tasks =
                table.newScan().filter(filter).planFiles()) {
              for (FileScanTask task : tasks) {
                gen.writeStartObject();
                gen.writeStringField("data_file_path", task.file().path().toString());

                // The applicable delete files, sorted by path for an order-independent comparison.
                java.util.List<String> deletePaths = new java.util.ArrayList<>();
                for (DeleteFile delete : task.deletes()) {
                  deletePaths.add(delete.path().toString());
                }
                java.util.Collections.sort(deletePaths);
                gen.writeArrayFieldStart("delete_file_paths");
                for (String deletePath : deletePaths) {
                  gen.writeString(deletePath);
                }
                gen.writeEndArray();

                // The residual is fully covered by partitioning iff it reduced to the alwaysTrue() singleton
                // (Java ResidualEvaluator returns Expressions.alwaysTrue() — op() == Operation.TRUE — when the
                // filter is entirely partition-implied for this file's partition tuple).
                boolean residualAlwaysTrue =
                    task.residual().op() == Expression.Operation.TRUE;
                gen.writeBooleanField("residual_always_true", residualAlwaysTrue);

                gen.writeEndObject();
              }
            }
            gen.writeEndArray();
          },
          true);
    }
  }

  // ===========================================================================================
  // Inspection-READABLE_METRICS oracle — the interop proof for the `files` table's trailing virtual
  // `readable_metrics` STRUCT, the LAST inspection-table surface A1-A4 explicitly DEFER. It builds DIRECTLY
  // on A1's harness (it reuses A1/A2/A4's LocalTableOperations + LocalFileIO + writeJson + real newAppend
  // commits) and writes a FOURTH, dedicated table under <dir>/table_rm (A1's <dir>/table + A2's
  // <dir>/table_a2 + A4's <dir>/table_a4 are UNTOUCHED).
  //
  // `readable_metrics` (Java MetricsUtil.READABLE_METRICS / readableMetricsStruct, MetricsUtil.java:140-193
  // for READABLE_METRIC_COLS, :356-393 for readableMetricsSchema) is a STRUCT with ONE sub-field per LEAF
  // (primitive-typed) column of the data table, each itself a struct of the six metrics
  // {column_size, value_count, null_value_count, nan_value_count, lower_bound, upper_bound}. The four counts
  // are read from the file's metric maps BY FIELD ID; the lower/upper bounds are the file's stored bound
  // bytes DECODED to the column's OWN type via Conversions.fromByteBuffer(field.type(), buffer)
  // (MetricsUtil.java:174-192) — Long for a long column, CharSequence/String for a string column, Double for
  // a double column. This oracle proves that typed decode across THREE type classes, the case the raw-map A1
  // test (which compares only the bound BYTES) cannot reach (notably the STRING bound).
  //
  // SCHEMA (unpartitioned V2): {1 id LONG, 2 name STRING, 3 score DOUBLE} — three leaf primitives of three
  // type classes. ONE data file carrying RICH, DISTINCT per-column metrics (distinct column_sizes; a
  // null_value_count non-zero for `name`; a nan_value_count non-zero for the DOUBLE `score` — Java allows NaN
  // counts only on floating types; distinct long/string/double lower & upper bounds).
  //
  // EMIT (java_rm_files.json): a JSON OBJECT keyed by LEAF COLUMN NAME -> an object of the six metric NAMES
  // -> the typed scalar value (long/string/double). The Rust test compares BY NAME, not by field id — Java's
  // readable_metrics sub-field ids come from a HashMap-order counter while the Rust port assigns ascending
  // ids (a documented divergence; see readable_metrics.rs module docs). The sub-field NAMES match, so
  // everything is keyed by name. A1/A2/A4 emitters are UNCHANGED, so their JSON stays byte-identical.
  // ===========================================================================================

  /**
   * The readable-metrics half of the oracle. Builds an UNPARTITIONED V2 table on local disk under
   * {@code <dir>/table_rm} via one real {@code newAppend} commit (so a real AVRO data manifest + a
   * manifest-list land under {@code <dir>/table_rm/metadata}), writes the final metadata to a deterministic
   * {@code <dir>/table_rm/metadata/final.metadata.json}, and emits the rows of Java's REAL {@link FilesTable}
   * INCLUDING the {@code readable_metrics} struct as {@code java_rm_files.json}, keyed by leaf column name
   * then metric name.
   */
  static final class InspectionReadableMetricsRmOracle {
    private InspectionReadableMetricsRmOracle() {}

    static void generate(Path dir) throws IOException {
      Files.createDirectories(dir);

      // 1. Build an UNPARTITIONED V2 table on local disk under <dir>/table_rm. Bare absolute location so the
      //    manifest/manifest-list paths the commit writes are bare absolute paths the Rust FileIO resolves
      //    directly (same convention as A1/A2/A4).
      File tableDir = dir.resolve("table_rm").toFile();
      File metadataDir = new File(tableDir, "metadata");
      if (!metadataDir.isDirectory() && !metadataDir.mkdirs()) {
        throw new IOException("failed to create metadata dir at " + metadataDir);
      }

      Schema schema =
          new Schema(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "name", Types.StringType.get()),
              Types.NestedField.optional(3, "score", Types.DoubleType.get()));
      PartitionSpec spec = PartitionSpec.unpartitioned();

      Map<String, String> props = new LinkedHashMap<>();
      props.put(TableProperties.FORMAT_VERSION, "2");
      TableMetadata seed =
          TableMetadata.newTableMetadata(
              schema, spec, SortOrder.unsorted(), tableDir.getAbsolutePath(), props);

      LocalTableOperations ops = new LocalTableOperations(tableDir, metadataDir);
      ops.commit(null, seed); // persist v0 metadata so the BaseTable has a current metadata to evolve.
      BaseTable table = new BaseTable(ops, "interop_inspection_readable_metrics");

      // 2. ONE DATA file carrying RICH, DISTINCT per-column metrics. The referenced .parquet path is pure
      //    metadata — it need not exist; the files table reads only the manifest entry. The metrics are
      //    chosen so EVERY one of the six readable-metric sub-fields has a distinct, unambiguous value per
      //    leaf column (so a cross-wiring of any metric source to the wrong sub-field is observable).
      DataFile dataFile =
          DataFiles.builder(spec)
              .withPath(tableDir.getAbsolutePath() + "/data/00000-rm.parquet")
              .withFileSizeInBytes(1234L)
              .withRecordCount(7L)
              .withMetrics(richMetrics())
              .build();

      // 3. One real commit: the data file via newAppend (writes a DATA manifest + manifest-list).
      table.newAppend().appendFile(dataFile).commit();

      // 4. Write the FINAL metadata to a KNOWN path so the Rust test loads it deterministically.
      Path finalMetadata = metadataDir.toPath().resolve("final.metadata.json");
      OutputFile finalOut =
          new LocalFileIO().newOutputFile(finalMetadata.toAbsolutePath().toString());
      TableMetadataParser.write(ops.current(), finalOut);

      // 5. Materialize + emit Java's REAL FilesTable rows INCLUDING the readable_metrics struct.
      writeJson(dir.resolve("java_rm_files.json"), readableMetricsToJson(table));
      System.out.println("generated inspection readable_metrics table + java_rm_files.json to " + dir);
    }

    /**
     * Build a pure-metadata {@link Metrics} for the schema {1 id long, 2 name string, 3 score double} with
     * RICH, DISTINCT values across all six readable-metric sub-fields:
     *
     * <ul>
     *   <li>column_sizes: DISTINCT per column (id=101, name=202, score=303).
     *   <li>value_counts: the record count (7) for all three columns.
     *   <li>null_value_counts: NON-ZERO for at least one column — name=2 (id=0, score=1).
     *   <li>nan_value_counts: NON-ZERO for the DOUBLE column score=3; absent for id/name (Java allows NaN
     *       counts only on floating types).
     *   <li>lower_bounds / upper_bounds: DISTINCT per column, each encoded with
     *       {@code Conversions.toByteBuffer(<the column's type>, value)} — long bounds for id (10/90),
     *       STRING bounds for name ("alpha"/"zulu", the case the raw-map A1 test cannot reach), double bounds
     *       for score (1.5/99.5).
     * </ul>
     */
    private static Metrics richMetrics() {
      Map<Integer, Long> columnSizes = new LinkedHashMap<>();
      columnSizes.put(1, 101L);
      columnSizes.put(2, 202L);
      columnSizes.put(3, 303L);
      Map<Integer, Long> valueCounts = new LinkedHashMap<>();
      valueCounts.put(1, 7L);
      valueCounts.put(2, 7L);
      valueCounts.put(3, 7L);
      Map<Integer, Long> nullValueCounts = new LinkedHashMap<>();
      nullValueCounts.put(1, 0L);
      nullValueCounts.put(2, 2L); // name has NON-ZERO nulls.
      nullValueCounts.put(3, 1L);
      Map<Integer, Long> nanValueCounts = new LinkedHashMap<>();
      nanValueCounts.put(3, 3L); // only the DOUBLE column carries a NaN count.
      Map<Integer, ByteBuffer> lowerBounds = new LinkedHashMap<>();
      lowerBounds.put(1, Conversions.toByteBuffer(Types.LongType.get(), 10L));
      lowerBounds.put(2, Conversions.toByteBuffer(Types.StringType.get(), "alpha"));
      lowerBounds.put(3, Conversions.toByteBuffer(Types.DoubleType.get(), 1.5d));
      Map<Integer, ByteBuffer> upperBounds = new LinkedHashMap<>();
      upperBounds.put(1, Conversions.toByteBuffer(Types.LongType.get(), 90L));
      upperBounds.put(2, Conversions.toByteBuffer(Types.StringType.get(), "zulu"));
      upperBounds.put(3, Conversions.toByteBuffer(Types.DoubleType.get(), 99.5d));
      return new Metrics(
          7L, columnSizes, valueCounts, nullValueCounts, nanValueCounts, lowerBounds, upperBounds);
    }

    /**
     * Materialize the rows of Java's REAL {@link FilesTable} (via {@link
     * MetadataTableUtils#createMetadataTableInstance} + {@code task.asDataTask().rows()}, reading the ON-DISK
     * AVRO manifest) and serialize ONLY the {@code readable_metrics} struct of the single data-file row,
     * keyed by leaf column NAME then metric NAME.
     *
     * <p>The {@code readable_metrics} column is located by NAME in {@code mt.schema().columns()} (never by a
     * hardcoded position); its Iceberg {@link Types.StructType} provides the per-leaf-column sub-fields (the
     * column struct names, sorted by name) and each column's six metric sub-fields, and the row value at that
     * position is the {@code ReadableMetricsStruct} ({@link StructLike}) whose positional values align with
     * the struct type. The bound metrics are ALREADY decoded Java objects (Long/CharSequence/Double), so
     * {@code writeMetricScalar} writes each as its natural JSON scalar. The single data file is asserted, so
     * exactly one row is expected.
     */
    private static String readableMetricsToJson(BaseTable baseTable) {
      Table mt = MetadataTableUtils.createMetadataTableInstance(baseTable, MetadataTableType.FILES);
      List<Types.NestedField> columns = mt.schema().columns();
      int metricsPosition = -1;
      Types.StructType metricsType = null;
      for (int i = 0; i < columns.size(); i++) {
        if (MetricsUtil.READABLE_METRICS.equals(columns.get(i).name())) {
          metricsPosition = i;
          metricsType = columns.get(i).type().asStructType();
          break;
        }
      }
      if (metricsPosition < 0 || metricsType == null) {
        throw new IllegalStateException("FilesTable schema is missing the readable_metrics column");
      }
      final int position = metricsPosition;
      final Types.StructType readableMetricsType = metricsType;

      return JsonUtil.generate(
          gen -> {
            int rowCount = 0;
            try (CloseableIterable<FileScanTask> tasks = mt.newScan().planFiles()) {
              for (FileScanTask task : tasks) {
                try (CloseableIterable<StructLike> taskRows = task.asDataTask().rows()) {
                  for (StructLike row : taskRows) {
                    if (rowCount > 0) {
                      throw new IllegalStateException(
                          "table_rm must have exactly one data file, found more rows");
                    }
                    rowCount++;
                    StructLike readableMetrics = row.get(position, StructLike.class);
                    writeReadableMetrics(gen, readableMetricsType, readableMetrics);
                  }
                }
              }
            }
            if (rowCount != 1) {
              throw new IllegalStateException(
                  "table_rm must have exactly one data file, found " + rowCount + " rows");
            }
          },
          true);
    }

    /**
     * Serialize the {@code readable_metrics} struct as a JSON object keyed by leaf column NAME -> an object of
     * the six metric NAMES -> the typed scalar value. Walks the struct type's per-column sub-fields (each a
     * StructType of the six metrics) positionally against the {@link StructLike} so the metric names come from
     * the schema, never hardcoded order.
     */
    private static void writeReadableMetrics(
        JsonGenerator gen, Types.StructType readableMetricsType, StructLike readableMetrics)
        throws IOException {
      gen.writeStartObject();
      List<Types.NestedField> columnStructs = readableMetricsType.fields();
      for (int columnIndex = 0; columnIndex < columnStructs.size(); columnIndex++) {
        Types.NestedField columnStruct = columnStructs.get(columnIndex);
        gen.writeObjectFieldStart(columnStruct.name());
        StructLike columnMetrics = readableMetrics.get(columnIndex, StructLike.class);
        List<Types.NestedField> metricFields = columnStruct.type().asStructType().fields();
        for (int metricIndex = 0; metricIndex < metricFields.size(); metricIndex++) {
          Types.NestedField metricField = metricFields.get(metricIndex);
          Object value = columnMetrics.get(metricIndex, Object.class);
          writeMetricScalar(gen, metricField.name(), value);
        }
        gen.writeEndObject();
      }
      gen.writeEndObject();
    }

    /**
     * Write one readable-metric value as its natural JSON scalar. The four counts are {@link Long}; the
     * bounds are the DECODED column value — {@link Long} for a long column, {@link CharSequence}/{@link
     * String} for a string column, {@link Double} for a double column. A null metric (absent in the file's
     * map) is written as JSON null.
     */
    private static void writeMetricScalar(JsonGenerator gen, String name, Object value)
        throws IOException {
      if (value == null) {
        gen.writeNullField(name);
      } else if (value instanceof Long) {
        gen.writeNumberField(name, (Long) value);
      } else if (value instanceof Integer) {
        gen.writeNumberField(name, (Integer) value);
      } else if (value instanceof Double) {
        gen.writeNumberField(name, (Double) value);
      } else if (value instanceof Float) {
        gen.writeNumberField(name, (Float) value);
      } else {
        // String bounds decode to CharSequence; toString() yields the underlying string.
        gen.writeStringField(name, value.toString());
      }
    }
  }

  // ===========================================================================================
  // Scan-EXECUTION oracle — the FIRST DATA-LEVEL scan-execution interop increment (the capstone). Every
  // mode above reads MANIFESTS or pure metadata; this one proves the DATA path end to end: Rust's
  // scan→Arrow with MERGE-ON-READ position-delete application must equal Java's OWN read of a JAVA-WRITTEN
  // table containing REAL parquet data + a REAL position-delete file.
  //
  // Unlike A1-A4 (which used PURE-METADATA DataFiles/FileMetadata builders whose referenced .parquet need
  // not exist), this oracle writes the ACTUAL parquet bytes via the generic parquet appender
  // (iceberg-data's GenericAppenderFactory, routed to iceberg-parquet's GenericParquetWriter) AND the
  // ACTUAL position-delete parquet via the generic position-delete writer — then builds the DataFile /
  // DeleteFile from the writer's real metrics + length + record_count (mirroring iceberg-data's
  // FileHelpers.writeDataFile / writePosDeleteFile "write records → DataFile" template).
  //
  // THE TABLE (under <dir>/table). Unpartitioned V2, schema {1 id long required, 2 data string optional}:
  //   data file 00000-data.parquet: rows (10,"a") (20,"b") (30,"c") (40,"d") (50,"e") at positions 0..4.
  //   position-delete 00000-data-deletes.parquet: deletes positions {1,3} of that data file (rows 20, 40).
  //   live rows after merge-on-read = {10,30,50}.
  // Commits: newAppend(dataFile) then newRowDelta(deleteFile) — real AVRO manifests + manifest-list land on
  // disk under <dir>/table/metadata, and final.metadata.json is written to a known path for the Rust test.
  //
  // THE GROUND TRUTH. Java materializes its OWN merge-on-read read via IcebergGenerics.read(table).build()
  // (which plans the scan, opens the parquet, and APPLIES the position deletes), collects the live rows,
  // sorts by id, and writes them to <dir>/java_scan_rows.json as [{id,data}, ...] = [{10,a},{30,c},{50,e}].
  // The Rust test reads the SAME table, runs table.scan().build()?.to_arrow(), and asserts equality.
  // ===========================================================================================

  /**
   * The scan-execution half of the oracle — the DATA-level merge-on-read interop. Builds an unpartitioned
   * V2 table on local disk under {@code <dir>/table}, writes a REAL parquet data file (5 rows) + a REAL
   * position-delete file (deleting positions 1 and 3) via the generic appender / position-delete writer,
   * commits them ({@code newAppend} + {@code newRowDelta}), writes {@code final.metadata.json}, and emits
   * Java's OWN merge-on-read read (via {@link IcebergGenerics}) as {@code java_scan_rows.json}.
   */
  static final class ScanExecOracle {
    private ScanExecOracle() {}

    static void generate(Path dir) throws IOException {
      Files.createDirectories(dir);

      // 1. Build the unpartitioned V2 table on local disk under <dir>/table. The location is the BARE
      //    absolute path so the manifest/manifest-list/data/delete paths the writers + commits use are bare
      //    absolute paths the Rust FileIO::new_with_fs() resolves directly (same convention as A1-A4).
      File tableDir = dir.resolve("table").toFile();
      File metadataDir = new File(tableDir, "metadata");
      File dataDir = new File(tableDir, "data");
      if (!metadataDir.isDirectory() && !metadataDir.mkdirs()) {
        throw new IOException("failed to create metadata dir at " + metadataDir);
      }
      if (!dataDir.isDirectory() && !dataDir.mkdirs()) {
        throw new IOException("failed to create data dir at " + dataDir);
      }

      Schema schema =
          new Schema(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "data", Types.StringType.get()));
      PartitionSpec spec = PartitionSpec.unpartitioned();

      Map<String, String> props = new LinkedHashMap<>();
      props.put(TableProperties.FORMAT_VERSION, "2");
      TableMetadata seed =
          TableMetadata.newTableMetadata(
              schema, spec, SortOrder.unsorted(), tableDir.getAbsolutePath(), props);

      LocalTableOperations ops = new LocalTableOperations(tableDir, metadataDir);
      ops.commit(null, seed); // persist v0 metadata so the BaseTable has a current metadata to evolve.
      BaseTable table = new BaseTable(ops, "interop_scan_exec");

      // 2. Write a REAL parquet DATA file: 5 GenericRecords (10,"a")..(50,"e") at positions 0..4. The
      //    DataFile is built from the appender's REAL metrics + length + record_count (the FileHelpers
      //    "write records → DataFile" template), so the on-disk parquet + the manifest entry agree.
      String dataPath = new File(dataDir, "00000-data.parquet").getAbsolutePath();
      DataFile dataFile = writeDataFile(table, schema, spec, dataPath);

      // 3. Write a REAL parquet POSITION-DELETE file deleting positions {1,3} of that data file (rows 20 and
      //    40). Built from the position-delete writer's real metrics, so a real merge-on-read reader applies it.
      String deletePath = new File(dataDir, "00000-data-deletes.parquet").getAbsolutePath();
      DeleteFile deleteFile = writePosDeleteFile(table, schema, spec, deletePath, dataFile.path());

      // 4. Real commits: the data file via newAppend (writes a DATA manifest + manifest-list), then the
      //    position-delete via newRowDelta (writes a DELETE manifest). Live rows are now {10,30,50}.
      table.newAppend().appendFile(dataFile).commit();
      table.newRowDelta().addDeletes(deleteFile).commit();

      // 5. Write the FINAL metadata to a KNOWN path so the Rust test loads it deterministically. (The real
      //    on-disk manifest-list + manifests + parquet already live under <dir>/table.)
      Path finalMetadata = metadataDir.toPath().resolve("final.metadata.json");
      OutputFile finalOut =
          new LocalFileIO().newOutputFile(finalMetadata.toAbsolutePath().toString());
      TableMetadataParser.write(ops.current(), finalOut);

      // 6. Java materializes its OWN merge-on-read READ (IcebergGenerics plans the scan, opens the parquet,
      //    and APPLIES the position deletes) → collect live rows, sort by id, emit java_scan_rows.json. This
      //    is the GROUND TRUTH the Rust scan→Arrow must equal: [{10,a},{30,c},{50,e}] (20 and 40 deleted).
      writeJson(dir.resolve("java_scan_rows.json"), readLiveRowsToJson(table));
      System.out.println("generated scan-exec table + java_scan_rows.json to " + dir);
    }

    /**
     * Write a REAL parquet data file via the generic appender and return its {@link DataFile} built from
     * the appender's REAL metrics + length + split offsets (the {@code FileHelpers.writeDataFile} template).
     * The 5 rows are (10,"a") (20,"b") (30,"c") (40,"d") (50,"e") at positions 0..4.
     */
    private static DataFile writeDataFile(
        BaseTable table, Schema schema, PartitionSpec spec, String path) throws IOException {
      List<Record> rows = new ArrayList<>();
      long[] ids = {10L, 20L, 30L, 40L, 50L};
      String[] values = {"a", "b", "c", "d", "e"};
      for (int i = 0; i < ids.length; i++) {
        GenericRecord record = GenericRecord.create(schema);
        record.setField("id", ids[i]);
        record.setField("data", values[i]);
        rows.add(record);
      }

      GenericAppenderFactory factory = new GenericAppenderFactory(schema, spec);
      OutputFile out = table.io().newOutputFile(path);
      // newDataWriter wires the appender to the DataFile builder: toDataFile() reads back the REAL metrics
      // (record count, file size, column sizes, bounds, split offsets) the parquet writer produced.
      DataWriter<Record> writer =
          factory.newDataWriter(
              org.apache.iceberg.encryption.EncryptedFiles.encryptedOutput(
                  out, org.apache.iceberg.encryption.EncryptionKeyMetadata.EMPTY),
              FileFormat.PARQUET,
              null);
      try (Closeable toClose = writer) {
        writer.write(rows);
      }
      return writer.toDataFile();
    }

    /**
     * Write a REAL parquet POSITION-DELETE file via the generic position-delete writer, deleting positions
     * {1, 3} of {@code referencedDataPath} (rows 20 and 40), and return its {@link DeleteFile} built from
     * the writer's REAL metrics (the {@code FileHelpers.writePosDeleteFile} template). The delete references
     * the data file's path + the two positions, so a real merge-on-read reader masks exactly those rows.
     */
    private static DeleteFile writePosDeleteFile(
        BaseTable table,
        Schema schema,
        PartitionSpec spec,
        String path,
        CharSequence referencedDataPath)
        throws IOException {
      GenericAppenderFactory factory = new GenericAppenderFactory(schema, spec);
      OutputFile out = table.io().newOutputFile(path);
      PositionDeleteWriter<Record> writer =
          factory.newPosDeleteWriter(
              org.apache.iceberg.encryption.EncryptedFiles.encryptedOutput(
                  out, org.apache.iceberg.encryption.EncryptionKeyMetadata.EMPTY),
              FileFormat.PARQUET,
              null);
      PositionDelete<Record> posDelete = PositionDelete.create();
      try (Closeable toClose = writer) {
        // Delete positions 1 and 3 (the 2nd and 4th rows: id 20 and id 40). No row data is carried.
        writer.write(posDelete.set(referencedDataPath, 1L, null));
        writer.write(posDelete.set(referencedDataPath, 3L, null));
      }
      return writer.toDeleteFile();
    }

    /**
     * Materialize Java's OWN merge-on-read READ of the table via {@link IcebergGenerics} (which plans the
     * scan, opens the parquet, AND applies the position deletes), collect the live rows, SORT by id, and
     * serialize them to a JSON array of {@code {id, data}}. This is the GROUND TRUTH = [{10,a},{30,c},{50,e}].
     */
    private static String readLiveRowsToJson(BaseTable table) {
      Map<Long, String> dataById = new LinkedHashMap<>();
      try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
        for (Record record : records) {
          Long id = (Long) record.getField("id");
          Object data = record.getField("data");
          dataById.put(id, data == null ? null : data.toString());
        }
      } catch (IOException error) {
        throw new RuntimeException("failed to read live rows via IcebergGenerics", error);
      }

      // Sort by id for a stable, order-independent ground truth.
      List<Long> ids = new ArrayList<>(dataById.keySet());
      ids.sort(Long::compareTo);
      return JsonUtil.generate(
          gen -> {
            gen.writeStartArray();
            for (Long id : ids) {
              gen.writeStartObject();
              gen.writeNumberField("id", id);
              String data = dataById.get(id);
              if (data == null) {
                gen.writeNullField("data");
              } else {
                gen.writeStringField("data", data);
              }
              gen.writeEndObject();
            }
            gen.writeEndArray();
          },
          true);
    }

    /**
     * DIRECTION 2 verify — read the RUST-written table and assert the merge-on-read rows.
     *
     * <p>Loads {@code <dir>/rust_table/metadata/final.metadata.json} (the metadata Rust's GEN path wrote),
     * builds a {@link BaseTable} over a {@link LocalFileIO} (so {@code io()} reads the real on-disk parquet +
     * avro the Rust commits produced), reads every live row with {@code IcebergGenerics.read(table).build()}
     * (which APPLIES the Rust-written position delete), sorts by id, and asserts the rows equal the expected
     * merge-on-read set {@code {(10,a),(30,c),(50,e)}} (ids 20/40 deleted). Prints PASS/FAIL per check and
     * returns the number of failures so {@code main} can {@code System.exit(1)} on any FAIL.
     *
     * <p>A failure here is a REAL write-incompatibility finding: it means Java could not read something Rust
     * wrote (a manifest, the manifest-list, the parquet data, or the position delete), or read the WRONG
     * rows. We do NOT massage Rust's output to be "Java-shaped" — the verify must fail loudly so a genuine
     * divergence surfaces.
     */
    static int verify(Path dir) {
      int failures = 0;
      Path finalMetadata = dir.resolve("rust_table").resolve("metadata").resolve("final.metadata.json");

      if (!Files.exists(finalMetadata)) {
        System.out.println(
            "FAIL scan-exec-d2: missing " + finalMetadata + " (run the Rust GEN path first)");
        return 1;
      }

      // 1. Load the Rust-written metadata. A parse failure is itself a divergence (Java cannot read Rust's
      //    on-disk TableMetadata JSON).
      TableMetadata metadata;
      try {
        metadata =
            TableMetadataParser.fromJson(finalMetadata.toString(), readString(finalMetadata));
      } catch (RuntimeException | IOException parseError) {
        System.out.println(
            "FAIL scan-exec-d2: Java could not parse the Rust-written final.metadata.json: "
                + parseError);
        return 1;
      }

      // 2. Build a BaseTable over a LocalFileIO so io() resolves the bare absolute manifest/parquet paths the
      //    Rust commits wrote, then read the live rows via IcebergGenerics (which applies the position delete).
      FileIO io = new LocalFileIO();
      BaseTable table = new BaseTable(new InMemoryInspectionOperations(metadata, io), "rust_table");

      Map<Long, String> dataById = new LinkedHashMap<>();
      try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
        for (Record record : records) {
          Long id = (Long) record.getField("id");
          Object data = record.getField("data");
          dataById.put(id, data == null ? null : data.toString());
        }
      } catch (RuntimeException | IOException readError) {
        // A read failure here is the headline divergence: Java could not read the Rust-written
        // manifests / parquet / position-delete. Report it precisely.
        System.out.println(
            "FAIL scan-exec-d2: Java could not READ the Rust-written table via IcebergGenerics "
                + "(manifests/parquet/position-delete incompatibility): "
                + readError);
        return 1;
      }

      List<Long> liveIds = new ArrayList<>(dataById.keySet());
      liveIds.sort(Long::compareTo);

      // 3a. Exactly 3 live rows survive (5 written, positions 1 and 3 deleted).
      if (liveIds.size() != 3) {
        System.out.println(
            "FAIL scan-exec-d2: expected 3 live rows after merge-on-read, got "
                + liveIds.size()
                + " "
                + liveIds);
        failures++;
      } else {
        System.out.println("PASS scan-exec-d2: 3 live rows survive merge-on-read");
      }

      // 3b. The deleted ids (20, 40) must be ABSENT — Rust's position delete must have been applied.
      if (liveIds.contains(20L) || liveIds.contains(40L)) {
        System.out.println(
            "FAIL scan-exec-d2: deleted ids 20/40 must be ABSENT, but live set is " + liveIds);
        failures++;
      } else {
        System.out.println("PASS scan-exec-d2: deleted ids 20/40 are absent (Rust's delete applied)");
      }

      // 3c. The exact surviving (id, data) set equals {(10,a),(30,c),(50,e)}.
      Map<Long, String> expected = new LinkedHashMap<>();
      expected.put(10L, "a");
      expected.put(30L, "c");
      expected.put(50L, "e");
      boolean valuesMatch = liveIds.equals(new ArrayList<>(expected.keySet()));
      for (Long id : liveIds) {
        String actual = dataById.get(id);
        String want = expected.get(id);
        if (want == null || !want.equals(actual)) {
          valuesMatch = false;
        }
      }
      if (!valuesMatch) {
        System.out.println(
            "FAIL scan-exec-d2: live (id,data) set mismatch: java-read="
                + dataById
                + " expected={10=a, 30=c, 50=e}");
        failures++;
      } else {
        System.out.println(
            "PASS scan-exec-d2: Java read the Rust-written table → {(10,a),(30,c),(50,e)}");
      }

      if (failures == 0) {
        System.out.println(
            "verify-interop-scan-exec OK — Java read the RUST-written table (real parquet data + "
                + "Rust position-delete), merge-on-read live rows = {10,30,50}");
      }
      return failures;
    }
  }

  // ===========================================================================================
  // Equality-DELETE merge-on-read oracle — the SIBLING of ScanExecOracle. Same row shape, same
  // {10,30,50} live result, but the merge-on-read mechanism is delete-by-VALUE (an EQUALITY delete keyed on
  // a field id), not delete-by-POSITION. This is the second merge-on-read delete kind.
  //
  // THE TABLE (under <dir>/table). Unpartitioned V2, schema {1 id long required, 2 data string optional}:
  //   data file 00000-eq-data.parquet: rows (10,"a") (20,"b") (30,"c") (40,"d") (50,"e"), appended at the
  //     FIRST commit (data-sequence-number 1).
  //   equality-delete 00000-eq-deletes.parquet: equality_ids = [1] (the `id` field), delete rows id=20 and
  //     id=40, committed at the SECOND commit (sequence-number 2).
  //   live rows after merge-on-read = {10,30,50}.
  //
  // THE SEQUENCE ORDERING IS THE CORRECTNESS POINT. An equality delete applies to data files with a STRICTLY
  // LOWER data-sequence-number than the delete (spec merge-on-read rule). The data is committed FIRST (seq 1)
  // and the equality delete SECOND (seq 2), so the delete (seq 2) reaches the data (seq 1): 1 < 2. (Were the
  // order reversed, the delete would NOT apply and all 5 rows would survive.)
  //
  // Commits: newAppend(dataFile) then newRowDelta(eqDeleteFile) — real AVRO manifests + manifest-list land on
  // disk under <dir>/table/metadata, and final.metadata.json is written to a known path for the Rust test.
  // Java materializes its OWN merge-on-read read via IcebergGenerics into java_eq_scan_rows.json = {10,30,50}.
  // ===========================================================================================

  /**
   * The equality-delete half of the oracle — the by-VALUE merge-on-read interop. Builds an unpartitioned V2
   * table on local disk under {@code <dir>/table}, writes a REAL parquet data file (5 rows, appended at
   * sequence 1) + a REAL parquet EQUALITY-delete file (equality_ids = [1], delete rows id=20 and id=40,
   * committed at sequence 2 via {@link RowDelta}), writes {@code final.metadata.json}, and emits Java's OWN
   * merge-on-read read (via {@link IcebergGenerics}) as {@code java_eq_scan_rows.json}.
   */
  static final class EqDeleteOracle {
    private EqDeleteOracle() {}

    static void generate(Path dir) throws IOException {
      Files.createDirectories(dir);

      // 1. Build the unpartitioned V2 table on local disk under <dir>/table (bare absolute path, same
      //    convention as ScanExecOracle so the manifest/parquet paths resolve directly under Rust FileIO).
      File tableDir = dir.resolve("table").toFile();
      File metadataDir = new File(tableDir, "metadata");
      File dataDir = new File(tableDir, "data");
      if (!metadataDir.isDirectory() && !metadataDir.mkdirs()) {
        throw new IOException("failed to create metadata dir at " + metadataDir);
      }
      if (!dataDir.isDirectory() && !dataDir.mkdirs()) {
        throw new IOException("failed to create data dir at " + dataDir);
      }

      Schema schema =
          new Schema(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "data", Types.StringType.get()));
      PartitionSpec spec = PartitionSpec.unpartitioned();

      Map<String, String> props = new LinkedHashMap<>();
      props.put(TableProperties.FORMAT_VERSION, "2");
      TableMetadata seed =
          TableMetadata.newTableMetadata(
              schema, spec, SortOrder.unsorted(), tableDir.getAbsolutePath(), props);

      LocalTableOperations ops = new LocalTableOperations(tableDir, metadataDir);
      ops.commit(null, seed); // persist v0 metadata so the BaseTable has a current metadata to evolve.
      BaseTable table = new BaseTable(ops, "interop_eq_delete");

      // 2. Write a REAL parquet DATA file: 5 GenericRecords (10,"a")..(50,"e"). Built from the appender's
      //    REAL metrics + length + record_count (the FileHelpers "write records → DataFile" template).
      String dataPath = new File(dataDir, "00000-eq-data.parquet").getAbsolutePath();
      DataFile dataFile = writeDataFile(table, schema, spec, dataPath);

      // 3. Write a REAL parquet EQUALITY-delete file: equality_ids = [1] (the `id` field), delete rows
      //    id=20 and id=40. The delete-row schema is the single `id` column projected from the table schema
      //    (Java derives equalityFieldIds from that schema's column field ids).
      String deletePath = new File(dataDir, "00000-eq-deletes.parquet").getAbsolutePath();
      DeleteFile eqDeleteFile = writeEqDeleteFile(table, schema, spec, deletePath);

      // 4. Real commits, IN ORDER: the data file via newAppend (data-sequence-number 1), THEN the equality
      //    delete via newRowDelta (sequence-number 2). Because the data (seq 1) precedes the delete (seq 2),
      //    the equality delete applies to it (1 < 2). Live rows are now {10,30,50}.
      table.newAppend().appendFile(dataFile).commit();
      table.newRowDelta().addDeletes(eqDeleteFile).commit();

      // 5. Write the FINAL metadata to a KNOWN path so the Rust test loads it deterministically.
      Path finalMetadata = metadataDir.toPath().resolve("final.metadata.json");
      OutputFile finalOut =
          new LocalFileIO().newOutputFile(finalMetadata.toAbsolutePath().toString());
      TableMetadataParser.write(ops.current(), finalOut);

      // 6. Java materializes its OWN merge-on-read READ (IcebergGenerics applies the equality delete) →
      //    sort by id, emit java_eq_scan_rows.json = [{10,a},{30,c},{50,e}] (20 and 40 deleted by VALUE).
      writeJson(dir.resolve("java_eq_scan_rows.json"), readLiveRowsToJson(table));
      System.out.println("generated equality-delete table + java_eq_scan_rows.json to " + dir);
    }

    /**
     * Write a REAL parquet data file via the generic appender and return its {@link DataFile} built from the
     * appender's REAL metrics. Identical to {@link ScanExecOracle}'s data file: 5 rows (10,"a")..(50,"e").
     */
    private static DataFile writeDataFile(
        BaseTable table, Schema schema, PartitionSpec spec, String path) throws IOException {
      List<Record> rows = new ArrayList<>();
      long[] ids = {10L, 20L, 30L, 40L, 50L};
      String[] values = {"a", "b", "c", "d", "e"};
      for (int i = 0; i < ids.length; i++) {
        GenericRecord record = GenericRecord.create(schema);
        record.setField("id", ids[i]);
        record.setField("data", values[i]);
        rows.add(record);
      }

      GenericAppenderFactory factory = new GenericAppenderFactory(schema, spec);
      OutputFile out = table.io().newOutputFile(path);
      DataWriter<Record> writer =
          factory.newDataWriter(
              org.apache.iceberg.encryption.EncryptedFiles.encryptedOutput(
                  out, org.apache.iceberg.encryption.EncryptionKeyMetadata.EMPTY),
              FileFormat.PARQUET,
              null);
      try (Closeable toClose = writer) {
        writer.write(rows);
      }
      return writer.toDataFile();
    }

    /**
     * Write a REAL parquet EQUALITY-delete file via the generic equality-delete writer
     * ({@link GenericAppenderFactory#newEqDeleteWriter}), keyed on field id 1 (the {@code id} column),
     * deleting the two rows id=20 and id=40, and return its {@link DeleteFile} built from the writer's REAL
     * metrics + the equality field ids (mirroring {@code FileHelpers.writeDeleteFile(..., deleteRowSchema)}).
     *
     * <p>The delete-row schema is the single-column projection {@code {1 id long}} of the table schema; Java
     * derives {@code equalityFieldIds = [1]} from that schema's column field ids. Each delete record carries
     * only the {@code id} value (no {@code data}), so a merge-on-read reader drops every data row whose
     * {@code id} equals 20 or 40.
     */
    private static DeleteFile writeEqDeleteFile(
        BaseTable table, Schema schema, PartitionSpec spec, String path) throws IOException {
      // The equality-delete row schema is the `id` column only (field id 1). The equality field ids are the
      // field ids of that schema's columns — here exactly [1].
      Schema eqDeleteRowSchema = schema.select("id");
      int[] equalityFieldIds =
          eqDeleteRowSchema.columns().stream().mapToInt(Types.NestedField::fieldId).toArray();

      List<Record> deletes = new ArrayList<>();
      for (long id : new long[] {20L, 40L}) {
        GenericRecord delete = GenericRecord.create(eqDeleteRowSchema);
        delete.setField("id", id);
        deletes.add(delete);
      }

      GenericAppenderFactory factory =
          new GenericAppenderFactory(schema, spec, equalityFieldIds, eqDeleteRowSchema, null);
      OutputFile out = table.io().newOutputFile(path);
      EqualityDeleteWriter<Record> writer =
          factory.newEqDeleteWriter(
              org.apache.iceberg.encryption.EncryptedFiles.encryptedOutput(
                  out, org.apache.iceberg.encryption.EncryptionKeyMetadata.EMPTY),
              FileFormat.PARQUET,
              null);
      try (Closeable toClose = writer) {
        writer.write(deletes);
      }
      return writer.toDeleteFile();
    }

    /**
     * Materialize Java's OWN merge-on-read READ of the table via {@link IcebergGenerics} (which APPLIES the
     * equality delete), collect the live rows, SORT by id, and serialize them to a JSON array of
     * {@code {id, data}}. This is the GROUND TRUTH = [{10,a},{30,c},{50,e}].
     */
    private static String readLiveRowsToJson(BaseTable table) {
      Map<Long, String> dataById = new LinkedHashMap<>();
      try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
        for (Record record : records) {
          Long id = (Long) record.getField("id");
          Object data = record.getField("data");
          dataById.put(id, data == null ? null : data.toString());
        }
      } catch (IOException error) {
        throw new RuntimeException("failed to read live rows via IcebergGenerics", error);
      }

      List<Long> ids = new ArrayList<>(dataById.keySet());
      ids.sort(Long::compareTo);
      return JsonUtil.generate(
          gen -> {
            gen.writeStartArray();
            for (Long id : ids) {
              gen.writeStartObject();
              gen.writeNumberField("id", id);
              String data = dataById.get(id);
              if (data == null) {
                gen.writeNullField("data");
              } else {
                gen.writeStringField("data", data);
              }
              gen.writeEndObject();
            }
            gen.writeEndArray();
          },
          true);
    }

    /**
     * DIRECTION 2 verify — read the RUST-written table (with a RUST equality delete) and assert the
     * merge-on-read rows. Mirrors {@link ScanExecOracle#verify} exactly, but the applied delete is an
     * equality delete, not a position delete.
     *
     * <p>Loads {@code <dir>/rust_table/metadata/final.metadata.json}, builds a {@link BaseTable} over a
     * {@link LocalFileIO} (so {@code io()} reads the real on-disk parquet + avro), reads every live row with
     * {@code IcebergGenerics.read(table).build()} (which APPLIES the Rust-written equality delete), sorts by
     * id, and asserts the rows equal {@code {(10,a),(30,c),(50,e)}} (ids 20/40 deleted by VALUE). A failure
     * here is a REAL write-incompatibility finding: Java could not read the Rust-written equality delete, or
     * read the WRONG rows.
     */
    static int verify(Path dir) {
      int failures = 0;
      Path finalMetadata =
          dir.resolve("rust_table").resolve("metadata").resolve("final.metadata.json");

      if (!Files.exists(finalMetadata)) {
        System.out.println(
            "FAIL eq-delete-d2: missing " + finalMetadata + " (run the Rust GEN path first)");
        return 1;
      }

      TableMetadata metadata;
      try {
        metadata =
            TableMetadataParser.fromJson(finalMetadata.toString(), readString(finalMetadata));
      } catch (RuntimeException | IOException parseError) {
        System.out.println(
            "FAIL eq-delete-d2: Java could not parse the Rust-written final.metadata.json: "
                + parseError);
        return 1;
      }

      FileIO io = new LocalFileIO();
      BaseTable table = new BaseTable(new InMemoryInspectionOperations(metadata, io), "rust_table");

      Map<Long, String> dataById = new LinkedHashMap<>();
      try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
        for (Record record : records) {
          Long id = (Long) record.getField("id");
          Object data = record.getField("data");
          dataById.put(id, data == null ? null : data.toString());
        }
      } catch (RuntimeException | IOException readError) {
        System.out.println(
            "FAIL eq-delete-d2: Java could not READ the Rust-written table via IcebergGenerics "
                + "(manifests/parquet/equality-delete incompatibility): "
                + readError);
        return 1;
      }

      List<Long> liveIds = new ArrayList<>(dataById.keySet());
      liveIds.sort(Long::compareTo);

      // 3a. Exactly 3 live rows survive (5 written, ids 20 and 40 deleted by VALUE).
      if (liveIds.size() != 3) {
        System.out.println(
            "FAIL eq-delete-d2: expected 3 live rows after merge-on-read, got "
                + liveIds.size()
                + " "
                + liveIds);
        failures++;
      } else {
        System.out.println("PASS eq-delete-d2: 3 live rows survive merge-on-read");
      }

      // 3b. The deleted ids (20, 40) must be ABSENT — Rust's equality delete must have been applied.
      if (liveIds.contains(20L) || liveIds.contains(40L)) {
        System.out.println(
            "FAIL eq-delete-d2: deleted ids 20/40 must be ABSENT, but live set is " + liveIds);
        failures++;
      } else {
        System.out.println(
            "PASS eq-delete-d2: deleted ids 20/40 are absent (Rust's equality delete applied)");
      }

      // 3c. The exact surviving (id, data) set equals {(10,a),(30,c),(50,e)}.
      Map<Long, String> expected = new LinkedHashMap<>();
      expected.put(10L, "a");
      expected.put(30L, "c");
      expected.put(50L, "e");
      boolean valuesMatch = liveIds.equals(new ArrayList<>(expected.keySet()));
      for (Long id : liveIds) {
        String actual = dataById.get(id);
        String want = expected.get(id);
        if (want == null || !want.equals(actual)) {
          valuesMatch = false;
        }
      }
      if (!valuesMatch) {
        System.out.println(
            "FAIL eq-delete-d2: live (id,data) set mismatch: java-read="
                + dataById
                + " expected={10=a, 30=c, 50=e}");
        failures++;
      } else {
        System.out.println(
            "PASS eq-delete-d2: Java read the Rust-written table → {(10,a),(30,c),(50,e)}");
      }

      if (failures == 0) {
        System.out.println(
            "verify-interop-eq-delete OK — Java read the RUST-written table (real parquet data + "
                + "Rust equality-delete), merge-on-read live rows = {10,30,50}");
      }
      return failures;
    }
  }

  // ===========================================================================================
  // PARTITIONED scan-execution oracle — the PARTITION-HANDLING proof. The sibling of ScanExecOracle, but
  // the table is PARTITIONED by identity(category) and the position-delete is PARTITION-SCOPED. Every
  // earlier scan-exec / eq-delete oracle wrote an UNPARTITIONED table; this one proves that BOTH Java and
  // Rust agree on partition-aware merge-on-read: the data files carry partition values (category=a /
  // category=b) and the position-delete is associated with the partition-a data file.
  //
  // THE TABLE (under <dir>/table). V2, schema {1 id long required, 2 category string required, 3 data
  // string optional}, partition spec identity(category) (spec id 0):
  //   data file 00000-a.parquet (partition category=a): rows (10,a,x) (20,a,y) (30,a,z) at positions 0..2.
  //   data file 00000-b.parquet (partition category=b): rows (40,b,p) (50,b,q) at positions 0..1.
  //   position-delete 00000-a-deletes.parquet (partition category=a): deletes position 1 of the cat=a data
  //     file (row id=20). The delete is PARTITION-SCOPED — it carries the category=a partition Struct and
  //     references the cat=a data file path.
  //   live rows after merge-on-read = {10,30,40,50} (only id=20 deleted; both partitions otherwise intact).
  // Commits: newAppend(dataFileA, dataFileB) at sequence 1, then newRowDelta(deleteFileA) at sequence 2.
  // Java materializes its OWN merge-on-read read via IcebergGenerics into java_part_scan_rows.json.
  // ===========================================================================================

  /**
   * The partitioned scan-execution half of the oracle — the partition-aware merge-on-read interop. Builds a
   * V2 table partitioned by identity(category) on local disk under {@code <dir>/table}, writes one REAL
   * parquet data file PER PARTITION (each stamped with its partition value via a {@link PartitionData}) + a
   * PARTITION-SCOPED position-delete in partition a (deleting position 1 = id=20), commits them
   * ({@code newAppend} at sequence 1 + {@code newRowDelta} at sequence 2), writes {@code final.metadata.json},
   * and emits Java's OWN merge-on-read read (via {@link IcebergGenerics}) as {@code java_part_scan_rows.json}.
   */
  static final class PartScanExecOracle {
    private PartScanExecOracle() {}

    static void generate(Path dir) throws IOException {
      Files.createDirectories(dir);

      // 1. Build the PARTITIONED V2 table on local disk under <dir>/table. The location is the BARE absolute
      //    path so the manifest/manifest-list/data/delete paths the writers + commits use are bare absolute
      //    paths the Rust FileIO::new_with_fs() resolves directly (same convention as ScanExecOracle).
      File tableDir = dir.resolve("table").toFile();
      File metadataDir = new File(tableDir, "metadata");
      File dataDir = new File(tableDir, "data");
      if (!metadataDir.isDirectory() && !metadataDir.mkdirs()) {
        throw new IOException("failed to create metadata dir at " + metadataDir);
      }
      if (!dataDir.isDirectory() && !dataDir.mkdirs()) {
        throw new IOException("failed to create data dir at " + dataDir);
      }

      Schema schema =
          new Schema(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "category", Types.StringType.get()),
              Types.NestedField.optional(3, "data", Types.StringType.get()));
      // Partition by identity(category). The spec gets id 0 (the default spec of a fresh table).
      PartitionSpec spec = PartitionSpec.builderFor(schema).identity("category").build();

      Map<String, String> props = new LinkedHashMap<>();
      props.put(TableProperties.FORMAT_VERSION, "2");
      TableMetadata seed =
          TableMetadata.newTableMetadata(
              schema, spec, SortOrder.unsorted(), tableDir.getAbsolutePath(), props);

      LocalTableOperations ops = new LocalTableOperations(tableDir, metadataDir);
      ops.commit(null, seed); // persist v0 metadata so the BaseTable has a current metadata to evolve.
      BaseTable table = new BaseTable(ops, "interop_part_scan_exec");

      // 2. The two partition values (category=a / category=b) as PartitionData over the spec's partition
      //    type. PartitionData is a StructLike the data/pos-delete writers stamp onto the DataFile (and the
      //    location provider routes the parquet under the partition path data/category=a/...).
      Types.StructType partitionType = spec.partitionType();
      PartitionData partitionA = new PartitionData(partitionType);
      partitionA.set(0, "a");
      PartitionData partitionB = new PartitionData(partitionType);
      partitionB.set(0, "b");

      // 3. One REAL parquet DATA file PER PARTITION, each carrying its partition value. cat=a holds ids
      //    10/20/30 at positions 0..2; cat=b holds ids 40/50 at positions 0..1. The DataFile is built from
      //    the appender's REAL metrics + the partition (the FileHelpers.writeDataFile(partition) template).
      String dataPathA = new File(dataDir, "category=a/00000-a.parquet").getAbsolutePath();
      DataFile dataFileA =
          writePartitionedDataFile(
              table,
              schema,
              spec,
              partitionA,
              dataPathA,
              new long[] {10L, 20L, 30L},
              new String[] {"x", "y", "z"});

      String dataPathB = new File(dataDir, "category=b/00000-b.parquet").getAbsolutePath();
      DataFile dataFileB =
          writePartitionedDataFile(
              table,
              schema,
              spec,
              partitionB,
              dataPathB,
              new long[] {40L, 50L},
              new String[] {"p", "q"});

      // 4. A PARTITION-SCOPED position-delete in partition a, deleting position 1 of the cat=a data file
      //    (row id=20). It carries the category=a partition Struct (so it is associated with that partition)
      //    and references the cat=a data file path. cat=b is untouched.
      String deletePathA = new File(dataDir, "category=a/00000-a-deletes.parquet").getAbsolutePath();
      DeleteFile deleteFileA =
          writePartitionedPosDeleteFile(
              table, schema, spec, partitionA, deletePathA, dataFileA.path());

      // 5. Real commits, IN ORDER: both data files via newAppend (data-sequence-number 1), THEN the
      //    partition-scoped position-delete via newRowDelta (sequence-number 2). Live rows = {10,30,40,50}.
      table.newAppend().appendFile(dataFileA).appendFile(dataFileB).commit();
      table.newRowDelta().addDeletes(deleteFileA).commit();

      // 6. Write the FINAL metadata to a KNOWN path so the Rust test loads it deterministically. (The real
      //    on-disk manifest-list + manifests + parquet already live under <dir>/table.)
      Path finalMetadata = metadataDir.toPath().resolve("final.metadata.json");
      OutputFile finalOut =
          new LocalFileIO().newOutputFile(finalMetadata.toAbsolutePath().toString());
      TableMetadataParser.write(ops.current(), finalOut);

      // 7. Java materializes its OWN merge-on-read READ (IcebergGenerics applies the partition-scoped
      //    position delete) → sort by id, emit java_part_scan_rows.json = [{10,x},{30,z},{40,p},{50,q}]
      //    (id=20 deleted; both partitions otherwise intact). This is the GROUND TRUTH the Rust scan equals.
      writeJson(dir.resolve("java_part_scan_rows.json"), readLiveRowsToJson(table));
      System.out.println("generated partitioned scan-exec table + java_part_scan_rows.json to " + dir);
    }

    /**
     * Write a REAL parquet data file for ONE partition via the generic appender and return its
     * {@link DataFile} built from the appender's REAL metrics + the partition value (the
     * {@code FileHelpers.writeDataFile(table, out, partition, rows)} template). Each row's {@code category}
     * field matches the partition's category so the on-disk data is consistent with the partition stamp.
     */
    private static DataFile writePartitionedDataFile(
        BaseTable table,
        Schema schema,
        PartitionSpec spec,
        StructLike partition,
        String path,
        long[] ids,
        String[] dataValues)
        throws IOException {
      // The category value is the single partition field's value (identity(category)) — read it back from the
      // partition struct so the on-disk records carry exactly the partition's category.
      String category = partition.get(0, String.class);
      List<Record> rows = new ArrayList<>();
      for (int i = 0; i < ids.length; i++) {
        GenericRecord record = GenericRecord.create(schema);
        record.setField("id", ids[i]);
        record.setField("category", category);
        record.setField("data", dataValues[i]);
        rows.add(record);
      }

      GenericAppenderFactory factory = new GenericAppenderFactory(schema, spec);
      OutputFile out = table.io().newOutputFile(path);
      // newDataWriter(file, format, partition) stamps the partition onto the DataFile builder; toDataFile()
      // reads back the REAL metrics the parquet writer produced AND the partition value.
      DataWriter<Record> writer =
          factory.newDataWriter(
              org.apache.iceberg.encryption.EncryptedFiles.encryptedOutput(
                  out, org.apache.iceberg.encryption.EncryptionKeyMetadata.EMPTY),
              FileFormat.PARQUET,
              partition);
      try (Closeable toClose = writer) {
        writer.write(rows);
      }
      return writer.toDataFile();
    }

    /**
     * Write a REAL parquet POSITION-DELETE file for ONE partition via the generic position-delete writer,
     * deleting position 1 of {@code referencedDataPath} (row id=20), and return its {@link DeleteFile} built
     * from the writer's REAL metrics + the partition value (the {@code FileHelpers.writePosDeleteFile(table,
     * out, partition, deletes)} template). The delete is PARTITION-SCOPED: it carries {@code partition} (the
     * category=a Struct) and references the cat=a data file path, so a real merge-on-read reader masks
     * exactly that one row in that one partition.
     */
    private static DeleteFile writePartitionedPosDeleteFile(
        BaseTable table,
        Schema schema,
        PartitionSpec spec,
        StructLike partition,
        String path,
        CharSequence referencedDataPath)
        throws IOException {
      GenericAppenderFactory factory = new GenericAppenderFactory(schema, spec);
      OutputFile out = table.io().newOutputFile(path);
      PositionDeleteWriter<Record> writer =
          factory.newPosDeleteWriter(
              org.apache.iceberg.encryption.EncryptedFiles.encryptedOutput(
                  out, org.apache.iceberg.encryption.EncryptionKeyMetadata.EMPTY),
              FileFormat.PARQUET,
              partition);
      PositionDelete<Record> posDelete = PositionDelete.create();
      try (Closeable toClose = writer) {
        // Delete position 1 (the 2nd row of the cat=a data file: id 20). No row data is carried.
        writer.write(posDelete.set(referencedDataPath, 1L, null));
      }
      return writer.toDeleteFile();
    }

    /**
     * Materialize Java's OWN merge-on-read READ of the partitioned table via {@link IcebergGenerics} (which
     * plans the scan across both partitions, opens the parquet, AND applies the partition-scoped position
     * delete), collect the live rows, SORT by id, and serialize them to a JSON array of {@code {id, data}}.
     * This is the GROUND TRUTH = [{10,x},{30,z},{40,p},{50,q}] (id=20 deleted, both partitions otherwise
     * intact). Note: the emitted {@code data} column is field 3 (the optional string), NOT category — the
     * Rust test compares on (id, data) exactly as the unpartitioned oracle does.
     */
    private static String readLiveRowsToJson(BaseTable table) {
      Map<Long, String> dataById = new LinkedHashMap<>();
      try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
        for (Record record : records) {
          Long id = (Long) record.getField("id");
          Object data = record.getField("data");
          dataById.put(id, data == null ? null : data.toString());
        }
      } catch (IOException error) {
        throw new RuntimeException("failed to read live rows via IcebergGenerics", error);
      }

      List<Long> ids = new ArrayList<>(dataById.keySet());
      ids.sort(Long::compareTo);
      return JsonUtil.generate(
          gen -> {
            gen.writeStartArray();
            for (Long id : ids) {
              gen.writeStartObject();
              gen.writeNumberField("id", id);
              String data = dataById.get(id);
              if (data == null) {
                gen.writeNullField("data");
              } else {
                gen.writeStringField("data", data);
              }
              gen.writeEndObject();
            }
            gen.writeEndArray();
          },
          true);
    }

    /**
     * DIRECTION 2 verify — read the RUST-written PARTITIONED table (with a RUST partition-scoped position
     * delete) and assert the merge-on-read rows. Mirrors {@link ScanExecOracle#verify}, but the table is
     * partitioned by identity(category) and the applied delete is partition-scoped.
     *
     * <p>Loads {@code <dir>/rust_table/metadata/final.metadata.json}, builds a {@link BaseTable} over a
     * {@link LocalFileIO} (so {@code io()} reads the real on-disk parquet + avro), reads every live row with
     * {@code IcebergGenerics.read(table).build()} (which APPLIES the Rust-written partition-scoped position
     * delete), sorts by id, and asserts the rows equal {@code {(10,x),(30,z),(40,p),(50,q)}} (id=20 deleted,
     * partition a's survivors 10/30 AND partition b's 40/50 all present). A failure here is a REAL partition-
     * aware write-incompatibility finding: Java could not read the Rust-written partitioned table / partition-
     * scoped delete, or read the WRONG rows.
     */
    static int verify(Path dir) {
      int failures = 0;
      Path finalMetadata =
          dir.resolve("rust_table").resolve("metadata").resolve("final.metadata.json");

      if (!Files.exists(finalMetadata)) {
        System.out.println(
            "FAIL part-scan-d2: missing " + finalMetadata + " (run the Rust GEN path first)");
        return 1;
      }

      TableMetadata metadata;
      try {
        metadata =
            TableMetadataParser.fromJson(finalMetadata.toString(), readString(finalMetadata));
      } catch (RuntimeException | IOException parseError) {
        System.out.println(
            "FAIL part-scan-d2: Java could not parse the Rust-written final.metadata.json: "
                + parseError);
        return 1;
      }

      FileIO io = new LocalFileIO();
      BaseTable table = new BaseTable(new InMemoryInspectionOperations(metadata, io), "rust_table");

      Map<Long, String> dataById = new LinkedHashMap<>();
      try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
        for (Record record : records) {
          Long id = (Long) record.getField("id");
          Object data = record.getField("data");
          dataById.put(id, data == null ? null : data.toString());
        }
      } catch (RuntimeException | IOException readError) {
        System.out.println(
            "FAIL part-scan-d2: Java could not READ the Rust-written partitioned table via "
                + "IcebergGenerics (manifests/parquet/partition-scoped-position-delete incompatibility): "
                + readError);
        return 1;
      }

      List<Long> liveIds = new ArrayList<>(dataById.keySet());
      liveIds.sort(Long::compareTo);

      // 3a. Exactly 4 live rows survive (5 written across both partitions, position 1 of cat=a deleted).
      if (liveIds.size() != 4) {
        System.out.println(
            "FAIL part-scan-d2: expected 4 live rows after partition-aware merge-on-read, got "
                + liveIds.size()
                + " "
                + liveIds);
        failures++;
      } else {
        System.out.println("PASS part-scan-d2: 4 live rows survive partition-aware merge-on-read");
      }

      // 3b. The deleted id (20) must be ABSENT — Rust's partition-scoped position delete must have applied.
      if (liveIds.contains(20L)) {
        System.out.println(
            "FAIL part-scan-d2: deleted id 20 must be ABSENT, but live set is " + liveIds);
        failures++;
      } else {
        System.out.println("PASS part-scan-d2: deleted id 20 is absent (Rust's partition-scoped delete applied)");
      }

      // 3c. Both partitions are otherwise intact: cat=a survivors 10/30 AND cat=b's 40/50 must all be present.
      if (!liveIds.contains(10L)
          || !liveIds.contains(30L)
          || !liveIds.contains(40L)
          || !liveIds.contains(50L)) {
        System.out.println(
            "FAIL part-scan-d2: both partitions must be intact (cat=a 10/30 + cat=b 40/50), live set is "
                + liveIds);
        failures++;
      } else {
        System.out.println(
            "PASS part-scan-d2: both partitions intact (cat=a survivors 10/30 + cat=b untouched 40/50)");
      }

      // 3d. The exact surviving (id, data) set equals {(10,x),(30,z),(40,p),(50,q)}.
      Map<Long, String> expected = new LinkedHashMap<>();
      expected.put(10L, "x");
      expected.put(30L, "z");
      expected.put(40L, "p");
      expected.put(50L, "q");
      boolean valuesMatch = liveIds.equals(new ArrayList<>(expected.keySet()));
      for (Long id : liveIds) {
        String actual = dataById.get(id);
        String want = expected.get(id);
        if (want == null || !want.equals(actual)) {
          valuesMatch = false;
        }
      }
      if (!valuesMatch) {
        System.out.println(
            "FAIL part-scan-d2: live (id,data) set mismatch: java-read="
                + dataById
                + " expected={10=x, 30=z, 40=p, 50=q}");
        failures++;
      } else {
        System.out.println(
            "PASS part-scan-d2: Java read the Rust-written partitioned table → {(10,x),(30,z),(40,p),(50,q)}");
      }

      if (failures == 0) {
        System.out.println(
            "verify-interop-part-scan OK — Java read the RUST-written PARTITIONED table (real per-partition "
                + "parquet data + Rust partition-scoped position-delete), merge-on-read live rows = "
                + "{10,30,40,50}");
      }
      return failures;
    }
  }

  /**
   * A minimal INSTANCE-based {@link TableOperations} that COMMITS metadata to LOCAL DISK (mirroring the
   * Java test-only {@code TestTables.TestTableOperations} / {@code LocalTableOperations}). Unlike the
   * in-memory ops above, a commit here writes the new {@code TableMetadata} to {@code <metadataDir>/vN.
   * metadata.json} via {@link LocalFileIO}, and {@code io()} returns that {@link LocalFileIO} so a real
   * {@code newAppend()} / {@code newRowDelta()} writes its AVRO manifests + manifest-list to disk. Single
   * instance per table, single-threaded — no optimistic-concurrency machinery needed.
   */
  private static final class LocalTableOperations implements TableOperations {
    private final File tableDir;
    private final File metadataDir;
    private TableMetadata current;
    private int version = -1;
    private long lastSnapshotId = 0;

    LocalTableOperations(File tableDir, File metadataDir) {
      this.tableDir = tableDir;
      this.metadataDir = metadataDir;
    }

    @Override
    public TableMetadata current() {
      return current;
    }

    @Override
    public TableMetadata refresh() {
      return current;
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      if (base != current) {
        throw new CommitFailedException("stale base metadata");
      }
      this.version += 1;
      String fileName = String.format("v%d.metadata.json", version);
      File metadataFile = new File(metadataDir, fileName);
      OutputFile out = io().newOutputFile(metadataFile.getAbsolutePath());
      TableMetadataParser.write(metadata, out);
      // Strip pending changes and pin the on-disk metadata location, exactly as a real catalog commit does.
      this.current =
          TableMetadata.buildFrom(metadata)
              .discardChanges()
              .withMetadataLocation(metadataFile.getAbsolutePath())
              .build();
      for (Snapshot snapshot : current.snapshots()) {
        this.lastSnapshotId = Math.max(lastSnapshotId, snapshot.snapshotId());
      }
    }

    @Override
    public FileIO io() {
      return new LocalFileIO();
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return new File(metadataDir, fileName).getAbsolutePath();
    }

    @Override
    public LocationProvider locationProvider() {
      return LocationProviders.locationsFor(current.location(), current.properties());
    }

    @Override
    public long newSnapshotId() {
      long next = lastSnapshotId + 1;
      this.lastSnapshotId = next;
      return next;
    }
  }

  /**
   * A pure-disk {@link FileIO} mirroring {@code TestTables.LocalFileIO}: reads/writes via
   * {@link org.apache.iceberg.Files#localInput} / {@link org.apache.iceberg.Files#localOutput} (which strip
   * a leading {@code file:} prefix), so the AVRO manifests + manifest-list + metadata land on the local
   * filesystem at the BARE absolute paths the Rust {@code FileIO::new_with_fs()} resolves.
   */
  private static final class LocalFileIO implements FileIO {
    @Override
    public InputFile newInputFile(String path) {
      return org.apache.iceberg.Files.localInput(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return org.apache.iceberg.Files.localOutput(path);
    }

    @Override
    public void deleteFile(String path) {
      String localPath = path.startsWith("file:") ? path.replaceFirst("file:", "") : path;
      if (!new File(localPath).delete()) {
        throw new RuntimeException("failed to delete file: " + path);
      }
    }
  }

  // ===========================================================================================
  // IO helpers
  // ===========================================================================================

  private static void writeJson(Path path, String json) throws IOException {
    Files.write(path, json.getBytes(StandardCharsets.UTF_8));
  }

  private static String readString(Path path) throws IOException {
    return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
  }
}
