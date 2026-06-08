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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Types;

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
  // IO helpers
  // ===========================================================================================

  private static void writeJson(Path path, String json) throws IOException {
    Files.write(path, json.getBytes(StandardCharsets.UTF_8));
  }

  private static String readString(Path path) throws IOException {
    return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
  }
}
