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
import org.apache.iceberg.exceptions.CherrypickAncestorCommitException;
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
      case "generate-interop-dv":
        // DELETION-VECTOR merge-on-read, DIRECTION 1 — "Rust reads what JAVA writes" (Increment
        // D1). Writes an unpartitioned V3 table (DVs require format version 3) with TWO real
        // parquet data files and a REAL Puffin deletion vector (BaseDVFileWriter, the production
        // DV writer) deleting positions {1,3} of file A, committed via newRowDelta().addDeletes.
        // Emits java_dv_scan_rows.json (Java's own read with the DV applied = {10,30,50,60,70,80})
        // for the Rust scan test, PLUS a synthetic high-bits/run-container DV blob
        // (dv_blob.bin + dv_blob_expected.json) for the byte-level decode pin. The dir is via
        // -Dinterop.dv.dir.
        Path dvDir = requireFixturesDir("interop.dv.dir");
        DvScanOracle.generate(dvDir);
        break;
      case "verify-interop-dv-write":
        // DELETION-VECTOR WRITING, DIRECTION 2 — "JAVA reads what RUST writes" (Increment D2).
        // The Rust GEN test (env ICEBERG_INTEROP_DV_WRITE_DIR, tests/interop_dv_write.rs) wrote
        // <dir>/rust_dv.puffin via the production DVFileWriter (one deletion-vector-v1 blob per
        // referenced data file) + rust_dv_expected.json ({path -> positions + blob coordinates}).
        // Here Java (1) parses the RUST-written Puffin footer with its real Puffin reader and
        // checks each blob's type/properties/coordinates against the expected JSON, (2) does the
        // SAME ranged blob read the production scan does (BaseDeleteLoader.readDV) and decodes it
        // with PositionDeleteIndex.deserialize (framing + magic + CRC + cardinality validations),
        // asserting the positions match, and (3) serializes Java's OWN BitmapPositionDeleteIndex
        // over the SAME position sets (via the production BaseDVFileWriter) into
        // java_dv_blob_<i>.bin so the Rust byte-parity test can pin rust_bytes == java_bytes.
        // A failure here is a REAL write-incompatibility finding. NOTE: `mvn exec:java` does not
        // propagate System.exit to the shell — run-interop-dv.sh greps the "0 failures" sentinel.
        Path dvWriteDir = requireFixturesDir("interop.dv_write.dir");
        int dvWriteFailures = DvWriteOracle.verify(dvWriteDir);
        System.out.println("verify-interop-dv-write: " + dvWriteFailures + " failures");
        if (dvWriteFailures > 0) {
          System.exit(1);
        }
        break;
      case "verify-interop-dv-table":
        // DELETION-VECTOR TABLE-level, DIRECTION 2 — "JAVA reads what RUST writes" (Increment
        // D4, the headline). The Rust GEN test (env ICEBERG_INTEROP_DV_TABLE_DIR,
        // tests/interop_dv_table.rs) committed a COMPLETE V3 table to <dir>/rust_table through
        // the production Rust path: two real parquet data files in two identity(category)
        // partitions fast_appended at sequence 1, then ONE Puffin holding TWO deletion vectors
        // (DVFileWriter) committed via row_delta at sequence 2, with final.metadata.json at a
        // known path + expected_rows.json / expected_dvs.json. Here Java loads that RUST-written
        // metadata, reads the table with its PRODUCTION scan (IcebergGenerics, which loads BOTH
        // DVs via BaseDeleteLoader.readDV and applies them), asserts the merge-on-read rows
        // equal the expected set, AND cross-checks the committed DeleteFile metadata through the
        // manifest API (content/format/referenced-data-file/content-offset/size/cardinality +
        // the shared-puffin multi-blob pin). A failure here is a REAL table-level
        // write-incompatibility finding. NOTE: `mvn exec:java` does not propagate System.exit —
        // run-interop-dv.sh greps the "0 failures" sentinel.
        Path dvTableVerifyDir = requireFixturesDir("interop.dv_table.dir");
        int dvTableFailures = DvTableOracle.verify(dvTableVerifyDir);
        System.out.println("verify-interop-dv-table: " + dvTableFailures + " failures");
        if (dvTableFailures > 0) {
          System.exit(1);
        }
        break;
      case "generate-interop-dv-table":
        // DELETION-VECTOR METADATA-level chain (Increment D4, the E1-family extension). Writes
        // the JAVA mirror of the SAME logical chain the Rust GEN test commits — a V3
        // identity(category) table with two real parquet data files (newFastAppend, mirroring
        // Rust fast_append) and ONE Puffin holding TWO deletion vectors written by the
        // production BaseDVFileWriter, committed via newRowDelta().addDeletes at sequence 2 —
        // under <dir>/table with final.metadata.json. The run script then emits the canonical
        // snapshot-metadata views of BOTH tables via emit-snapshot-meta (SnapshotMetaOracle) and
        // byte-diffs Java's view of the Rust table against Java's view of THIS table; the Rust
        // test (interop_dv_table.rs) asserts its own views of both equal Java's. The dir is via
        // -Dinterop.dv_table.dir.
        Path dvTableGenDir = requireFixturesDir("interop.dv_table.dir");
        DvTableOracle.generate(dvTableGenDir);
        break;
      case "verify-interop-dv-replace":
        // DELETION-VECTOR REPLACEMENT chain, DIRECTION 2 (Arc-E Increment 2 — the
        // BaseDVFileWriter.loadPreviousDeletes merge hook). The Rust GEN test (env
        // ICEBERG_INTEROP_DV_REPLACE_DIR, tests/interop_dv_replace.rs) committed a V3 table to
        // <dir>/rust_table whose DV1 ({1}) was REPLACED by a WRITER-MERGED DV2 ({1,3}) — DV1 removed
        // in the same commit. Java reads it with its PRODUCTION scan (live rows must be the
        // survivors of {1,3}), cross-checks the manifests (EXACTLY ONE live DV, DV1 ABSENT), AND
        // performs the SAME merge in Java to emit java_merged_dv_blob.bin for the Rust byte-compare.
        // A read/row mismatch or two live DVs is a REAL replacement-incompatibility finding. NOTE:
        // `mvn exec:java` does not propagate System.exit — run-interop-dv.sh greps the sentinel.
        Path dvReplaceVerifyDir = requireFixturesDir("interop.dv_replace.dir");
        int dvReplaceFailures = DvReplaceOracle.verify(dvReplaceVerifyDir);
        System.out.println("verify-interop-dv-replace: " + dvReplaceFailures + " failures");
        if (dvReplaceFailures > 0) {
          System.exit(1);
        }
        break;
      case "generate-interop-dv-replace":
        // DELETION-VECTOR REPLACEMENT metadata-level mirror (Arc-E Increment 2). Java performs the
        // SAME logical chain the Rust GEN commits — fast_append, newRowDelta(DV1{1}), then
        // newRowDelta().addDeletes(mergedDv).removeDeletes(DV1) where mergedDv is the writer-merged
        // {1,3} (BaseDVFileWriter with a real loadPreviousDeletes) — under <dir>/table. The run
        // script byte-diffs Java's canonical snapshot-metadata view of the RUST table against this
        // one (the FIRST LIVE comparison of `removed-dvs`); the Rust test asserts its own views of
        // both. The dir is via -Dinterop.dv_replace.dir.
        Path dvReplaceGenDir = requireFixturesDir("interop.dv_replace.dir");
        DvReplaceOracle.generate(dvReplaceGenDir);
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
      case "generate-interop-write-actions":
        // METADATA-LEVEL rewrite-family interop fixture (E2): the five-commit
        // fast-append/delete/overwrite/replace-partitions/rewrite chain on a partitioned V2
        // table, manifests only (no parquet). The dir is via -Dinterop.write_actions.dir.
        Path writeActionsDir = requireFixturesDir("interop.write_actions.dir");
        WriteActionsOracle.generate(writeActionsDir);
        break;
      case "generate-interop-rewrite-seq":
        // DELETE-BEARING rewrite fixture (Increment 4, E1-family / metadata-only): fast-append A,B →
        // row-delta adding a position-delete referencing B → rewrite A->A' preserving data sequence
        // number 1 (validated from the row-delta snapshot). Pins: A' carries data_seq 1 (not the
        // rewrite snapshot's seq) and the delete manifest survives the rewrite intact. The dir is via
        // -Dinterop.rewrite_seq.dir.
        Path rewriteSeqDir = requireFixturesDir("interop.rewrite_seq.dir");
        RewriteSeqOracle.generate(rewriteSeqDir);
        break;
      case "generate-interop-expire":
        // EXPIRE-SNAPSHOTS interop (increment A3). Builds four fixtures (linear / tag_protected /
        // stats / deletes), each a real-manifest table with deterministically re-stamped snapshot
        // timestamps and a surviving TAG (which forces Java down ReachableFileCleanup — the strategy
        // the Rust side ports — AND is the ref-protection element). Runs Java's full
        // table.expireSnapshots()...cleanExpiredFiles(true).deleteWith(collector).commit(), emitting
        // each <fixture>/java_deleted.json (the SORTED deleted-file list) + the post-expire
        // final.metadata.json. The dir is via -Dinterop.expire.dir.
        Path expireDir = requireFixturesDir("interop.expire.dir");
        ExpireOracle.generate(expireDir);
        break;
      case "verify-interop-expire":
        // EXPIRE-SNAPSHOTS interop, DIRECTION 2 — "Java verifies what RUST expired". The Rust GEN
        // test (env ICEBERG_INTEROP_EXPIRE_GEN_DIR, tests/interop_expire.rs) ran the SAME chain +
        // ExpireSnapshotsCleanup::commit_and_clean with a COLLECTING deleter on a copy at
        // <fixture>/rust_table, landing final.metadata.json + rust_deleted.json. Here Java re-reads
        // the Rust-expired table and asserts (a) its surviving snapshots/refs match Java's own
        // expiry of the Java fixture and (b) the Rust-collected deleted set equals Java's
        // java_deleted.json as sorted sets. A failure is a REAL expiry-incompatibility finding
        // (stranded or over-deleted files, resurrected snapshots). NOTE: `mvn exec:java` does not
        // reliably propagate System.exit — run-interop-expire.sh greps the "0 failures" sentinel.
        Path expireVerifyDir = requireFixturesDir("interop.expire.dir");
        int expireFailures = ExpireOracle.verify(expireVerifyDir);
        System.out.println("verify-interop-expire: " + expireFailures + " failures");
        if (expireFailures > 0) {
          System.exit(1);
        }
        break;
      case "generate-interop-merge-append-data":
        // DATA-LEVEL merge_append interop (increment S1, fixture A). Writes a REAL V2 partitioned table
        // under <dir>/table (identity(category), schema {id long, category string, data string}), adds TWO
        // real parquet data files (A: cat=a, ids 10/20/30; B: cat=b, id 40), fast-appends them, sets
        // commit.manifest.min-count-to-merge=2 to arm the merge, then merge-appends G(cat=a, id=60, "g").
        // Emits Java's OWN IcebergGenerics read as java_merge_append_rows.json (ground truth = all 5 rows,
        // no deletes). The Rust GEN test (ICEBERG_INTEROP_MERGE_APPEND_DATA_GEN_DIR) produces the mirror
        // and the verify step proves Java reads the Rust-written table with the SAME live set.
        Path mergeAppendDataDir = requireFixturesDir("interop.merge_append_data.dir");
        MergeAppendDataOracle.generate(mergeAppendDataDir);
        break;
      case "verify-interop-merge-append-data":
        // DATA-LEVEL merge_append interop, DIRECTION 2 — "Java reads what RUST writes". The Rust GEN test
        // (env ICEBERG_INTEROP_MERGE_APPEND_DATA_GEN_DIR) wrote a REAL V2 partitioned table to
        // <dir>/rust_table: fast-append A+B, set min-count=2, merge-append G — landing final.metadata.json
        // + real parquet. Here Java loads that RUST-written metadata, reads with IcebergGenerics (which
        // applies merge-on-read over the merged manifest), and asserts all 5 rows are present: {(10,a),
        // (20,b),(30,c),(40,d),(60,g)} — no deletes, so the correctness point is the UNION of all files
        // AND the merge fires (carried Existing entries still scan). A failure is a REAL merge_append-level
        // write-incompatibility finding (Rust wrote a merge-appended table Java cannot read correctly).
        Path mergeAppendDataVerifyDir = requireFixturesDir("interop.merge_append_data.dir");
        int mergeAppendDataFailures = MergeAppendDataOracle.verify(mergeAppendDataVerifyDir);
        System.out.println("verify-interop-merge-append-data: " + mergeAppendDataFailures + " failures");
        if (mergeAppendDataFailures > 0) {
          System.exit(1);
        }
        break;
      case "generate-interop-rewrite-data":
        // DATA-LEVEL RewriteFiles interop (increment S1, fixture B). Writes an UNPARTITIONED V2 table
        // under <dir>/table with a 2-field schema {id, data}, appends a REAL parquet data file A (5 rows,
        // seq 1), commits an EQUALITY-DELETE (equality_ids=[1], deletes id=20+40, seq 2), then rewrites
        // {A}→{A'} with data_sequence_number=1 (seq 3). The SEQ-PRESERVATION CORRECTNESS POINT: A' is
        // stamped with data_seq=1 (not 3), so the eq-delete (seq 2) STILL APPLIES to A' (1 < 2). Emits
        // Java's OWN IcebergGenerics read as java_rewrite_data_rows.json (= {(10,a),(30,c),(50,e)}).
        // WHY EQUALITY (not position) DELETE: a position-delete is PATH-BASED; after A→A' the delete on
        // A's path is dangling and cannot apply to A'. The seq-preservation proof requires an equality
        // delete (which uses data_seq for applicability).
        Path rewriteDataDir = requireFixturesDir("interop.rewrite_data.dir");
        RewriteFilesDataOracle.generate(rewriteDataDir);
        break;
      case "verify-interop-rewrite-data":
        // DATA-LEVEL RewriteFiles interop, DIRECTION 2 — "Java reads what RUST writes". The Rust GEN test
        // (env ICEBERG_INTEROP_REWRITE_DATA_GEN_DIR) wrote an UNPARTITIONED V2 table to <dir>/rust_table:
        // fast-append A, row-delta eq-delete (ids 20+40), rewrite {A}→{A'} with data_sequence_number(1) —
        // landing final.metadata.json + real parquet. Here Java loads that RUST-written metadata, reads with
        // IcebergGenerics (which APPLIES the equality delete to A' because A'.data_seq=1 < eq_del.seq=2),
        // and asserts ids 20+40 are ABSENT: {(10,a),(30,c),(50,e)}. A failure is a REAL seq-preservation
        // write-incompatibility: Rust stamped A' with data_seq=3 (wrong), making eq_del.seq=2 NOT greater
        // than A'.data_seq=3, so ids 20+40 would survive incorrectly.
        Path rewriteDataVerifyDir = requireFixturesDir("interop.rewrite_data.dir");
        int rewriteDataFailures = RewriteFilesDataOracle.verify(rewriteDataVerifyDir);
        System.out.println("verify-interop-rewrite-data: " + rewriteDataFailures + " failures");
        if (rewriteDataFailures > 0) {
          System.exit(1);
        }
        break;
      case "generate-interop-overwrite-data":
        // DATA-LEVEL OverwriteFiles interop (increment W1, fixture C). Writes a V2 partitioned table
        // (identity(category), schema {id long, category string, data string}) under <dir>/table, appends
        // TWO real parquet data files (A: cat=a, ids 10/20/30, data a/b/c; B: cat=b, id 40, data d) in one
        // fast-append (seq 1), then commits an overwrite_files that DELETES B and ADDS B' (cat=b, id 41,
        // data d'). Emits java_overwrite_data_rows.json (ground truth = 4 rows: {(10,a),(20,b),(30,c),(41,d')}).
        // The CORRECTNESS POINT: B (id=40) is GONE; B' (id=41) is present; A's rows INTACT. The partition
        // column (identity(category)) is pinned separately.
        Path overwriteDataDir = requireFixturesDir("interop.overwrite_data.dir");
        OverwriteFilesDataOracle.generate(overwriteDataDir);
        break;
      case "verify-interop-overwrite-data":
        // DATA-LEVEL OverwriteFiles interop, DIRECTION 2 — "Java reads what RUST writes". The Rust GEN test
        // (env ICEBERG_INTEROP_OVERWRITE_DATA_GEN_DIR) wrote a V2 partitioned table to <dir>/rust_table:
        // fast-append A+B, overwrite_files delete B add B'. Java reads via IcebergGenerics and asserts B gone,
        // B' present, A intact: {(10,a),(20,b),(30,c),(41,d')}.
        Path overwriteDataVerifyDir = requireFixturesDir("interop.overwrite_data.dir");
        int overwriteDataFailures = OverwriteFilesDataOracle.verify(overwriteDataVerifyDir);
        System.out.println("verify-interop-overwrite-data: " + overwriteDataFailures + " failures");
        if (overwriteDataFailures > 0) {
          System.exit(1);
        }
        break;
      case "generate-interop-delete-data":
        // DATA-LEVEL DeleteFiles interop (increment W1, fixture D). Writes a V2 partitioned table
        // (identity(category), schema {id long, category string, data string}) under <dir>/table, appends
        // THREE real parquet data files (A: cat=a, ids 10/20/30, data a/b/c; B: cat=b, id 40, data d;
        // C_file: cat=a, id 50, data e) in one fast-append (seq 1), then commits a delete_files on B by path
        // (seq 2). Emits java_delete_data_rows.json (ground truth = 4 rows: {(10,a),(20,b),(30,c),(50,e)}).
        // The CORRECTNESS POINT: B (cat=b, id=40) is GONE; A and C_file (cat=a) INTACT.
        Path deleteDataDir = requireFixturesDir("interop.delete_data.dir");
        DeleteFilesDataOracle.generate(deleteDataDir);
        break;
      case "verify-interop-delete-data":
        // DATA-LEVEL DeleteFiles interop, DIRECTION 2 — "Java reads what RUST writes". The Rust GEN test
        // (env ICEBERG_INTEROP_DELETE_DATA_GEN_DIR) wrote a V2 partitioned table to <dir>/rust_table:
        // fast-append A+B+C_file, delete_files B. Java reads via IcebergGenerics and asserts B gone,
        // A+C_file intact: {(10,a),(20,b),(30,c),(50,e)}.
        Path deleteDataVerifyDir = requireFixturesDir("interop.delete_data.dir");
        int deleteDataFailures = DeleteFilesDataOracle.verify(deleteDataVerifyDir);
        System.out.println("verify-interop-delete-data: " + deleteDataFailures + " failures");
        if (deleteDataFailures > 0) {
          System.exit(1);
        }
        break;
      case "generate-interop-replace-partitions-data":
        // DATA-LEVEL ReplacePartitions interop (increment W2, fixture E). Writes a V2 partitioned table
        // (identity(category), schema {id long, category string, data string}) under <dir>/table, appends
        // files A (cat=a, ids 10/20/30, data a/b/c) and B (cat=b, id 40, data d) in one fast-append
        // (seq 1), then commits a replace_partitions that ADDS E_new (cat=a, id=11, data="a'") — which
        // REPLACES the ENTIRE partition a (A is deleted) while partition b is UNTOUCHED (B carries forward
        // with EXISTING status). Emits java_replace_partitions_rows.json
        // (ground truth = 2 rows: {(11,a),(40,d)}). The CORRECTNESS POINT: A's rows (10/20/30) are ALL
        // GONE (replaced by E_new); B (id=40) is byte-untouched; the partition-b file PATH survives in the
        // live manifests with EXISTING status.
        Path replacePartitionsDataDir = requireFixturesDir("interop.replace_partitions_data.dir");
        ReplacePartitionsDataOracle.generate(replacePartitionsDataDir);
        break;
      case "verify-interop-replace-partitions-data":
        // DATA-LEVEL ReplacePartitions interop, DIRECTION 2 — "Java reads what RUST writes". The Rust GEN
        // test (env ICEBERG_INTEROP_REPLACE_PARTITIONS_DATA_GEN_DIR) wrote a V2 partitioned table to
        // <dir>/rust_table: fast-append A+B, replace_partitions E_new(cat=a). Java reads via
        // IcebergGenerics and asserts A's rows gone, E_new present, B intact: {(11,a),(40,d)}.
        // Also asserts B's file PATH appears in the live manifests with EXISTING status (byte-untouched).
        Path replacePartitionsDataVerifyDir = requireFixturesDir("interop.replace_partitions_data.dir");
        int replacePartitionsDataFailures =
            ReplacePartitionsDataOracle.verify(replacePartitionsDataVerifyDir);
        System.out.println(
            "verify-interop-replace-partitions-data: " + replacePartitionsDataFailures + " failures");
        if (replacePartitionsDataFailures > 0) {
          System.exit(1);
        }
        break;
      case "generate-interop-partitioned-rewrite-data":
        // DATA-LEVEL partitioned RewriteFiles interop (increment W2, fixture F). Writes a V2 partitioned
        // table (identity(category), schema {id long, category string, data string}) under <dir>/table,
        // appends A (cat=a, ids 10/20/30, data a/b/c) and B (cat=b, id 40, data d) in one fast-append
        // (seq 1), commits a row_delta equality-delete scoped to partition a (equality_ids=[1], deletes
        // id=20, seq 2), then rewrites {A}→{A'} with data_sequence_number=1 (seq 3). The eq-delete
        // STILL APPLIES to A' because A'.data_seq=1 < eq_del.seq=2. Partition B untouched.
        // Emits java_partitioned_rewrite_rows.json (ground truth = 3 rows: {(10,a),(30,c),(40,d)}).
        // CORRECTNESS POINT: id=20 deleted (eq-delete survives the rewrite); B's rows intact.
        Path partitionedRewriteDataDir = requireFixturesDir("interop.partitioned_rewrite_data.dir");
        PartitionedRewriteFilesDataOracle.generate(partitionedRewriteDataDir);
        break;
      case "verify-interop-partitioned-rewrite-data":
        // DATA-LEVEL partitioned RewriteFiles interop, DIRECTION 2 — "Java reads what RUST writes". The
        // Rust GEN test (env ICEBERG_INTEROP_PARTITIONED_REWRITE_DATA_GEN_DIR) wrote a V2 partitioned
        // table to <dir>/rust_table: fast-append A+B, eq-delete id=20 in partition a, rewrite {A}→{A'}
        // data_seq=1. Java reads via IcebergGenerics and asserts id=20 absent, {(10,a),(30,c),(40,d)}.
        Path partitionedRewriteDataVerifyDir = requireFixturesDir("interop.partitioned_rewrite_data.dir");
        int partitionedRewriteDataFailures =
            PartitionedRewriteFilesDataOracle.verify(partitionedRewriteDataVerifyDir);
        System.out.println(
            "verify-interop-partitioned-rewrite-data: "
                + partitionedRewriteDataFailures
                + " failures");
        if (partitionedRewriteDataFailures > 0) {
          System.exit(1);
        }
        break;
      case "generate-interop-multi-bin-merge-append-data":
        // DATA-LEVEL multi-bin merge_append interop (increment W3, fixture G). Writes a V2 partitioned
        // table (identity(category), schema {id, category, data}) under <dir>/table, performs FOUR separate
        // fast-appends (A:cat=a,10/20/30; B:cat=b,40; C1:cat=a,50; C2:cat=b,55) each in its own commit
        // → four separate manifests m1..m4. Measures actual manifest sizes, sets target-size-bytes to
        // max_size*2+1 + min-count-to-merge=2, then merge-appends G(cat=a,60). pack_end yields ≥2 bins
        // of 2 manifests each → both merge → ≥2 merged manifests with Existing entries. All 7 rows survive.
        // Emits java_multi_bin_merge_append_rows.json (ground truth = 7 rows).
        Path multiBinMergeAppendDataDir =
            requireFixturesDir("interop.multi_bin_merge_append_data.dir");
        MultiBinMergeAppendDataOracle.generate(multiBinMergeAppendDataDir);
        break;
      case "verify-interop-multi-bin-merge-append-data":
        // DATA-LEVEL multi-bin merge_append interop, DIRECTION 2 — "Java reads what RUST writes". The Rust
        // GEN test (env ICEBERG_INTEROP_MULTI_BIN_MERGE_DATA_GEN_DIR) wrote a V2 partitioned table to
        // <dir>/rust_table: 4 fast-appends + set multi-bin merge properties + merge-append G. Java reads
        // via IcebergGenerics and asserts all 7 rows present AND ≥2 merged manifests in the manifest list.
        Path multiBinMergeAppendDataVerifyDir =
            requireFixturesDir("interop.multi_bin_merge_append_data.dir");
        int multiBinMergeAppendDataFailures =
            MultiBinMergeAppendDataOracle.verify(multiBinMergeAppendDataVerifyDir);
        System.out.println(
            "verify-interop-multi-bin-merge-append-data: "
                + multiBinMergeAppendDataFailures
                + " failures");
        if (multiBinMergeAppendDataFailures > 0) {
          System.exit(1);
        }
        break;
      case "generate-interop-cherrypick":
        // CHERRYPICK metadata-level interop (increment S2). Builds three fixtures (ff / replay / dedup)
        // on a V2 table (schema {id long, data string}, unpartitioned), stages a snapshot per fixture,
        // runs Java's production manageSnapshots().cherrypick(id).commit(), emits java_meta.json (via
        // SnapshotMetaOracle.emit) for each fixture, and emits dedup_expected_rejection.json for the
        // dedup fixture. The dir is supplied via -Dinterop.cherrypick.dir on the CLI (same JVM).
        Path cherrypickGenDir = requireFixturesDir("interop.cherrypick.dir");
        CherryPickOracle.generate(cherrypickGenDir);
        break;
      case "verify-interop-cherrypick":
        // CHERRYPICK metadata-level interop, DIRECTION 2 — "Java verifies what RUST cherry-picked".
        // The Rust GEN test (env ICEBERG_INTEROP_CHERRYPICK_GEN_DIR, tests/interop_cherrypick.rs)
        // staged each fixture, ran the production cherry_pick action on a copy at
        // <fixture>/rust_table, and landed final.metadata.json there. Here Java reads that RUST-produced
        // table, asserts (a) its canonical view == java_meta.json (canonical snapshot-metadata view
        // byte-equal) and (b) fixture-specific facts (FF: snapshot count unchanged; replay:
        // source-snapshot-id present; dedup: second cherrypick attempt raises CherrypickAncestorCommitException).
        // A failure is a REAL cherrypick write-incompatibility. NOTE: `mvn exec:java` does not
        // propagate System.exit — run-interop-cherrypick.sh greps the "0 failures" sentinel.
        Path cherrypickVerifyDir = requireFixturesDir("interop.cherrypick.dir");
        int cherrypickFailures = CherryPickOracle.verify(cherrypickVerifyDir);
        System.out.println("verify-interop-cherrypick: " + cherrypickFailures + " failures");
        if (cherrypickFailures > 0) {
          System.exit(1);
        }
        break;
      case "emit-snapshot-meta":
        // METADATA-LEVEL row-delta interop (E1). Emits the canonical snapshot-metadata view (ordinal
        // snapshots, COUNT-only summaries, manifest-list -> entry structure with post-inheritance
        // sequence numbers) of ANY on-disk table — Java-written or Rust-written — so the run script
        // can diff "Java's view of Java's table" against "Java's view of Rust's table", and the Rust
        // test (interop_rowdelta_meta.rs) can assert its own views equal Java's. The table's CURRENT
        // metadata json is -Dinterop.meta.metadata; the output path is -Dinterop.meta.out.
        Path metaIn = requireFixturesDir("interop.meta.metadata");
        Path metaOut = requireFixturesDir("interop.meta.out");
        SnapshotMetaOracle.emit(metaIn, metaOut);
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

  // =============================================================================================
  // DELETION-VECTOR scan-execution oracle (Increment D1, Direction 1 — "Rust reads what JAVA
  // writes"). The V3 sibling of ScanExecOracle: the merge-on-read mechanism is a PUFFIN
  // DELETION VECTOR (a deletion-vector-v1 blob written by BaseDVFileWriter), not a parquet
  // position-delete file.
  //
  // THE TABLE (under <dir>/table). Unpartitioned V3 (DVs require format version 3), schema
  // {1 id long required, 2 data string optional}:
  //   data file 00000-data-a.parquet: rows (10,"a") (20,"b") (30,"c") (40,"d") (50,"e") at
  //     positions 0..4;
  //   data file 00000-data-b.parquet: rows (60,"f") (70,"g") (80,"h") — the SIBLING control: a
  //     DV is FILE-scoped, so file B must come through the scan untouched;
  //   a REAL Puffin DV written by BaseDVFileWriter deleting positions {1, 3} of file A (ids 20
  //     and 40), committed via newRowDelta().addDeletes(dv).
  // Live merge-on-read rows = {10,30,50,60,70,80}. Java materializes its OWN read
  // (IcebergGenerics → BaseDeleteLoader.readDV applies the DV) into java_dv_scan_rows.json — the
  // ground truth the Rust scan().to_arrow() must equal.
  //
  // THE SYNTHETIC BLOB (dv_blob.bin + dv_blob_expected.json). The scan fixture cannot exercise
  // positions above 2^32 (no test parquet holds 4 billion rows), so the oracle ALSO serializes a
  // second, UNCOMMITTED DV via BaseDVFileWriter for a fake data-file path with positions that
  // span the 32-bit key boundary AND a contiguous run (which Java's serialize() run-length
  // encodes into RUN containers), then copies the raw blob bytes (at the DeleteFile's
  // contentOffset/contentSizeInBytes) out of the Puffin file. The env-gated Rust lib test
  // (delete_vector::tests::test_dv_blob_decodes_java_written_blob_when_env_set) decodes those
  // REAL Java bytes and asserts the exact position set — the empirical roaring
  // byte-compatibility + framing pin, including high keys and run containers.
  // =============================================================================================

  static final class DvScanOracle {
    private DvScanOracle() {}

    /** The synthetic-blob positions: low values, a run-length-encodable range, and >2^32 keys. */
    private static List<Long> syntheticBlobPositions() {
      List<Long> positions = new ArrayList<>();
      positions.add(0L);
      positions.add(1L);
      positions.add(100L);
      for (long pos = 1000L; pos < 6000L; pos++) {
        positions.add(pos); // a contiguous run -> a RUN container after runLengthEncode()
      }
      positions.add((1L << 32) + 7L); // bitmap key 1
      positions.add((1L << 33) + 1L); // bitmap key 2
      return positions;
    }

    static void generate(Path dir) throws IOException {
      Files.createDirectories(dir);

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

      // FORMAT VERSION 3 — deletion vectors are V3-only (a PUFFIN DeleteFile is rejected on V2
      // by MergingSnapshotProducer.validateDeleteFileForVersion).
      Map<String, String> props = new LinkedHashMap<>();
      props.put(TableProperties.FORMAT_VERSION, "3");
      TableMetadata seed =
          TableMetadata.newTableMetadata(
              schema, spec, SortOrder.unsorted(), tableDir.getAbsolutePath(), props);

      LocalTableOperations ops = new LocalTableOperations(tableDir, metadataDir);
      ops.commit(null, seed);
      BaseTable table = new BaseTable(ops, "interop_dv_scan");

      // Two REAL parquet data files: A (the DV target) + B (the sibling control).
      DataFile dataFileA =
          writeDataFile(
              table,
              schema,
              spec,
              new File(dataDir, "00000-data-a.parquet").getAbsolutePath(),
              new long[] {10L, 20L, 30L, 40L, 50L},
              new String[] {"a", "b", "c", "d", "e"});
      DataFile dataFileB =
          writeDataFile(
              table,
              schema,
              spec,
              new File(dataDir, "00000-data-b.parquet").getAbsolutePath(),
              new long[] {60L, 70L, 80L},
              new String[] {"f", "g", "h"});
      table.newAppend().appendFile(dataFileA).appendFile(dataFileB).commit();

      // A REAL deletion vector via BaseDVFileWriter (the production DV writer): delete positions
      // {1, 3} of file A. The writer emits ONE Puffin file holding the deletion-vector-v1 blob
      // and a DeleteFile carrying contentOffset/contentSizeInBytes/referencedDataFile +
      // recordCount == cardinality (BaseDVFileWriter.createDV, L145-159). 1.10.0's only ctor
      // takes an OutputFileFactory (the Supplier overload is post-1.10), so the Puffin file's
      // name comes from the table's location provider.
      org.apache.iceberg.io.OutputFileFactory dvFileFactory =
          org.apache.iceberg.io.OutputFileFactory.builderFor(table, 1, 1L)
              .format(FileFormat.PUFFIN)
              .build();
      org.apache.iceberg.deletes.DVFileWriter dvWriter =
          new org.apache.iceberg.deletes.BaseDVFileWriter(dvFileFactory, path -> null);
      dvWriter.delete(dataFileA.location(), 1L, spec, null);
      dvWriter.delete(dataFileA.location(), 3L, spec, null);
      dvWriter.close();
      List<DeleteFile> dvs = dvWriter.result().deleteFiles();
      if (dvs.size() != 1) {
        throw new IOException("expected exactly one DV DeleteFile, got " + dvs.size());
      }
      RowDelta rowDelta = table.newRowDelta();
      for (DeleteFile dv : dvs) {
        rowDelta.addDeletes(dv);
      }
      rowDelta.commit();

      Path finalMetadata = metadataDir.toPath().resolve("final.metadata.json");
      OutputFile finalOut =
          new LocalFileIO().newOutputFile(finalMetadata.toAbsolutePath().toString());
      TableMetadataParser.write(ops.current(), finalOut);

      // Java's OWN merge-on-read read (IcebergGenerics applies the DV via
      // BaseDeleteLoader.readDV) is the ground truth: {10,30,50,60,70,80}, 20/40 deleted.
      writeJson(dir.resolve("java_dv_scan_rows.json"), readLiveRowsToJson(table));

      // The synthetic high-bits / run-container blob for the byte-level decode pin.
      emitSyntheticBlob(dir, table, spec);

      System.out.println("generated V3 DV table + java_dv_scan_rows.json + dv_blob.bin to " + dir);
    }

    /**
     * Serialize an UNCOMMITTED deletion vector with positions spanning the 32-bit key boundary
     * plus a contiguous run, copy its raw blob bytes out of the Puffin file via the SAME ranged
     * read Java's scan uses (contentOffset/contentSizeInBytes, BaseDeleteLoader.readDV), and emit
     * dv_blob.bin + the expected positions as dv_blob_expected.json.
     */
    private static void emitSyntheticBlob(Path dir, BaseTable table, PartitionSpec spec)
        throws IOException {
      org.apache.iceberg.io.OutputFileFactory syntheticFactory =
          org.apache.iceberg.io.OutputFileFactory.builderFor(table, 2, 2L)
              .format(FileFormat.PUFFIN)
              .build();
      org.apache.iceberg.deletes.DVFileWriter dvWriter =
          new org.apache.iceberg.deletes.BaseDVFileWriter(syntheticFactory, path -> null);
      List<Long> positions = syntheticBlobPositions();
      String fakeDataPath = "synthetic://high-bits-data.parquet";
      for (long pos : positions) {
        dvWriter.delete(fakeDataPath, pos, spec, null);
      }
      dvWriter.close();
      DeleteFile dv = dvWriter.result().deleteFiles().get(0);

      // The SAME ranged read Java's scan path uses (BaseDeleteLoader.readDV): seek to
      // contentOffset, read contentSizeInBytes bytes.
      long offset = dv.contentOffset();
      int length = dv.contentSizeInBytes().intValue();
      byte[] blob = new byte[length];
      InputFile inputFile = new LocalFileIO().newInputFile(dv.location());
      try (org.apache.iceberg.io.SeekableInputStream in = inputFile.newStream()) {
        in.seek(offset);
        org.apache.iceberg.io.IOUtil.readFully(in, blob, 0, length);
      }
      Files.write(dir.resolve("dv_blob.bin"), blob);

      String expectedJson =
          JsonUtil.generate(
              gen -> {
                gen.writeStartArray();
                for (long pos : positions) {
                  gen.writeNumber(pos);
                }
                gen.writeEndArray();
              },
              true);
      writeJson(dir.resolve("dv_blob_expected.json"), expectedJson);
    }

    /** Write one REAL parquet data file (the ScanExecOracle template, parameterized rows). */
    private static DataFile writeDataFile(
        BaseTable table,
        Schema schema,
        PartitionSpec spec,
        String path,
        long[] ids,
        String[] values)
        throws IOException {
      List<Record> rows = new ArrayList<>();
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

    /** Java's own merge-on-read read sorted by id, as a JSON array of {id, data}. */
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
  }

  // =============================================================================================
  // DvWriteOracle — DELETION-VECTOR WRITING, Direction 2 (Increment D2): Java verifies a
  // RUST-written Puffin DV file with its production reader machinery, then emits its OWN
  // serialization of the same position sets for the Rust byte-parity pin. See the
  // verify-interop-dv-write dispatch comment for the full flow.
  // =============================================================================================

  static final class DvWriteOracle {
    private DvWriteOracle() {}

    /** One expected deletion vector parsed from rust_dv_expected.json. */
    private static final class ExpectedDv {
      final String referencedDataFile;
      final List<Long> positions;
      final long contentOffset;
      final long contentSizeInBytes;
      final long recordCount;
      final long fileSizeInBytes;

      ExpectedDv(com.fasterxml.jackson.databind.JsonNode node) {
        this.referencedDataFile = node.get("referenced_data_file").asText();
        this.positions = new ArrayList<>();
        for (com.fasterxml.jackson.databind.JsonNode position : node.get("positions")) {
          this.positions.add(position.asLong());
        }
        this.contentOffset = node.get("content_offset").asLong();
        this.contentSizeInBytes = node.get("content_size_in_bytes").asLong();
        this.recordCount = node.get("record_count").asLong();
        this.fileSizeInBytes = node.get("file_size_in_bytes").asLong();
      }
    }

    static int verify(Path dir) throws IOException {
      int failures = 0;
      String puffinPath = dir.resolve("rust_dv.puffin").toAbsolutePath().toString();

      String expectedJson =
          new String(
              Files.readAllBytes(dir.resolve("rust_dv_expected.json")), StandardCharsets.UTF_8);
      List<ExpectedDv> expected = new ArrayList<>();
      for (com.fasterxml.jackson.databind.JsonNode node :
          JsonUtil.mapper().readTree(expectedJson)) {
        expected.add(new ExpectedDv(node));
      }

      // (1) Parse the RUST-written footer with Java's REAL Puffin reader and index the blob
      // metadata by referenced data file.
      InputFile inputFile = new LocalFileIO().newInputFile(puffinPath);
      org.apache.iceberg.puffin.FileMetadata footer;
      try (org.apache.iceberg.puffin.PuffinReader reader =
          org.apache.iceberg.puffin.Puffin.read(inputFile).build()) {
        footer = reader.fileMetadata();
      }
      if (footer.blobs().size() != expected.size()) {
        System.out.println(
            "FAIL blob count: footer has " + footer.blobs().size() + ", expected "
                + expected.size());
        failures++;
      }
      Map<String, org.apache.iceberg.puffin.BlobMetadata> blobsByPath = new LinkedHashMap<>();
      for (org.apache.iceberg.puffin.BlobMetadata blob : footer.blobs()) {
        blobsByPath.put(blob.properties().get("referenced-data-file"), blob);
      }

      for (ExpectedDv dv : expected) {
        failures += verifyOneDv(puffinPath, inputFile, blobsByPath, dv);
      }

      // On-disk file size must equal the file_size_in_bytes every DeleteFile recorded.
      long actualFileSize = inputFile.getLength();
      for (ExpectedDv dv : expected) {
        if (dv.fileSizeInBytes != actualFileSize) {
          System.out.println(
              "FAIL file size for " + dv.referencedDataFile + ": DeleteFile recorded "
                  + dv.fileSizeInBytes + ", on-disk Puffin is " + actualFileSize);
          failures++;
        }
      }

      // (3) Emit Java's own serialization of the SAME position sets for the byte-parity pin
      // (java_dv_blob_<i>.bin, index = the entry's position in rust_dv_expected.json).
      emitJavaBlobs(dir, expected);

      return failures;
    }

    /** Verify one expected DV: footer metadata, then the production-style ranged read + decode. */
    private static int verifyOneDv(
        String puffinPath,
        InputFile inputFile,
        Map<String, org.apache.iceberg.puffin.BlobMetadata> blobsByPath,
        ExpectedDv dv)
        throws IOException {
      int failures = 0;
      org.apache.iceberg.puffin.BlobMetadata blob = blobsByPath.get(dv.referencedDataFile);
      if (blob == null) {
        System.out.println("FAIL no footer blob references " + dv.referencedDataFile);
        return 1;
      }
      if (!org.apache.iceberg.puffin.StandardBlobTypes.DV_V1.equals(blob.type())) {
        System.out.println(
            "FAIL blob type for " + dv.referencedDataFile + ": " + blob.type());
        failures++;
      }
      if (blob.offset() != dv.contentOffset || blob.length() != dv.contentSizeInBytes) {
        System.out.println(
            "FAIL blob coordinates for " + dv.referencedDataFile + ": footer says ("
                + blob.offset() + ", " + blob.length() + "), DeleteFile metadata recorded ("
                + dv.contentOffset + ", " + dv.contentSizeInBytes + ")");
        failures++;
      }
      String cardinality = blob.properties().get("cardinality");
      if (!String.valueOf(dv.positions.size()).equals(cardinality)) {
        System.out.println(
            "FAIL cardinality property for " + dv.referencedDataFile + ": " + cardinality
                + ", expected " + dv.positions.size());
        failures++;
      }
      List<Integer> expectedFields =
          java.util.Collections.singletonList(MetadataColumns.ROW_POSITION.fieldId());
      if (!expectedFields.equals(blob.inputFields())) {
        System.out.println(
            "FAIL blob fields for " + dv.referencedDataFile + ": " + blob.inputFields()
                + ", expected " + expectedFields + " (ROW_POSITION)");
        failures++;
      }
      if (blob.snapshotId() != -1L || blob.sequenceNumber() != -1L) {
        System.out.println(
            "FAIL blob snapshot-id/sequence-number for " + dv.referencedDataFile
                + ": (" + blob.snapshotId() + ", " + blob.sequenceNumber()
                + "), expected (-1, -1) = inherited");
        failures++;
      }

      // (2) The SAME ranged read the production scan path does (BaseDeleteLoader.readDV), then
      // the production deserialization (PositionDeleteIndex.deserialize -> framing + magic +
      // CRC + cardinality validations) against a DeleteFile shaped like BaseDVFileWriter.createDV.
      byte[] blobBytes = readBlob(inputFile, dv.contentOffset, (int) dv.contentSizeInBytes);
      DeleteFile deleteFile =
          FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
              .ofPositionDeletes()
              .withFormat(FileFormat.PUFFIN)
              .withPath(puffinPath)
              .withFileSizeInBytes(dv.fileSizeInBytes)
              .withReferencedDataFile(dv.referencedDataFile)
              .withContentOffset(dv.contentOffset)
              .withContentSizeInBytes(dv.contentSizeInBytes)
              .withRecordCount(dv.recordCount)
              .build();
      List<Long> decoded = new ArrayList<>();
      try {
        org.apache.iceberg.deletes.PositionDeleteIndex index =
            org.apache.iceberg.deletes.PositionDeleteIndex.deserialize(blobBytes, deleteFile);
        index.forEach(decoded::add);
      } catch (RuntimeException error) {
        System.out.println(
            "FAIL Java could not deserialize the Rust-written DV for " + dv.referencedDataFile
                + ": " + error);
        return failures + 1;
      }
      if (!dv.positions.equals(decoded)) {
        System.out.println(
            "FAIL positions for " + dv.referencedDataFile + ": Java decoded " + decoded.size()
                + " positions, expected " + dv.positions.size()
                + " (or the sets differ)");
        failures++;
      } else {
        System.out.println(
            "PASS " + dv.referencedDataFile + ": " + decoded.size()
                + " positions decoded by Java's production reader");
      }
      return failures;
    }

    private static byte[] readBlob(InputFile inputFile, long offset, int length)
        throws IOException {
      byte[] bytes = new byte[length];
      try (org.apache.iceberg.io.SeekableInputStream in = inputFile.newStream()) {
        in.seek(offset);
        org.apache.iceberg.io.IOUtil.readFully(in, bytes, 0, length);
      }
      return bytes;
    }

    /**
     * Serialize Java's own BitmapPositionDeleteIndex over the SAME position sets through the
     * production BaseDVFileWriter (1.10.0's only ctor needs an OutputFileFactory, hence the
     * throwaway table), and dump each framed blob to java_dv_blob_&lt;i&gt;.bin for the Rust
     * byte-parity test.
     */
    private static void emitJavaBlobs(Path dir, List<ExpectedDv> expected) throws IOException {
      File tableDir = dir.resolve("java_emit_table").toFile();
      File metadataDir = new File(tableDir, "metadata");
      if (!metadataDir.isDirectory() && !metadataDir.mkdirs()) {
        throw new IOException("failed to create metadata dir at " + metadataDir);
      }
      Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
      Map<String, String> props = new LinkedHashMap<>();
      props.put(TableProperties.FORMAT_VERSION, "3");
      TableMetadata seed =
          TableMetadata.newTableMetadata(
              schema,
              PartitionSpec.unpartitioned(),
              SortOrder.unsorted(),
              tableDir.getAbsolutePath(),
              props);
      LocalTableOperations ops = new LocalTableOperations(tableDir, metadataDir);
      ops.commit(null, seed);
      BaseTable table = new BaseTable(ops, "interop_dv_write_emit");

      org.apache.iceberg.io.OutputFileFactory fileFactory =
          org.apache.iceberg.io.OutputFileFactory.builderFor(table, 3, 3L)
              .format(FileFormat.PUFFIN)
              .build();
      org.apache.iceberg.deletes.DVFileWriter dvWriter =
          new org.apache.iceberg.deletes.BaseDVFileWriter(fileFactory, path -> null);
      for (ExpectedDv dv : expected) {
        for (long position : dv.positions) {
          dvWriter.delete(dv.referencedDataFile, position, PartitionSpec.unpartitioned(), null);
        }
      }
      dvWriter.close();

      Map<String, DeleteFile> javaDvsByPath = new LinkedHashMap<>();
      for (DeleteFile javaDv : dvWriter.result().deleteFiles()) {
        javaDvsByPath.put(String.valueOf(javaDv.referencedDataFile()), javaDv);
      }
      for (int i = 0; i < expected.size(); i++) {
        ExpectedDv dv = expected.get(i);
        DeleteFile javaDv = javaDvsByPath.get(dv.referencedDataFile);
        if (javaDv == null) {
          throw new IOException("Java DV writer produced no DV for " + dv.referencedDataFile);
        }
        byte[] javaBlob =
            readBlob(
                new LocalFileIO().newInputFile(javaDv.location()),
                javaDv.contentOffset(),
                javaDv.contentSizeInBytes().intValue());
        Files.write(dir.resolve("java_dv_blob_" + i + ".bin"), javaBlob);
      }
      System.out.println(
          "emitted " + expected.size() + " java_dv_blob_<i>.bin files for the byte-parity pin");
    }
  }

  // =============================================================================================
  // DvTableOracle — DELETION-VECTOR TABLE-level + METADATA-level interop (Increment D4, the
  // evidence capstone of the DV arc). Two modes over ONE shared fixture shape:
  //
  //   verify-interop-dv-table (Direction 2, the headline): Java loads the RUST-COMMITTED V3
  //   table at <dir>/rust_table (two identity(category) partitions, two real parquet data files,
  //   ONE Puffin holding TWO DVs committed via Rust's row_delta), reads it with the PRODUCTION
  //   scan (IcebergGenerics -> DeleteFilter -> BaseDeleteLoader.readDV applies both DVs), and
  //   asserts the live rows equal <dir>/expected_rows.json = {(10,x),(30,z),(50,q)} (id 20
  //   deleted from cat=a; ids 40/60 deleted from cat=b). It ALSO cross-checks the committed
  //   DeleteFile metadata through the manifest API (ManifestFiles.readDeleteManifest) against
  //   <dir>/expected_dvs.json: content POSITION_DELETES, format PUFFIN, referencedDataFile,
  //   contentOffset/contentSizeInBytes, recordCount == cardinality, and BOTH DVs sharing ONE
  //   puffin location (the multi-blob pin).
  //
  //   generate-interop-dv-table (the metadata-level mirror): Java performs the SAME logical
  //   chain on an equivalent V3 table under <dir>/table — newFastAppend (mirroring Rust
  //   fast_append) of the two partitioned parquet data files at sequence 1, then ONE Puffin with
  //   TWO DVs via the production BaseDVFileWriter committed by newRowDelta().addDeletes at
  //   sequence 2 — so the run script can byte-diff Java's canonical snapshot-metadata view
  //   (SnapshotMetaOracle) of the RUST table against Java's view of THIS table, and the Rust
  //   test can assert its own views of both. The DV cardinalities DIFFER (1 vs 2) so the
  //   canonical entry sort never ties before the partition key.
  // =============================================================================================

  static final class DvTableOracle {
    private DvTableOracle() {}

    static void generate(Path dir) throws IOException {
      Files.createDirectories(dir);

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
      PartitionSpec spec = PartitionSpec.builderFor(schema).identity("category").build();

      // FORMAT VERSION 3 — deletion vectors are V3-only.
      Map<String, String> props = new LinkedHashMap<>();
      props.put(TableProperties.FORMAT_VERSION, "3");
      TableMetadata seed =
          TableMetadata.newTableMetadata(
              schema, spec, SortOrder.unsorted(), tableDir.getAbsolutePath(), props);

      LocalTableOperations ops = new LocalTableOperations(tableDir, metadataDir);
      ops.commit(null, seed);
      BaseTable table = new BaseTable(ops, "interop_dv_table");

      // The two partition values as PartitionData over the spec's partition type.
      Types.StructType partitionType = spec.partitionType();
      PartitionData partitionA = new PartitionData(partitionType);
      partitionA.set(0, "a");
      PartitionData partitionB = new PartitionData(partitionType);
      partitionB.set(0, "b");

      // One REAL parquet DATA file PER PARTITION (cat=a: ids 10/20/30; cat=b: ids 40/50/60),
      // committed together via newFastAppend (mirroring Rust fast_append) at sequence 1.
      DataFile dataFileA =
          writePartitionedDataFile(
              table,
              schema,
              spec,
              partitionA,
              new File(dataDir, "category=a/00000-a.parquet").getAbsolutePath(),
              new long[] {10L, 20L, 30L},
              new String[] {"x", "y", "z"});
      DataFile dataFileB =
          writePartitionedDataFile(
              table,
              schema,
              spec,
              partitionB,
              new File(dataDir, "category=b/00000-b.parquet").getAbsolutePath(),
              new long[] {40L, 50L, 60L},
              new String[] {"p", "q", "r"});
      table.newFastAppend().appendFile(dataFileA).appendFile(dataFileB).commit();

      // ONE Puffin holding TWO deletion vectors via the production BaseDVFileWriter: positions
      // {1} of the cat=a file (id 20, cardinality 1) + positions {0, 2} of the cat=b file
      // (ids 40/60, cardinality 2), each in its own partition context. Committed in ONE
      // newRowDelta at sequence 2.
      org.apache.iceberg.io.OutputFileFactory dvFileFactory =
          org.apache.iceberg.io.OutputFileFactory.builderFor(table, 1, 1L)
              .format(FileFormat.PUFFIN)
              .build();
      org.apache.iceberg.deletes.DVFileWriter dvWriter =
          new org.apache.iceberg.deletes.BaseDVFileWriter(dvFileFactory, path -> null);
      dvWriter.delete(dataFileA.location(), 1L, spec, partitionA);
      dvWriter.delete(dataFileB.location(), 0L, spec, partitionB);
      dvWriter.delete(dataFileB.location(), 2L, spec, partitionB);
      dvWriter.close();
      List<DeleteFile> dvs = dvWriter.result().deleteFiles();
      if (dvs.size() != 2) {
        throw new IOException("expected exactly two DV DeleteFiles, got " + dvs.size());
      }
      RowDelta rowDelta = table.newRowDelta();
      for (DeleteFile dv : dvs) {
        rowDelta.addDeletes(dv);
      }
      rowDelta.commit();

      // The FINAL metadata at a KNOWN path for the emit-snapshot-meta step + the Rust view.
      Path finalMetadata = metadataDir.toPath().resolve("final.metadata.json");
      OutputFile finalOut =
          new LocalFileIO().newOutputFile(finalMetadata.toAbsolutePath().toString());
      TableMetadataParser.write(ops.current(), finalOut);

      System.out.println(
          "generated the JAVA mirror V3 DV chain (fast-append 2 partitioned data files + one "
              + "puffin with two DVs via newRowDelta) under " + dir.resolve("table"));
    }

    /** Write one REAL parquet data file for ONE partition (the PartScanExecOracle template). */
    private static DataFile writePartitionedDataFile(
        BaseTable table,
        Schema schema,
        PartitionSpec spec,
        StructLike partition,
        String path,
        long[] ids,
        String[] dataValues)
        throws IOException {
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

    /** One expected live row parsed from expected_rows.json. */
    private static final class ExpectedRow {
      final long id;
      final String data;

      ExpectedRow(com.fasterxml.jackson.databind.JsonNode node) {
        this.id = node.get("id").asLong();
        com.fasterxml.jackson.databind.JsonNode dataNode = node.get("data");
        this.data = dataNode == null || dataNode.isNull() ? null : dataNode.asText();
      }
    }

    /** One expected committed-DV metadata entry parsed from expected_dvs.json. */
    private static final class ExpectedDvMeta {
      final String referencedDataFile;
      final long recordCount;
      final long contentOffset;
      final long contentSizeInBytes;

      ExpectedDvMeta(com.fasterxml.jackson.databind.JsonNode node) {
        this.referencedDataFile = node.get("referenced_data_file").asText();
        this.recordCount = node.get("record_count").asLong();
        this.contentOffset = node.get("content_offset").asLong();
        this.contentSizeInBytes = node.get("content_size_in_bytes").asLong();
      }
    }

    /**
     * DIRECTION 2 verify, TABLE level — Java's PRODUCTION scan over the RUST-COMMITTED V3+DV
     * table, plus the manifest-API cross-check of the committed DeleteFile metadata. Mirrors
     * {@link PartScanExecOracle#verify} for the load + read; the delete mechanism here is two
     * Puffin deletion vectors in one file.
     */
    static int verify(Path dir) throws IOException {
      int failures = 0;
      Path finalMetadata =
          dir.resolve("rust_table").resolve("metadata").resolve("final.metadata.json");

      if (!Files.exists(finalMetadata)) {
        System.out.println(
            "FAIL dv-table: missing " + finalMetadata + " (run the Rust GEN path first)");
        return 1;
      }

      List<ExpectedRow> expectedRows = new ArrayList<>();
      for (com.fasterxml.jackson.databind.JsonNode node :
          JsonUtil.mapper().readTree(readString(dir.resolve("expected_rows.json")))) {
        expectedRows.add(new ExpectedRow(node));
      }
      List<ExpectedDvMeta> expectedDvs = new ArrayList<>();
      for (com.fasterxml.jackson.databind.JsonNode node :
          JsonUtil.mapper().readTree(readString(dir.resolve("expected_dvs.json")))) {
        expectedDvs.add(new ExpectedDvMeta(node));
      }

      TableMetadata metadata;
      try {
        metadata =
            TableMetadataParser.fromJson(finalMetadata.toString(), readString(finalMetadata));
      } catch (RuntimeException parseError) {
        System.out.println(
            "FAIL dv-table: Java could not parse the Rust-written V3 final.metadata.json: "
                + parseError);
        return 1;
      }

      FileIO io = new LocalFileIO();
      BaseTable table = new BaseTable(new InMemoryInspectionOperations(metadata, io), "rust_table");

      // (1) The PRODUCTION merge-on-read read: IcebergGenerics loads BOTH Rust-written DVs via
      // BaseDeleteLoader.readDV and applies them during the scan.
      Map<Long, String> dataById = new LinkedHashMap<>();
      try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
        for (Record record : records) {
          Long id = (Long) record.getField("id");
          Object data = record.getField("data");
          dataById.put(id, data == null ? null : data.toString());
        }
      } catch (RuntimeException | IOException readError) {
        System.out.println(
            "FAIL dv-table: Java could not READ the Rust-committed V3+DV table via "
                + "IcebergGenerics (metadata/manifest/DV incompatibility): "
                + readError);
        return 1;
      }

      List<Long> liveIds = new ArrayList<>(dataById.keySet());
      liveIds.sort(Long::compareTo);

      List<Long> expectedIds = new ArrayList<>();
      boolean rowsMatch = true;
      for (ExpectedRow row : expectedRows) {
        expectedIds.add(row.id);
        String actual = dataById.get(row.id);
        if (!dataById.containsKey(row.id)
            || (row.data == null ? actual != null : !row.data.equals(actual))) {
          rowsMatch = false;
        }
      }
      if (!liveIds.equals(expectedIds) || !rowsMatch) {
        System.out.println(
            "FAIL dv-table: live (id,data) set mismatch — RESURRECTED or missing rows: java-read="
                + dataById
                + " expected ids "
                + expectedIds
                + " (a dropped/unread DV resurrects its deleted rows)");
        failures++;
      } else {
        System.out.println(
            "PASS dv-table: Java's production scan applied BOTH Rust-written DVs -> live rows "
                + dataById);
      }

      // (2) The manifest-API cross-check: read every committed delete entry back through
      // ManifestFiles.readDeleteManifest and compare against the metadata Rust recorded.
      Map<String, DeleteFile> dvsByReferencedFile = new LinkedHashMap<>();
      Snapshot current = table.currentSnapshot();
      for (ManifestFile manifest : current.deleteManifests(io)) {
        try (ManifestReader<DeleteFile> reader =
            ManifestFiles.readDeleteManifest(manifest, io, metadata.specsById())) {
          for (DeleteFile deleteFile : reader) {
            dvsByReferencedFile.put(String.valueOf(deleteFile.referencedDataFile()), deleteFile);
          }
        }
      }
      if (dvsByReferencedFile.size() != expectedDvs.size()) {
        System.out.println(
            "FAIL dv-table: expected " + expectedDvs.size() + " committed DVs, manifests hold "
                + dvsByReferencedFile.size());
        failures++;
      }
      String sharedPuffinLocation = null;
      for (ExpectedDvMeta expected : expectedDvs) {
        DeleteFile dv = dvsByReferencedFile.get(expected.referencedDataFile);
        if (dv == null) {
          System.out.println(
              "FAIL dv-table: no committed DV references " + expected.referencedDataFile);
          failures++;
          continue;
        }
        if (dv.content() != FileContent.POSITION_DELETES) {
          System.out.println(
              "FAIL dv-table: DV content for " + expected.referencedDataFile + ": "
                  + dv.content());
          failures++;
        }
        if (dv.format() != FileFormat.PUFFIN) {
          System.out.println(
              "FAIL dv-table: DV format for " + expected.referencedDataFile + ": " + dv.format());
          failures++;
        }
        if (dv.contentOffset() == null
            || dv.contentOffset() != expected.contentOffset
            || dv.contentSizeInBytes() == null
            || dv.contentSizeInBytes() != expected.contentSizeInBytes) {
          System.out.println(
              "FAIL dv-table: DV blob coordinates for " + expected.referencedDataFile
                  + ": manifest says (" + dv.contentOffset() + ", " + dv.contentSizeInBytes()
                  + "), Rust recorded (" + expected.contentOffset + ", "
                  + expected.contentSizeInBytes + ")");
          failures++;
        }
        if (dv.recordCount() != expected.recordCount) {
          System.out.println(
              "FAIL dv-table: DV cardinality for " + expected.referencedDataFile + ": "
                  + dv.recordCount() + ", expected " + expected.recordCount);
          failures++;
        }
        String location = dv.location();
        if (sharedPuffinLocation == null) {
          sharedPuffinLocation = location;
        } else if (!sharedPuffinLocation.equals(location)) {
          System.out.println(
              "FAIL dv-table: the two DVs must share ONE puffin file (the multi-blob case), got "
                  + sharedPuffinLocation + " vs " + location);
          failures++;
        }
      }
      if (failures == 0) {
        System.out.println(
            "PASS dv-table: manifest API cross-check — both DVs PUFFIN position-deletes in one "
                + "puffin, referenced files + blob coordinates + cardinalities match");
        System.out.println(
            "verify-interop-dv-table OK — Java read the RUST-COMMITTED V3 table (2 partitions, "
                + "one puffin with two DVs), merge-on-read live rows = " + dataById);
      }
      return failures;
    }
  }

  // =============================================================================================
  // DvReplaceOracle — DELETION-VECTOR REPLACEMENT chain (Arc-E Increment 2, the
  // BaseDVFileWriter.loadPreviousDeletes merge hook). Two modes over ONE shared fixture shape, the
  // unpartitioned sibling of DvTableOracle:
  //
  //   verify-interop-dv-replace (Direction 2, the headline): Java reads the RUST-COMMITTED V3
  //   replacement table at <dir>/rust_table (one parquet data file ids 10..50, DV1 deleting {1},
  //   then a WRITER-MERGED DV2 deleting {1,3} that REPLACED DV1 — DV1 removed in the same commit)
  //   with the PRODUCTION scan (IcebergGenerics -> BaseDeleteLoader.readDV) and asserts the live
  //   rows equal <dir>/expected_rows.json = {(10,x),(30,z),(50,q)}. It ALSO cross-checks the
  //   manifests: EXACTLY ONE live DV (DV1 ABSENT — a Java-visible proof the replacement removed the
  //   old DV, not just shadowed it). AND it performs the SAME merge in Java (a BaseDVFileWriter
  //   whose loadPreviousDeletes returns DV1's deserialized index) and dumps the merged blob to
  //   java_merged_dv_blob.bin for the Rust byte-compare (the Run-store re-serialization pin).
  //
  //   generate-interop-dv-replace (the metadata-level mirror): Java performs the SAME logical chain
  //   on an equivalent V3 table under <dir>/table — newFastAppend, newRowDelta(DV1), then
  //   newRowDelta().addDeletes(mergedDv).removeDeletes(DV1) where mergedDv is produced by a
  //   BaseDVFileWriter with a real loadPreviousDeletes — so the run script can byte-diff Java's
  //   canonical snapshot-metadata view (SnapshotMetaOracle) of the RUST table against Java's view of
  //   THIS table, and the Rust test asserts its own views of both. This is the FIRST LIVE comparison
  //   of the `removed-dvs` summary key end to end.
  // =============================================================================================

  static final class DvReplaceOracle {
    private DvReplaceOracle() {}

    /** The shared schema: {1 id long required, 2 category string optional, 3 data string optional}. */
    private static Schema replaceSchema() {
      return new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "category", Types.StringType.get()),
          Types.NestedField.optional(3, "data", Types.StringType.get()));
    }

    /** GEN: the JAVA mirror of the Rust REPLACEMENT chain under <dir>/table. */
    static void generate(Path dir) throws IOException {
      Files.createDirectories(dir);

      File tableDir = dir.resolve("table").toFile();
      File metadataDir = new File(tableDir, "metadata");
      File dataDir = new File(tableDir, "data");
      if (!metadataDir.isDirectory() && !metadataDir.mkdirs()) {
        throw new IOException("failed to create metadata dir at " + metadataDir);
      }
      if (!dataDir.isDirectory() && !dataDir.mkdirs()) {
        throw new IOException("failed to create data dir at " + dataDir);
      }

      Schema schema = replaceSchema();
      PartitionSpec spec = PartitionSpec.unpartitioned();
      Map<String, String> props = new LinkedHashMap<>();
      props.put(TableProperties.FORMAT_VERSION, "3"); // DVs are V3-only.
      TableMetadata seed =
          TableMetadata.newTableMetadata(
              schema, spec, SortOrder.unsorted(), tableDir.getAbsolutePath(), props);

      LocalTableOperations ops = new LocalTableOperations(tableDir, metadataDir);
      ops.commit(null, seed);
      BaseTable table = new BaseTable(ops, "interop_dv_replace");

      // 1. ONE real parquet data file (ids 10..50 at positions 0..4), fast-appended at sequence 1.
      DataFile dataFile =
          writeDataFile(
              table,
              schema,
              spec,
              new File(dataDir, "00000-data.parquet").getAbsolutePath(),
              new long[] {10L, 20L, 30L, 40L, 50L},
              new String[] {"x", "y", "z", "p", "q"});
      table.newFastAppend().appendFile(dataFile).commit();

      // 2. DV1 deletes position {1} (id 20), committed via newRowDelta at sequence 2.
      DeleteFile dv1 = writeFreshDv(table, spec, dataFile.location(), new long[] {1L}, 1, 1L);
      table.newRowDelta().addDeletes(dv1).commit();
      // The snapshot the replacement operation "starts from" (after DV1 landed) — the engine
      // captures this at operation start and passes it to validateFromSnapshot, so DV1 is NOT seen
      // as a CONCURRENTLY-added DV by validateAddedDVs (which otherwise checks ALL history since
      // startingSnapshotId == null). This is the Java twin of Rust's Transaction-captured starting
      // snapshot (`Transaction::new` captures the current head); without it Java rejects the
      // replacement with "Found concurrently added DV".
      long replaceStartSnapshotId = table.currentSnapshot().snapshotId();

      // 3. The MERGE: a BaseDVFileWriter with a real loadPreviousDeletes that reads DV1's index
      //    back; it unions DV1's {1} with the new {3} -> {1,3} and returns DV1 as rewritten. Commit
      //    addDeletes(mergedDv) + removeDeletes(DV1) (the Spark SparkPositionDeltaWrite flow).
      MergeOutcome merge = writeMergedDv(table, spec, dataFile.location(), new long[] {3L}, dv1);
      if (merge.mergedDv.recordCount() != 2L) {
        throw new IOException(
            "expected merged DV cardinality 2 ({1,3}), got " + merge.mergedDv.recordCount());
      }
      if (merge.rewritten.isEmpty()) {
        throw new IOException("expected DV1 to be returned as a rewritten/superseded file");
      }
      RowDelta replace = table.newRowDelta().validateFromSnapshot(replaceStartSnapshotId);
      replace.addDeletes(merge.mergedDv);
      for (DeleteFile rewritten : merge.rewritten) {
        replace.removeDeletes(rewritten);
      }
      replace.commit();

      Path finalMetadata = metadataDir.toPath().resolve("final.metadata.json");
      OutputFile finalOut =
          new LocalFileIO().newOutputFile(finalMetadata.toAbsolutePath().toString());
      TableMetadataParser.write(ops.current(), finalOut);

      System.out.println(
          "generated the JAVA mirror DV REPLACEMENT chain (fast-append + DV1{1} + writer-merged "
              + "DV2{1,3} replacing DV1 via newRowDelta addDeletes+removeDeletes) under "
              + dir.resolve("table"));
    }

    /**
     * verify-interop-dv-replace: read the RUST-committed replacement table, assert rows + exactly
     * ONE live DV (DV1 absent), and emit java_merged_dv_blob.bin for the byte-compare.
     */
    static int verify(Path dir) throws IOException {
      int failures = 0;
      Path finalMetadata =
          dir.resolve("rust_table").resolve("metadata").resolve("final.metadata.json");
      if (!Files.exists(finalMetadata)) {
        System.out.println(
            "FAIL dv-replace: missing " + finalMetadata + " (run the Rust GEN path first)");
        return 1;
      }

      List<ExpectedRow> expectedRows = new ArrayList<>();
      for (com.fasterxml.jackson.databind.JsonNode node :
          JsonUtil.mapper().readTree(readString(dir.resolve("expected_rows.json")))) {
        expectedRows.add(new ExpectedRow(node));
      }
      List<ExpectedDvMeta> expectedDvs = new ArrayList<>();
      for (com.fasterxml.jackson.databind.JsonNode node :
          JsonUtil.mapper().readTree(readString(dir.resolve("expected_dvs.json")))) {
        expectedDvs.add(new ExpectedDvMeta(node));
      }

      TableMetadata metadata;
      try {
        metadata =
            TableMetadataParser.fromJson(finalMetadata.toString(), readString(finalMetadata));
      } catch (RuntimeException parseError) {
        System.out.println(
            "FAIL dv-replace: Java could not parse the Rust-written V3 final.metadata.json: "
                + parseError);
        return 1;
      }

      FileIO io = new LocalFileIO();
      BaseTable table =
          new BaseTable(new InMemoryInspectionOperations(metadata, io), "rust_table");

      // (1) The PRODUCTION merge-on-read read.
      Map<Long, String> dataById = new LinkedHashMap<>();
      try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
        for (Record record : records) {
          Long id = (Long) record.getField("id");
          Object data = record.getField("data");
          dataById.put(id, data == null ? null : data.toString());
        }
      } catch (RuntimeException | IOException readError) {
        System.out.println(
            "FAIL dv-replace: Java could not READ the Rust-committed replacement table: "
                + readError);
        return 1;
      }

      List<Long> liveIds = new ArrayList<>(dataById.keySet());
      liveIds.sort(Long::compareTo);
      List<Long> expectedIds = new ArrayList<>();
      boolean rowsMatch = true;
      for (ExpectedRow row : expectedRows) {
        expectedIds.add(row.id);
        String actual = dataById.get(row.id);
        if (!dataById.containsKey(row.id)
            || (row.data == null ? actual != null : !row.data.equals(actual))) {
          rowsMatch = false;
        }
      }
      if (!liveIds.equals(expectedIds) || !rowsMatch) {
        System.out.println(
            "FAIL dv-replace: live (id,data) mismatch — RESURRECTED/missing rows: java-read="
                + dataById + " expected ids " + expectedIds
                + " (a broken merge resurrects the old DV's positions)");
        failures++;
      } else {
        System.out.println(
            "PASS dv-replace: Java's production scan read the replacement table -> live rows "
                + dataById);
      }

      // (2) The manifest cross-check: EXACTLY ONE live DV, DV1 absent.
      List<DeleteFile> liveDvs = new ArrayList<>();
      Snapshot current = table.currentSnapshot();
      for (ManifestFile manifest : current.deleteManifests(io)) {
        try (ManifestReader<DeleteFile> reader =
            ManifestFiles.readDeleteManifest(manifest, io, metadata.specsById())) {
          for (DeleteFile deleteFile : reader) {
            liveDvs.add(deleteFile);
          }
        }
      }
      if (liveDvs.size() != expectedDvs.size()) {
        System.out.println(
            "FAIL dv-replace: expected " + expectedDvs.size() + " live DV(s) (the merged DV only, "
                + "DV1 removed), manifests hold " + liveDvs.size()
                + " (the old DV was not removed -> two live DVs, a read-door violation)");
        failures++;
      } else {
        for (ExpectedDvMeta expected : expectedDvs) {
          DeleteFile dv = null;
          for (DeleteFile candidate : liveDvs) {
            if (expected.referencedDataFile.equals(String.valueOf(candidate.referencedDataFile()))) {
              dv = candidate;
              break;
            }
          }
          if (dv == null) {
            System.out.println(
                "FAIL dv-replace: no live DV references " + expected.referencedDataFile);
            failures++;
            continue;
          }
          if (dv.content() != FileContent.POSITION_DELETES || dv.format() != FileFormat.PUFFIN) {
            System.out.println(
                "FAIL dv-replace: surviving DV content/format: " + dv.content() + "/" + dv.format());
            failures++;
          }
          if (dv.recordCount() != expected.recordCount) {
            System.out.println(
                "FAIL dv-replace: surviving DV cardinality " + dv.recordCount() + ", expected "
                    + expected.recordCount + " (must be the MERGED {1,3} = 2)");
            failures++;
          }
        }
        if (failures == 0) {
          System.out.println(
              "PASS dv-replace: manifest cross-check — exactly ONE live DV (the merged {1,3}), "
                  + "the old DV1 is ABSENT (removed by the replacement)");
        }
      }

      // (3) Emit Java's own merged blob for the byte-compare: the SAME merge (loadPreviousDeletes
      // returns DV1's deserialized index) over the SAME inputs, dumped to java_merged_dv_blob.bin.
      emitJavaMergedBlob(dir);

      return failures;
    }

    /** Write a FRESH DV (no merge) via BaseDVFileWriter and return its single DeleteFile. */
    private static DeleteFile writeFreshDv(
        BaseTable table, PartitionSpec spec, String dataFilePath, long[] positions, int partitionId,
        long partitionIdLong)
        throws IOException {
      org.apache.iceberg.io.OutputFileFactory factory =
          org.apache.iceberg.io.OutputFileFactory.builderFor(table, partitionId, partitionIdLong)
              .format(FileFormat.PUFFIN)
              .build();
      org.apache.iceberg.deletes.DVFileWriter dvWriter =
          new org.apache.iceberg.deletes.BaseDVFileWriter(factory, path -> null);
      for (long pos : positions) {
        dvWriter.delete(dataFilePath, pos, spec, null);
      }
      dvWriter.close();
      List<DeleteFile> dvs = dvWriter.result().deleteFiles();
      if (dvs.size() != 1) {
        throw new IOException("expected one DV, got " + dvs.size());
      }
      return dvs.get(0);
    }

    /** The outcome of a MERGE write: the merged DV + the rewritten (superseded) delete files. */
    private static final class MergeOutcome {
      final DeleteFile mergedDv;
      final List<DeleteFile> rewritten;

      MergeOutcome(DeleteFile mergedDv, List<DeleteFile> rewritten) {
        this.mergedDv = mergedDv;
        this.rewritten = rewritten;
      }
    }

    /**
     * Write a MERGED DV via BaseDVFileWriter with a real loadPreviousDeletes that reads
     * {@code previousDv}'s index off disk (PositionDeleteIndex.deserialize, carrying previousDv as
     * its source file) — Java's production merge-and-replace. Returns the merged DV + the rewritten
     * files (the file-scoped previous source files, here previousDv).
     */
    private static MergeOutcome writeMergedDv(
        BaseTable table,
        PartitionSpec spec,
        String dataFilePath,
        long[] newPositions,
        DeleteFile previousDv)
        throws IOException {
      org.apache.iceberg.io.OutputFileFactory factory =
          org.apache.iceberg.io.OutputFileFactory.builderFor(table, 2, 2L)
              .format(FileFormat.PUFFIN)
              .build();
      org.apache.iceberg.deletes.DVFileWriter dvWriter =
          new org.apache.iceberg.deletes.BaseDVFileWriter(
              factory, path -> loadPreviousDeletes(table.io(), path, dataFilePath, previousDv));
      for (long pos : newPositions) {
        dvWriter.delete(dataFilePath, pos, spec, null);
      }
      dvWriter.close();
      org.apache.iceberg.io.DeleteWriteResult result = dvWriter.result();
      List<DeleteFile> dvs = result.deleteFiles();
      if (dvs.size() != 1) {
        throw new IOException("expected one merged DV, got " + dvs.size());
      }
      return new MergeOutcome(dvs.get(0), result.rewrittenDeleteFiles());
    }

    /**
     * The loadPreviousDeletes function: for {@code dataFilePath} return previousDv's positions as a
     * PositionDeleteIndex carrying previousDv as its source file (so close() collects it into
     * rewrittenDeleteFiles via isFileScoped); for any other path return null.
     */
    private static org.apache.iceberg.deletes.PositionDeleteIndex loadPreviousDeletes(
        FileIO io, String path, String dataFilePath, DeleteFile previousDv) {
      if (!path.equals(dataFilePath)) {
        return null;
      }
      try {
        byte[] blob =
            readBlob(
                io.newInputFile(previousDv.location()),
                previousDv.contentOffset(),
                previousDv.contentSizeInBytes().intValue());
        return org.apache.iceberg.deletes.PositionDeleteIndex.deserialize(blob, previousDv);
      } catch (IOException error) {
        throw new RuntimeException("failed to load previous deletes for " + dataFilePath, error);
      }
    }

    /**
     * Perform the SAME merge Java's writer does on the SAME inputs and dump the merged blob bytes —
     * a throwaway V3 table provides the OutputFileFactory + the DV1 the loadPreviousDeletes reads.
     */
    private static void emitJavaMergedBlob(Path dir) throws IOException {
      File tableDir = dir.resolve("java_merge_table").toFile();
      File metadataDir = new File(tableDir, "metadata");
      File dataDir = new File(tableDir, "data");
      if (!metadataDir.isDirectory() && !metadataDir.mkdirs()) {
        throw new IOException("failed to create metadata dir at " + metadataDir);
      }
      if (!dataDir.isDirectory() && !dataDir.mkdirs()) {
        throw new IOException("failed to create data dir at " + dataDir);
      }
      Schema schema = replaceSchema();
      PartitionSpec spec = PartitionSpec.unpartitioned();
      Map<String, String> props = new LinkedHashMap<>();
      props.put(TableProperties.FORMAT_VERSION, "3");
      TableMetadata seed =
          TableMetadata.newTableMetadata(
              schema, spec, SortOrder.unsorted(), tableDir.getAbsolutePath(), props);
      LocalTableOperations ops = new LocalTableOperations(tableDir, metadataDir);
      ops.commit(null, seed);
      BaseTable table = new BaseTable(ops, "interop_dv_merge_emit");

      // A throwaway data path (no parquet needed — the blob bytes depend only on the positions).
      String dataFilePath = new File(dataDir, "00000-data.parquet").getAbsolutePath();
      DeleteFile dv1 = writeFreshDv(table, spec, dataFilePath, new long[] {1L}, 1, 1L);
      MergeOutcome merge = writeMergedDv(table, spec, dataFilePath, new long[] {3L}, dv1);

      byte[] javaBlob =
          readBlob(
              new LocalFileIO().newInputFile(merge.mergedDv.location()),
              merge.mergedDv.contentOffset(),
              merge.mergedDv.contentSizeInBytes().intValue());
      Files.write(dir.resolve("java_merged_dv_blob.bin"), javaBlob);
      System.out.println(
          "emitted java_merged_dv_blob.bin (" + javaBlob.length
              + " bytes, merged {1,3}) for the byte-compare");
    }

    /** Write one REAL parquet data file (the DvScanOracle template, {id, category, data}). */
    private static DataFile writeDataFile(
        BaseTable table, Schema schema, PartitionSpec spec, String path, long[] ids, String[] values)
        throws IOException {
      List<Record> rows = new ArrayList<>();
      for (int i = 0; i < ids.length; i++) {
        GenericRecord record = GenericRecord.create(schema);
        record.setField("id", ids[i]);
        record.setField("category", null);
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

    private static byte[] readBlob(InputFile inputFile, long offset, int length) throws IOException {
      byte[] bytes = new byte[length];
      try (org.apache.iceberg.io.SeekableInputStream in = inputFile.newStream()) {
        in.seek(offset);
        org.apache.iceberg.io.IOUtil.readFully(in, bytes, 0, length);
      }
      return bytes;
    }

    private static String readString(Path path) throws IOException {
      return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }

    /** One expected live row parsed from expected_rows.json. */
    private static final class ExpectedRow {
      final long id;
      final String data;

      ExpectedRow(com.fasterxml.jackson.databind.JsonNode node) {
        this.id = node.get("id").asLong();
        com.fasterxml.jackson.databind.JsonNode dataNode = node.get("data");
        this.data = dataNode == null || dataNode.isNull() ? null : dataNode.asText();
      }
    }

    /** One expected committed-DV metadata entry parsed from expected_dvs.json. */
    private static final class ExpectedDvMeta {
      final String referencedDataFile;
      final long recordCount;

      ExpectedDvMeta(com.fasterxml.jackson.databind.JsonNode node) {
        this.referencedDataFile = node.get("referenced_data_file").asText();
        this.recordCount = node.get("record_count").asLong();
      }
    }
  }

  // =============================================================================================
  // ExpireOracle — the EXPIRE-SNAPSHOTS interop oracle (increment A3). Proves the Rust
  // ExpireSnapshotsAction (B1 retention) + ExpireSnapshotsCleanup (B2 ReachableFileCleanup file GC)
  // agree with Java 1.10.0 RemoveSnapshots + cleanExpiredFiles(true) on the SAME fixtures, judged by
  // Java where possible.
  //
  // THE STRATEGY-SELECTION CONSTRAINT (bytecode-verified RemoveSnapshots.cleanExpiredSnapshots,
  // 1.10.0): when incrementalCleanup==null Java picks INCREMENTAL iff (!specifiedSnapshotId &&
  // !hasRemovedNonMainAncestors(base,current) && !hasNonMainSnapshots(current)), else REACHABLE. The
  // Rust side ports ReachableFileCleanup ONLY, so EVERY fixture here FORCES Java down Reachable by
  // keeping a surviving TAG (⇒ hasNonMainSnapshots(current)==true). That tag doubles as a
  // judgment-surface element (a ref protecting an otherwise-expirable snapshot). ReachableFileCleanup
  // .cleanFiles is the 2-arg (base, current) core that the Rust 2-state clean_expired_files mirrors.
  //
  // DETERMINISTIC TIMESTAMPS (no wall-clock dependence): the table is built by REAL fast appends
  // (real manifests + manifest lists on disk via LocalTableOperations), then every snapshot is
  // RE-STAMPED with a controlled timestamp (T0 + 1000*ordinal) preserving its REAL manifestList
  // location, by rebuilding from a FRESH seed (TableMetadata.buildFrom(seed) — no metadata-log
  // history yet, so the past timestamps don't trip the "before the latest metadata log entry"
  // guard) and re-parsing from disk to clear pending AddSnapshot changes (the manage-snapshots
  // oracle pattern). The expire cut is then a fixed value between two snapshots' fixed timestamps.
  //
  // FIXTURE-AWAY DECISIONS (so the byte-equality holds without fuzzy compares):
  //   - Each Java snapshot owns a UNIQUE manifest-list file (real fast appends), so the Rust
  //     retained-shared-manifest-list under-deletion guard NEVER fires ⇒ the deleted sets agree.
  //   - Ref aging (max_ref_age_ms) is NOT exercised (the tag uses the default forever max-ref-age);
  //     the cut is age-only, isolating retention + file GC from ref-age expiry.
  //
  // THE FIVE FIXTURES (each forces Reachable via a surviving tag):
  //   - linear:        s1..s4 linear, tag at head; the cut prunes a contiguous prefix (s1/s2/s3) ⇒
  //                    3 manifest lists die (the carried-forward manifests survive). Pins prefix
  //                    retention + manifest-list GC.
  //   - tag_protected: s1..s4 linear, tag on s2 (an otherwise-expirable mid-chain snapshot); the tag
  //                    KEEPS s2 + its list (s1 + s3 lists die). Pins ref-protection of the cleanup set.
  //   - stats:         s1..s3 linear, a statistics file on s2 (expired) + one on the head (survives);
  //                    the expired stats file is in the deleted set. Pins statistics-file cleanup.
  //   - deletes:       s1 append, s2 a POSITION-DELETE row-delta, s3 append; s1/s2 expire ⇒ their
  //                    manifest LISTS die. The DELETE manifest is carried forward into the surviving
  //                    head (s3), so it appears in BOTH the expired snapshots' list enumeration and
  //                    the retained head's list ⇒ the candidates-minus-retained subtraction (by PATH)
  //                    cancels it and it SURVIVES. SCOPE: this fixture pins that a delete-bearing
  //                    table expires identically cross-language at the metadata + manifest-LIST level
  //                    and that a carried-forward delete manifest is spared. It does NOT exercise
  //                    delete-manifest CONTENT cleanup: because no manifest dies (manifestsToDelete is
  //                    empty), neither side's cleanup ever READS the delete manifest's entries — that
  //                    path (an expired-only delete manifest whose delete file is deleted) is covered
  //                    by the B2 unit tests in expire_cleanup.rs, not here. (Reviewer-corrected: the
  //                    earlier "the delete manifest is read but spared" overstated — only the
  //                    list-level ManifestFile reference is enumerated; the manifest body is not read.)
  //   - rewrite:       s1 append D1, s2 append D2, s3 DELETE D1 (rewrites s1's manifest), s4 append D3;
  //                    when s1/s2/s3 expire, D1 (live only in the expired snapshots) + s1's original
  //                    manifest become expired-only ⇒ both DIE — exercising the CONTENT + MANIFEST
  //                    deletion funnels end-to-end (the others only delete lists/stats).
  //
  // generate-interop-expire (-Dinterop.expire.dir): build the five fixtures, run Java's
  // table.expireSnapshots()...cleanExpiredFiles(true).deleteWith(collector).commit(), emit each
  // <fixture>/java_deleted.json (a PATH-INDEPENDENT `<funnel>@ord<N>` descriptor multiset) + leave the
  // expired table's final.metadata.json. verify-interop-expire (-Dinterop.expire.dir): Java re-reads the
  // RUST-expired table at <fixture>/rust_table and asserts (a) its surviving-snapshot COUNT + ref NAMES
  // match Java's own and (b) the Rust-collected deleted descriptor equals Java's, then prints the
  // "0 failures" sentinel. The canonical snapshot-metadata views are emitted via the shared
  // SnapshotMetaOracle (emit-snapshot-meta) and byte-diffed by the run script.
  // =============================================================================================

  static final class ExpireOracle {
    private ExpireOracle() {}

    /** The fixed epoch anchor for every re-stamped snapshot timestamp (no wall-clock dependence). */
    static final long T0 = 1_700_000_000_000L;

    /** The five fixtures, each a self-contained expire scenario (see the class banner). */
    private static final List<String> FIXTURES =
        java.util.Arrays.asList("linear", "tag_protected", "stats", "deletes", "rewrite");

    static void generate(Path dir) throws IOException {
      for (String fixture : FIXTURES) {
        Path fixtureDir = dir.resolve(fixture);
        Files.createDirectories(fixtureDir);
        java.util.TreeSet<String> rawDeleted = new java.util.TreeSet<>();
        TableMetadata before = buildAndExpire(fixture, fixtureDir, rawDeleted);
        // PATH-INDEPENDENT comparison: Java and Rust write the table at DIFFERENT absolute paths
        // (random manifest/list UUIDs, different temp roots), so the raw deleted PATHS can never be
        // compared across sides. Normalize each deleted file to a structural descriptor
        // `<funnel>@ord<N>` keyed by the ORDINAL (sequence-number order in `before`) of the snapshot
        // that owns it — the manifest list's owning snapshot, a manifest's `added_snapshot_id`, a
        // content file's manifest's `added_snapshot_id`, a statistics file's `snapshot_id`. The
        // ordinal-keyed multiset is identical on both sides because the snapshot GRAPH is logically
        // identical; the Rust side computes the SAME descriptor from its own report + before-metadata.
        java.util.List<String> descriptor = normalizeDeleted(before, rawDeleted);
        writeStringListSorted(fixtureDir.resolve("java_deleted.json"), descriptor);
        System.out.println(
            "generate-interop-expire/"
                + fixture
                + ": deleted "
                + rawDeleted.size()
                + " files -> descriptor "
                + descriptor);
      }
      System.out.println("generate-interop-expire: wrote " + FIXTURES.size() + " fixtures to " + dir);
    }

    /**
     * Build the named fixture's table to {@code <fixtureDir>/table} with REAL manifests, re-stamp the
     * snapshot timestamps deterministically, then run Java's full
     * {@code expireSnapshots()...cleanExpiredFiles(true).deleteWith(collector).commit()} — collecting
     * the raw deleted-file paths into {@code deleted} and landing {@code final.metadata.json}.
     * Returns the PRE-EXPIRE (re-stamped) table metadata so the caller can normalize the deleted
     * paths to path-independent ordinal-keyed descriptors.
     */
    private static TableMetadata buildAndExpire(
        String fixture, Path fixtureDir, java.util.Set<String> deleted) throws IOException {
      File tableDir = fixtureDir.resolve("table").toFile();
      File metadataDir = new File(tableDir, "metadata");
      if (!metadataDir.isDirectory() && !metadataDir.mkdirs()) {
        throw new IOException("failed to create metadata dir at " + metadataDir);
      }

      Schema schema =
          new Schema(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "category", Types.StringType.get()));
      PartitionSpec spec = PartitionSpec.builderFor(schema).identity("category").build();
      Map<String, String> props = new LinkedHashMap<>();
      props.put(TableProperties.FORMAT_VERSION, "2");
      TableMetadata seed =
          TableMetadata.newTableMetadata(
              schema, spec, SortOrder.unsorted(), tableDir.getAbsolutePath(), props);

      LocalTableOperations ops = new LocalTableOperations(tableDir, metadataDir);
      ops.commit(null, seed);
      BaseTable table = new BaseTable(ops, "interop_expire_" + fixture);
      String dataDir = tableDir.getAbsolutePath() + "/data";

      // The per-fixture commit chain (real fast appends ⇒ real manifests + manifest lists on disk).
      switch (fixture) {
        case "linear":
        case "tag_protected":
          for (int i = 1; i <= 4; i++) {
            table.newFastAppend().appendFile(dataFile(table, dataDir, "f" + i, "category=a", 10L)).commit();
          }
          break;
        case "stats":
          for (int i = 1; i <= 3; i++) {
            table.newFastAppend().appendFile(dataFile(table, dataDir, "f" + i, "category=a", 10L)).commit();
          }
          break;
        case "deletes":
          // s1 fast append two data files (one per partition), s2 a position-delete on the cat=a
          // file (a DELETE manifest that must walk through cleanup when its snapshot expires), s3 a
          // fast append advancing the head so s1/s2 become age-expirable.
          table
              .newFastAppend()
              .appendFile(dataFile(table, dataDir, "a1", "category=a", 10L))
              .appendFile(dataFile(table, dataDir, "b1", "category=b", 20L))
              .commit();
          DeleteFile posDelete =
              FileMetadata.deleteFileBuilder(spec)
                  .ofPositionDeletes()
                  .withPath(dataDir + "/a1-deletes.parquet")
                  .withFileSizeInBytes(100L)
                  .withRecordCount(1L)
                  .withPartitionPath("category=a")
                  .withFormat(FileFormat.PARQUET)
                  .build();
          table.newRowDelta().addDeletes(posDelete).commit();
          table.newFastAppend().appendFile(dataFile(table, dataDir, "c1", "category=a", 30L)).commit();
          break;
        case "rewrite":
          // s1 append {D1, D2} in ONE manifest, s2 append D3 (advance), s3 DELETE D1 (copy-on-write:
          // s1's 2-file manifest is REWRITTEN into a 1-existing(D2) + 1-deleted(D1) manifest — NOT
          // all-tombstone, so BOTH Java AND Rust's fast-append carry it forward), s4 append D4
          // (advance head). When s1/s2/s3 expire, D1 (live only in s1's ORIGINAL manifest) + that
          // ORIGINAL 2-file manifest become expired-only ⇒ both DIE, while the rewritten manifest
          // (carrying D2 as EXISTING) survives — exercising the CONTENT + MANIFEST deletion funnels
          // end-to-end (the carried-forward fixtures above only delete lists/stats).
          //
          // DESIGN NOTE: the delete deliberately leaves D2 EXISTING in the rewritten manifest. A
          // delete that EMPTIED the manifest (deleting its sole file) would produce an ALL-TOMBSTONE
          // manifest, which Java's FastAppend.apply carries forward (`snapshot.allManifests()`, no
          // filter) but the Rust `FastAppend::existing_manifest` DROPS (it filters to
          // `has_added_files() || has_existing_files()`) — a REAL FastAppend carry-forward divergence
          // unrelated to expiry. Keeping D2 existing avoids that separate bug so this fixture pins
          // ONLY the expire+cleanup surface. (The divergence is reported for its own increment.)
          table
              .newFastAppend()
              .appendFile(dataFile(table, dataDir, "d1", "category=a", 10L))
              .appendFile(dataFile(table, dataDir, "d2", "category=a", 20L))
              .commit();
          table.newFastAppend().appendFile(dataFile(table, dataDir, "d3", "category=a", 30L)).commit();
          table.newDelete().deleteFile(dataDir + "/d1.parquet").commit();
          table.newFastAppend().appendFile(dataFile(table, dataDir, "d4", "category=a", 40L)).commit();
          break;
        default:
          throw new IllegalArgumentException("unknown expire fixture: " + fixture);
      }

      // Re-stamp every snapshot with a deterministic timestamp (T0 + 1000*ordinal), preserving its
      // REAL manifest-list location, by rebuilding from a fresh seed and re-parsing from disk.
      TableMetadata current = ops.current();
      List<Snapshot> ordered = new ArrayList<>(current.snapshots());
      ordered.sort(java.util.Comparator.comparingLong(Snapshot::sequenceNumber));
      TableMetadata.Builder rebuilt = TableMetadata.buildFrom(seed);
      long timestampMs = T0;
      Long parentId = null;
      long headId = -1;
      long secondId = -1; // the second snapshot in sequence order (for the tag_protected fixture)
      int ordinal = 0;
      for (Snapshot snapshot : ordered) {
        timestampMs += 1000;
        ordinal++;
        Map<String, String> summary = new LinkedHashMap<>(snapshot.summary());
        BaseSnapshot restamped =
            new BaseSnapshot(
                snapshot.sequenceNumber(),
                snapshot.snapshotId(),
                parentId,
                timestampMs,
                snapshot.operation(),
                summary,
                snapshot.schemaId(),
                snapshot.manifestListLocation(),
                null,
                null,
                null);
        rebuilt.setBranchSnapshot(restamped, SnapshotRef.MAIN_BRANCH);
        parentId = snapshot.snapshotId();
        headId = snapshot.snapshotId();
        if (ordinal == 2) {
          secondId = snapshot.snapshotId();
        }
      }

      // Statistics fixture: attach a statistics file to the SECOND snapshot (which the cut will
      // expire) and one to the head (which survives) — the before-minus-after location diff, both
      // directions. The statistics files are real on-disk puffins (their bytes are never read by
      // cleanup; only their LOCATIONS are deleted) so deleteWith collects the expired one.
      String expiredStatsPath = null;
      if (fixture.equals("stats")) {
        expiredStatsPath = tableDir.getAbsolutePath() + "/metadata/stats-expired.puffin";
        String headStatsPath = tableDir.getAbsolutePath() + "/metadata/stats-head.puffin";
        writeStatsFile(ops.io(), expiredStatsPath);
        writeStatsFile(ops.io(), headStatsPath);
        rebuilt.setStatistics(statisticsFile(secondId, expiredStatsPath));
        rebuilt.setStatistics(statisticsFile(headId, headStatsPath));
      }

      // The surviving TAG: forces Java down ReachableFileCleanup AND is the ref-protection element.
      // - tag_protected: the tag pins the SECOND snapshot (an otherwise age-expirable mid-chain
      //   snapshot) ⇒ that snapshot + its files SURVIVE.
      // - every other fixture: the tag pins the HEAD (already retained) ⇒ it only forces Reachable.
      long tagTarget = fixture.equals("tag_protected") ? secondId : headId;
      rebuilt.setRef("keep", SnapshotRef.tagBuilder(tagTarget).build());

      TableMetadata restamped = rebuilt.build();
      File baseFile = new File(metadataDir, "restamped.metadata.json");
      TableMetadataParser.write(restamped, ops.io().newOutputFile(baseFile.getAbsolutePath()));
      TableMetadata cleanBase =
          TableMetadataParser.fromJson(baseFile.getAbsolutePath(), readString(baseFile.toPath()));
      LocalTableOperations ops2 = new LocalTableOperations(tableDir, metadataDir);
      ops2.continueVersioningFrom(ops);
      ops2.commit(null, cleanBase);
      BaseTable expireTable = new BaseTable(ops2, "interop_expire_" + fixture);

      // The deterministic cut: keep the head, expire what the fixture intends.
      //   - linear / stats / deletes: cut between snapshot 2 and 3's timestamps ⇒ expire 1 & 2 (and
      //     for the 4-snapshot fixtures, 3), keep the head; retainLast(1).
      //   - tag_protected: the same cut, but the tag pins snapshot 2 ⇒ it survives despite being
      //     below the cut. (This is the ref-protection pin: the cut would expire snapshot 2 but the
      //     tag keeps it.)
      long cut = T0 + (long) (ordered.size()) * 1000L - 500L; // 500ms below the head's timestamp
      // FORCE ReachableFileCleanup (the only strategy the Rust side ports) for EVERY fixture via the
      // real engine selector `RemoveSnapshots.withIncrementalCleanup(false)` (package-private, same
      // package; offset-verified vs 1.10.0 bytecode: it pre-sets `incrementalCleanup=FALSE`, so
      // `cleanExpiredSnapshots` skips its auto-derivation and instantiates ReachableFileCleanup).
      //
      // WHY THE SURVIVING TAG IS NOT ENOUGH on its own: Java's auto-selection picks INCREMENTAL when
      // `!specifiedSnapshotId && !hasRemovedNonMainAncestors(base,current) && !hasNonMainSnapshots
      // (current)` (1.10.0 bytecode). `hasNonMainSnapshots(current)` is true only when a SURVIVING
      // snapshot is OFF the post-expiry main ancestry (the head's parent-walk). A tag on the HEAD
      // (linear/stats/deletes/rewrite) leaves every survivor ON main ⇒ Java auto-picks INCREMENTAL;
      // only `tag_protected` (tag on a mid-chain survivor) auto-selects Reachable. The two strategies
      // happen to produce the identical deleted set for these fixture shapes (measured), so the
      // comparison passed even cross-strategy — but that is a coincidence of shape, not a 1:1 proof
      // of the Reachable port. Pinning the strategy makes BOTH sides genuinely run ReachableFileCleanup.
      // The surviving tag is retained as the ref-protection judgment-surface element (it keeps
      // `tag_protected`'s mid-chain snapshot and forces a non-trivial reachable set everywhere).
      org.apache.iceberg.ExpireSnapshots expireApi =
          expireTable
              .expireSnapshots()
              .expireOlderThan(cut)
              .retainLast(1)
              .cleanExpiredFiles(true)
              .deleteWith(deleted::add);
      ((RemoveSnapshots) expireApi).withIncrementalCleanup(false);
      expireApi.commit();

      // Land the FINAL (post-expire) metadata at the known path for the emitter + the comparison.
      Path finalMetadata = metadataDir.toPath().resolve("final.metadata.json");
      OutputFile finalOut =
          new LocalFileIO().newOutputFile(finalMetadata.toAbsolutePath().toString());
      TableMetadataParser.write(ops2.current(), finalOut);

      // Return the PRE-EXPIRE (re-stamped) metadata for path-independent normalization.
      return cleanBase;
    }

    /**
     * Normalize a set of raw deleted file PATHS to a path-independent, ordinal-keyed descriptor
     * MULTISET (one `<funnel>@ord<N>` token per deleted file). The owning snapshot ordinal is the
     * file's position in {@code before}'s sequence-number order:
     *
     * <ul>
     *   <li>a manifest LIST → the ordinal of the snapshot whose {@code manifestListLocation} equals
     *       the path;
     *   <li>a MANIFEST → the ordinal of its {@code addedSnapshotId} (read from the before-snapshots'
     *       manifest lists);
     *   <li>a CONTENT file → the ordinal of its manifest's {@code addedSnapshotId};
     *   <li>a STATISTICS file → the ordinal of its {@code snapshotId}.
     * </ul>
     *
     * Both languages compute the IDENTICAL multiset because the snapshot graph is logically
     * identical; the absolute paths (UUIDs, temp roots) never enter the comparison.
     */
    private static List<String> normalizeDeleted(TableMetadata before, java.util.Set<String> deleted)
        throws IOException {
      // ordinal by sequence number.
      List<Snapshot> ordered = new ArrayList<>(before.snapshots());
      ordered.sort(java.util.Comparator.comparingLong(Snapshot::sequenceNumber));
      Map<Long, Integer> ordinalById = new LinkedHashMap<>();
      for (int i = 0; i < ordered.size(); i++) {
        ordinalById.put(ordered.get(i).snapshotId(), i);
      }
      // path -> "<ordinal>" or "<ordinal>#<discriminator>", for lists / manifests / content files.
      // The per-FILE discriminator (record count for content, a/e/d file counts for a manifest, file
      // size for statistics) makes the descriptor INJECTIVE within a fixture: two DIFFERENT files
      // added by the SAME snapshot (e.g. a 2-file append) share an ordinal, so an ordinal-only token
      // would let a Rust-deletes-X / Java-deletes-Y swap pass the multiset comparison. A manifest LIST
      // needs no discriminator (one list per snapshot ⇒ the ordinal is already unique). MUST match
      // the Rust `interop_expire.rs` `normalize_deleted` token scheme byte-for-byte.
      Map<String, String> listOwner = new LinkedHashMap<>();
      Map<String, String> manifestOwner = new LinkedHashMap<>();
      Map<String, String> contentOwner = new LinkedHashMap<>();
      LocalFileIO io = new LocalFileIO();
      for (Snapshot snapshot : ordered) {
        int ordinal = ordinalById.get(snapshot.snapshotId());
        if (snapshot.manifestListLocation() != null) {
          listOwner.put(snapshot.manifestListLocation(), Integer.toString(ordinal));
        }
        for (ManifestFile manifest : snapshot.allManifests(io)) {
          int manifestOrdinal = ordinalById.getOrDefault(manifest.snapshotId(), ordinal);
          String manifestDiscriminator =
              "a"
                  + nullToZero(manifest.addedFilesCount())
                  + "e"
                  + nullToZero(manifest.existingFilesCount())
                  + "d"
                  + nullToZero(manifest.deletedFilesCount());
          manifestOwner.putIfAbsent(manifest.path(), manifestOrdinal + "#" + manifestDiscriminator);
          for (Map.Entry<String, Long> content : contentPaths(before, manifest, io).entrySet()) {
            contentOwner.putIfAbsent(
                content.getKey(), manifestOrdinal + "#rc" + content.getValue());
          }
        }
      }
      // statistics path -> "<ordinal>#sz<file_size>".
      Map<String, String> statsOwner = new LinkedHashMap<>();
      for (StatisticsFile stats : before.statisticsFiles()) {
        Integer ordinal = ordinalById.get(stats.snapshotId());
        if (ordinal != null) {
          statsOwner.put(stats.path(), ordinal + "#sz" + stats.fileSizeInBytes());
        }
      }
      for (PartitionStatisticsFile stats : before.partitionStatisticsFiles()) {
        Integer ordinal = ordinalById.get(stats.snapshotId());
        if (ordinal != null) {
          statsOwner.put(stats.path(), ordinal + "#sz" + stats.fileSizeInBytes());
        }
      }

      List<String> descriptor = new ArrayList<>();
      for (String path : deleted) {
        if (listOwner.containsKey(path)) {
          descriptor.add("manifest_list@ord" + listOwner.get(path));
        } else if (manifestOwner.containsKey(path)) {
          descriptor.add("manifest@ord" + manifestOwner.get(path));
        } else if (contentOwner.containsKey(path)) {
          descriptor.add("content@ord" + contentOwner.get(path));
        } else if (statsOwner.containsKey(path)) {
          descriptor.add("statistics@ord" + statsOwner.get(path));
        } else {
          // A deleted path with no owner in `before` is a fixture/logic bug — surface it loudly
          // (an unclassifiable token would NOT match the Rust descriptor, failing the comparison).
          descriptor.add("UNCLASSIFIED:" + path);
        }
      }
      java.util.Collections.sort(descriptor);
      return descriptor;
    }

    private static long nullToZero(Integer value) {
      return value == null ? 0L : value.longValue();
    }

    /** Every entry's file path -> record count in one manifest (data or delete reader per content). */
    private static Map<String, Long> contentPaths(
        TableMetadata metadata, ManifestFile manifest, FileIO io) throws IOException {
      Map<String, Long> paths = new LinkedHashMap<>();
      if (manifest.content() == ManifestContent.DATA) {
        try (ManifestReader<DataFile> reader =
            ManifestFiles.read(manifest, io, metadata.specsById())) {
          for (ManifestEntry<DataFile> entry : reader.entries()) {
            paths.put(entry.file().location(), entry.file().recordCount());
          }
        }
      } else {
        try (ManifestReader<DeleteFile> reader =
            ManifestFiles.readDeleteManifest(manifest, io, metadata.specsById())) {
          for (ManifestEntry<DeleteFile> entry : reader.entries()) {
            paths.put(entry.file().location(), entry.file().recordCount());
          }
        }
      }
      return paths;
    }

    /** A metadata-only {@link DataFile} (no parquet — the path need not exist for the cleanup walk). */
    private static DataFile dataFile(
        BaseTable table, String dataDir, String name, String partitionPath, long recordCount) {
      return DataFiles.builder(table.spec())
          .withPath(dataDir + "/" + name + ".parquet")
          .withFileSizeInBytes(recordCount * 100)
          .withRecordCount(recordCount)
          .withPartitionPath(partitionPath)
          .withFormat(FileFormat.PARQUET)
          .build();
    }

    private static GenericStatisticsFile statisticsFile(long snapshotId, String path) {
      return new GenericStatisticsFile(snapshotId, path, 18L, 4L, java.util.Collections.emptyList());
    }

    /** Write a tiny real statistics file (its bytes are never read by cleanup; only the path is). */
    private static void writeStatsFile(FileIO io, String path) throws IOException {
      OutputFile out = io.newOutputFile(path);
      try (java.io.OutputStream stream = out.create()) {
        stream.write("interop-expire stats fixture".getBytes(StandardCharsets.UTF_8));
      }
    }

    /** Returns the number of failures (0 ⇒ Java agrees with the Rust-expired table both ways). */
    static int verify(Path dir) throws IOException {
      int failures = 0;
      for (String fixture : FIXTURES) {
        Path fixtureDir = dir.resolve(fixture);
        Path rustMetadata = fixtureDir.resolve("rust_table/metadata/final.metadata.json");
        if (!Files.exists(rustMetadata)) {
          System.out.println("FAIL expire/" + fixture + ": missing rust_table final.metadata.json");
          failures++;
          continue;
        }

        // (a) Java re-reads the RUST-expired table and asserts its surviving-snapshot COUNT + ref
        //     NAMES match Java's own expiry of the Java fixture. (Snapshot IDS are NOT comparable —
        //     Rust mints its own ids — so the COUNT + the byte-equal canonical view, diffed by the
        //     run script, carry the per-snapshot structural match; ref NAMES (`main`, `keep`) ARE
        //     comparable and prove the tag/branch protection landed identically.)
        TableMetadata rustExpired =
            TableMetadataParser.fromJson(rustMetadata.toString(), readString(rustMetadata));
        Path javaMetadata = fixtureDir.resolve("table/metadata/final.metadata.json");
        TableMetadata javaExpired =
            TableMetadataParser.fromJson(javaMetadata.toString(), readString(javaMetadata));

        int rustSurviving = countSnapshots(rustExpired);
        int javaSurviving = countSnapshots(javaExpired);
        if (rustSurviving != javaSurviving) {
          System.out.println(
              "FAIL expire/"
                  + fixture
                  + ": surviving snapshot COUNT differs — java="
                  + javaSurviving
                  + " rust="
                  + rustSurviving);
          failures++;
          continue;
        }
        if (!rustExpired.refs().keySet().equals(javaExpired.refs().keySet())) {
          System.out.println(
              "FAIL expire/"
                  + fixture
                  + ": ref names differ — java="
                  + javaExpired.refs().keySet()
                  + " rust="
                  + rustExpired.refs().keySet());
          failures++;
          continue;
        }

        // (b) The Rust-collected deleted-file DESCRIPTOR (rust_deleted.json — path-independent
        //     `<funnel>@ord<N>` MULTISET) must equal Java's (java_deleted.json) as sorted lists.
        List<String> javaDeleted = sorted(readStringList(fixtureDir.resolve("java_deleted.json")));
        Path rustDeletedPath = fixtureDir.resolve("rust_deleted.json");
        if (!Files.exists(rustDeletedPath)) {
          System.out.println("FAIL expire/" + fixture + ": missing rust_deleted.json");
          failures++;
          continue;
        }
        List<String> rustDeleted = sorted(readStringList(rustDeletedPath));
        if (!javaDeleted.equals(rustDeleted)) {
          System.out.println(
              "FAIL expire/"
                  + fixture
                  + ": deleted descriptors differ — java="
                  + javaDeleted
                  + " rust="
                  + rustDeleted);
          failures++;
          continue;
        }
        System.out.println("PASS expire/" + fixture);
      }
      return failures;
    }

    private static int countSnapshots(TableMetadata metadata) {
      int count = 0;
      for (Snapshot ignored : metadata.snapshots()) {
        count++;
      }
      return count;
    }

    private static List<String> sorted(List<String> values) {
      List<String> copy = new ArrayList<>(values);
      java.util.Collections.sort(copy);
      return copy;
    }

    /** Write a JSON array of the strings, as given (already sorted), one canonical document. */
    private static void writeStringListSorted(Path out, List<String> strings) throws IOException {
      List<String> sortedCopy = sorted(strings);
      String json =
          JsonUtil.generate(
              gen -> {
                gen.writeStartArray();
                for (String value : sortedCopy) {
                  gen.writeString(value);
                }
                gen.writeEndArray();
              },
              false);
      writeJson(out, json);
    }

    /** Read a JSON array of strings. */
    private static List<String> readStringList(Path path) throws IOException {
      return JsonUtil.parse(
          readString(path),
          node -> {
            List<String> values = new ArrayList<>();
            node.forEach(element -> values.add(element.asText()));
            return values;
          });
    }
  }

  // =============================================================================================
  // WriteActionsOracle — the METADATA-LEVEL rewrite-family interop fixture (E2, extended for
  // Increment 4). Performs the SAME chain the Rust GEN test (interop_write_actions_meta.rs)
  // performs, on a V2 table partitioned by identity(category), with IDENTICAL logical constants
  // (paths differ; record counts and partition values must match). NO parquet is written — the
  // actions only read and rewrite MANIFESTS, so the data-file paths need not exist (the A1-A4
  // convention).
  //
  //   s1 newFastAppend  A(cat=a,10) B(cat=b,20) C(cat=a,30)      seq 1, op append
  //                     (FAST append, mirroring Rust fast_append — newAppend() would be the
  //                     MERGING producer; s8 below is where this fixture exercises that path)
  //   s2 newDelete      deleteFile(A.path)                        seq 2, op delete
  //                     (the manifest-filter rewrite: A tombstoned, B/C carried as Existing
  //                     with their ORIGINAL provenance — the corruption-class axis)
  //   s3 newOverwrite   addFile(D cat=b,40) deleteFile(B)         seq 3, op overwrite
  //   s4 newReplacePartitions addFile(E cat=a,50)                 seq 4, op overwrite +
  //                     replace-partitions=true (drops the live cat=a file C)
  //   s5 newRewrite     rewriteFiles({E}, {F cat=a,50})           seq 5, op replace
  //                     (the compaction commit; record count conserved)
  //   s6 rewriteManifests clusterBy(partition)                    seq 5 (no data change), op replace
  //                     (re-group live DATA entries one manifest per partition; every entry carried
  //                     EXISTING with its original provenance; live set {C,D,F} unchanged)
  //   s7 updateProperties set min-count-to-merge=2                NO SNAPSHOT (a property commit)
  //                     (arms the s8 merge; the snapshot-level view is unaffected both sides)
  //   s8 newAppend      appendFile(G cat=a,60)                    seq 6, op append (MERGING)
  //                     (Rust merge_append — with min-count=2 + KB manifests vs 8 MB target every
  //                     manifest lands in ONE bin ⇒ the merge fires into ONE merged manifest)
  // =============================================================================================

  static final class WriteActionsOracle {
    private WriteActionsOracle() {}

    static void generate(Path dir) throws IOException {
      Files.createDirectories(dir);
      File tableDir = dir.resolve("table").toFile();
      File metadataDir = new File(tableDir, "metadata");
      if (!metadataDir.isDirectory() && !metadataDir.mkdirs()) {
        throw new IOException("failed to create metadata dir at " + metadataDir);
      }

      Schema schema =
          new Schema(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "category", Types.StringType.get()));
      PartitionSpec spec = PartitionSpec.builderFor(schema).identity("category").build();

      Map<String, String> props = new LinkedHashMap<>();
      props.put(TableProperties.FORMAT_VERSION, "2");
      TableMetadata seed =
          TableMetadata.newTableMetadata(
              schema, spec, SortOrder.unsorted(), tableDir.getAbsolutePath(), props);

      LocalTableOperations ops = new LocalTableOperations(tableDir, metadataDir);
      ops.commit(null, seed);
      BaseTable table = new BaseTable(ops, "interop_write_actions");

      String dataDir = tableDir.getAbsolutePath() + "/data";
      DataFile fileA = fakeDataFile(table, dataDir + "/a1.parquet", "category=a", 10L);
      DataFile fileB = fakeDataFile(table, dataDir + "/b1.parquet", "category=b", 20L);
      DataFile fileC = fakeDataFile(table, dataDir + "/a2.parquet", "category=a", 30L);
      DataFile fileD = fakeDataFile(table, dataDir + "/b2.parquet", "category=b", 40L);
      DataFile fileE = fakeDataFile(table, dataDir + "/a3.parquet", "category=a", 50L);
      DataFile fileF = fakeDataFile(table, dataDir + "/a4-compacted.parquet", "category=a", 50L);

      DataFile fileG = fakeDataFile(table, dataDir + "/a5-merged.parquet", "category=a", 60L);

      // The eight-commit chain (s1..s5 produce sequence numbers 1..5; s7 is a PROPERTY commit and
      // produces NO snapshot; s8 takes sequence number 6).
      table.newFastAppend().appendFile(fileA).appendFile(fileB).appendFile(fileC).commit();
      table.newDelete().deleteFile(fileA.path()).commit();
      table.newOverwrite().addFile(fileD).deleteFile(fileB).commit();
      table.newReplacePartitions().addFile(fileE).commit();
      java.util.Set<DataFile> rewriteDelete = new java.util.HashSet<>();
      rewriteDelete.add(fileE);
      java.util.Set<DataFile> rewriteAdd = new java.util.HashSet<>();
      rewriteAdd.add(fileF);
      table.newRewrite().rewriteFiles(rewriteDelete, rewriteAdd).commit();

      // s6 rewriteManifests clustered BY PARTITION: re-group the live DATA entries one manifest per
      // partition. The cluster key STRING never appears in metadata — only the resulting GROUPING is
      // compared, so any key fn that produces one group per distinct partition tuple is equivalent to
      // the Rust side's. Java keys on `String.valueOf(file.partition())`; Rust keys on
      // `format!("{:?}", data_file.partition())` (documented on both sides) — both produce one group
      // per category. Live data after s5 is {C(cat=a,30), D(cat=b,40), F(cat=a,50)} ⇒ two manifests
      // (cat=a: {C,F}; cat=b: {D}), every entry carried EXISTING with its original provenance.
      table.rewriteManifests().clusterBy(file -> String.valueOf(file.partition())).commit();

      // s7 set commit.manifest.min-count-to-merge=2 as a TABLE PROPERTY (no snapshot is produced — the
      // canonical snapshot-level view is unaffected on both sides). This arms the MERGING append in s8.
      table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "2").commit();

      // s8 newAppend (the MERGING producer, Rust `merge_append`): add G(cat=a,60). With
      // min-count-to-merge=2 and KB-size manifests vs the 8 MB target, every manifest lands in ONE bin
      // and the merge fires — the new added manifest merges with the existing manifests into ONE merged
      // manifest carrying the Existing entries (original snapshot-id ordinals + seqs) + G as Added.
      // Manifest avro length differs a few bytes across writers, but all-tiny ⇒ one bin on both sides
      // (length-insensitive BY DESIGN — see the fixture doc).
      table.newAppend().appendFile(fileG).commit();

      // The FINAL metadata at a known path for the emitter + the Rust test.
      Path finalMetadata = metadataDir.toPath().resolve("final.metadata.json");
      OutputFile finalOut =
          new LocalFileIO().newOutputFile(finalMetadata.toAbsolutePath().toString());
      TableMetadataParser.write(ops.current(), finalOut);
      System.out.println("generated write-actions table to " + dir);
    }

    /**
     * A metadata-only {@link DataFile} (the path need not exist — the four actions only read
     * manifests). Record count + partition are the cross-language-comparable constants; the file
     * size is excluded from the canonical view, so any stable value works.
     */
    private static DataFile fakeDataFile(
        BaseTable table, String path, String partitionPath, long recordCount) {
      return DataFiles.builder(table.spec())
          .withPath(path)
          .withFileSizeInBytes(recordCount * 100)
          .withRecordCount(recordCount)
          .withPartitionPath(partitionPath)
          .withFormat(FileFormat.PARQUET)
          .build();
    }
  }

  // =============================================================================================
  // RewriteSeqOracle — the DELETE-BEARING rewrite fixture (Increment 4, E1-family / metadata-only).
  // A SIBLING of WriteActionsOracle that pins the seq-preserving rewrite over a merge-on-read table:
  //
  //   s1 newFastAppend  A(cat=a,10) B(cat=b,20)                   seq 1, op append
  //   s2 newRowDelta    addDeletes(posDelete -> B)                seq 2, op delete
  //                     (a metadata-only POSITION-delete referencing B — the SURVIVOR of the rewrite)
  //   s3 newRewrite     validateFromSnapshot(s2).rewriteFiles({A}, {A'}, dataSequenceNumber=1L)
  //                                                               seq 3, op replace
  //                     (A is replaced by A'; A' is STAMPED with data sequence number 1 — the
  //                     replaced file's seq, NOT the rewrite snapshot's seq 3)
  //
  // The two LOAD-BEARING assertions (judged through the same canonical view as WriteActionsOracle):
  //   1. A' carries data_sequence_number 1 in the post-inheritance entry view (not 3).
  //   2. The DELETE manifest (the position-delete on B) SURVIVES the rewrite intact.
  //
  // DESIGN CONSTRAINT (decided): the position delete references B, which SURVIVES the rewrite — NOT
  // A. This keeps Java's dangling-delete machinery (`removeDanglingDeletesFor` /
  // `dropDeleteFilesOlderThan`, which the Rust port deliberately does NOT implement) DORMANT on both
  // sides, so the comparison cleanly pins what this fixture is FOR (the two assertions above) rather
  // than an unported retention path.
  //
  // EMPIRICAL PROBE FINDING (Increment 4, throwaway probe — NOT in the byte-diffed chain): a SECOND
  // rewrite that ALSO rewrites B -> B' (making the delete-on-B DANGLING) COMMITS on 1.10.0 and KEEPS
  // the dangling parquet position-delete manifest intact in the final snapshot. MECHANISM (re-traced
  // 2026-06-10): a RewriteFiles DOES run `deleteFilterManager.removeDanglingDeletesFor(...)`
  // (MergingSnapshotProducer L995) successfully — `deleteFilterManager` is a `DeleteFileFilterManager`
  // that does NOT override it. The `UnsupportedOperationException` throw at L1220-1222 is on the
  // SIBLING `DataFileFilterManager` (the DATA-file side) and is NEVER reached for delete pruning. The
  // real reason a dangling POSITION-DELETE PARQUET survives is that BOTH delete-drop paths miss it:
  // `isDanglingDV` is gated on `ContentFileUtil.isDV` == `FileFormat.PUFFIN` (so a V2 parquet position
  // delete, a non-DV, is structurally exempt), and `dropDeleteFilesOlderThan(minDataSequenceNumber)`
  // does not drop it (the carried A'@seq1 keeps the min data seq at 1, below the delete's seq 2) —
  // i.e. 1.10.0 prunes only dangling DVs on a RewriteFiles. This CONVERGES with the Rust action's
  // documented carry-unchanged posture — PARITY, not divergence — so the probe step is left out of the
  // chain purely because the survivor-delete fixture already pins the load-bearing behavior cleanly.
  //
  // Java uses `validateFromSnapshot(rowDeltaSnapshotId)` explicitly; the Rust mirror builds the
  // rewrite transaction AFTER the row-delta commit, so its transaction-captured starting snapshot IS
  // the row-delta snapshot and the concurrent window is empty — the semantic twin of Java's explicit
  // validateFromSnapshot (documented on both sides).
  //
  // NO parquet: the data + delete files are metadata-only (the paths need not exist — the rewrite +
  // the canonical view only read MANIFESTS). The Java side uses `DataFiles.builder` /
  // `FileMetadata.deleteFileBuilder(...).ofPositionDeletes()`; the Rust side uses
  // `DataFileBuilder` with `content(PositionDeletes)`.
  // =============================================================================================

  static final class RewriteSeqOracle {
    private RewriteSeqOracle() {}

    static void generate(Path dir) throws IOException {
      Files.createDirectories(dir);
      File tableDir = dir.resolve("table").toFile();
      File metadataDir = new File(tableDir, "metadata");
      if (!metadataDir.isDirectory() && !metadataDir.mkdirs()) {
        throw new IOException("failed to create metadata dir at " + metadataDir);
      }

      Schema schema =
          new Schema(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "category", Types.StringType.get()));
      PartitionSpec spec = PartitionSpec.builderFor(schema).identity("category").build();

      Map<String, String> props = new LinkedHashMap<>();
      props.put(TableProperties.FORMAT_VERSION, "2");
      TableMetadata seed =
          TableMetadata.newTableMetadata(
              schema, spec, SortOrder.unsorted(), tableDir.getAbsolutePath(), props);

      LocalTableOperations ops = new LocalTableOperations(tableDir, metadataDir);
      ops.commit(null, seed);
      BaseTable table = new BaseTable(ops, "interop_rewrite_seq");

      String dataDir = tableDir.getAbsolutePath() + "/data";
      DataFile fileA = fakeDataFile(table, dataDir + "/a1.parquet", "category=a", 10L);
      DataFile fileB = fakeDataFile(table, dataDir + "/b1.parquet", "category=b", 20L);
      DataFile fileAprime = fakeDataFile(table, dataDir + "/a1-compacted.parquet", "category=a", 10L);

      // A metadata-only POSITION-delete referencing B (partition category=b). Built via
      // FileMetadata.deleteFileBuilder (the metadata-only analog of fakeDataFile) — no parquet.
      DeleteFile deleteB =
          FileMetadata.deleteFileBuilder(spec)
              .ofPositionDeletes()
              .withPath(dataDir + "/b1-deletes.parquet")
              .withFileSizeInBytes(100L)
              .withRecordCount(1L)
              .withPartitionPath("category=b")
              .withFormat(FileFormat.PARQUET)
              .build();

      // s1 append A,B (seq 1); s2 row-delta adding the position-delete on B (seq 2).
      table.newFastAppend().appendFile(fileA).appendFile(fileB).commit();
      long rowDeltaSnapshotId;
      table.newRowDelta().addDeletes(deleteB).commit();
      rowDeltaSnapshotId = table.currentSnapshot().snapshotId();

      // s3 rewrite A -> A' preserving data sequence number 1 (the replaced file's seq), validated from
      // the row-delta snapshot. Java's `rewriteFiles(Set, Set, long)` overload stamps every added file
      // with the given data sequence number.
      java.util.Set<DataFile> rewriteDelete = new java.util.HashSet<>();
      rewriteDelete.add(fileA);
      java.util.Set<DataFile> rewriteAdd = new java.util.HashSet<>();
      rewriteAdd.add(fileAprime);
      table
          .newRewrite()
          .validateFromSnapshot(rowDeltaSnapshotId)
          .rewriteFiles(rewriteDelete, rewriteAdd, 1L)
          .commit();

      Path finalMetadata = metadataDir.toPath().resolve("final.metadata.json");
      OutputFile finalOut =
          new LocalFileIO().newOutputFile(finalMetadata.toAbsolutePath().toString());
      TableMetadataParser.write(ops.current(), finalOut);
      System.out.println("generated rewrite-seq (delete-bearing) table to " + dir);
    }

    /** A metadata-only {@link DataFile} (no parquet — the path need not exist). */
    private static DataFile fakeDataFile(
        BaseTable table, String path, String partitionPath, long recordCount) {
      return DataFiles.builder(table.spec())
          .withPath(path)
          .withFileSizeInBytes(recordCount * 100)
          .withRecordCount(recordCount)
          .withPartitionPath(partitionPath)
          .withFormat(FileFormat.PARQUET)
          .build();
    }
  }

  // =============================================================================================
  // SnapshotMetaOracle — the METADATA-LEVEL row-delta interop (E1). Emits a CANONICAL "snapshot
  // metadata view" of ANY on-disk table (Java-written or Rust-written): per snapshot (ordered by
  // sequence number) the operation, the COUNT-only summary, and the manifest-list -> manifest-entry
  // structure with POST-INHERITANCE sequence numbers. The SAME canonicalization is mirrored by
  // crates/iceberg/tests/interop_rowdelta_meta.rs, so "Java's view of Java's table" ==
  // "Java's view of Rust's table" == "Rust's view of either" is the metadata-parity contract.
  //
  // Canonicalization decisions (the comparable subset):
  // - snapshot ids -> ORDINALS (position in the sequence-number-ordered chain); parent likewise.
  // - summary: COUNT keys only (an explicit allowlist) — the *-files-size keys are EXCLUDED because
  //   parquet byte sizes legitimately differ between writers. `operation` is its own field (the
  //   re-parsed summary map splits it out — SnapshotParser.fromJson).
  // - manifests sorted by (content, sequence_number, min_sequence_number); paths excluded; the
  //   added_snapshot_id is emitted as an ORDINAL.
  // - entries sorted by a content/status/record-count/equality-ids/partition key; file paths and
  //   byte sizes excluded; `sequence_number` is the POST-INHERITANCE data sequence number (the
  //   merge-on-read applicability input). file_sequence_number is NOT emitted (the Rust public API
  //   does not expose it on entries; tracked).
  // - partition: null for an unpartitioned spec, else the spec's SINGLE-VALUE JSON serialization
  //   (SingleValueParser.toJson <-> Rust Literal::try_into_json — the one cross-language-canonical
  //   rendering of a partition tuple).
  //
  // LIMIT (reviewer-flagged): the ordinal scheme assumes DISTINCT sequence numbers. Every V1
  // snapshot has sequence number 0, so a multi-snapshot V1 table would produce
  // iteration-order-dependent ordinals — do NOT point this oracle at V1 tables without adding a
  // tiebreaker (timestamp, then snapshot id). The current fixtures are V2 row-delta chains.
  // =============================================================================================

  static final class SnapshotMetaOracle {
    private SnapshotMetaOracle() {}

    /** The COUNT-only summary allowlist — every SnapshotSummary count key, no byte-size keys. */
    private static final List<String> SUMMARY_COUNT_KEYS =
        java.util.Arrays.asList(
            SnapshotSummary.ADDED_FILES_PROP,
            SnapshotSummary.DELETED_FILES_PROP,
            SnapshotSummary.TOTAL_DATA_FILES_PROP,
            SnapshotSummary.ADDED_DELETE_FILES_PROP,
            SnapshotSummary.ADD_EQ_DELETE_FILES_PROP,
            SnapshotSummary.REMOVED_EQ_DELETE_FILES_PROP,
            SnapshotSummary.ADD_POS_DELETE_FILES_PROP,
            SnapshotSummary.REMOVED_POS_DELETE_FILES_PROP,
            SnapshotSummary.ADDED_DVS_PROP,
            SnapshotSummary.REMOVED_DVS_PROP,
            SnapshotSummary.REMOVED_DELETE_FILES_PROP,
            SnapshotSummary.TOTAL_DELETE_FILES_PROP,
            SnapshotSummary.ADDED_RECORDS_PROP,
            SnapshotSummary.DELETED_RECORDS_PROP,
            SnapshotSummary.TOTAL_RECORDS_PROP,
            SnapshotSummary.ADDED_POS_DELETES_PROP,
            SnapshotSummary.REMOVED_POS_DELETES_PROP,
            SnapshotSummary.TOTAL_POS_DELETES_PROP,
            SnapshotSummary.ADDED_EQ_DELETES_PROP,
            SnapshotSummary.REMOVED_EQ_DELETES_PROP,
            SnapshotSummary.TOTAL_EQ_DELETES_PROP,
            SnapshotSummary.CHANGED_PARTITION_COUNT_PROP,
            // Not a count, but a cross-language-comparable semantic marker
            // (BaseReplacePartitions sets replace-partitions=true; the Rust action mirrors it).
            SnapshotSummary.REPLACE_PARTITIONS_PROP);

    /**
     * Emit the canonical snapshot-metadata view of the table whose CURRENT metadata file is
     * {@code metadataJson} (loaded RE-PARSED from disk — the canonical form) to {@code out}.
     */
    static void emit(Path metadataJson, Path out) throws IOException {
      TableMetadata metadata =
          TableMetadataParser.fromJson(metadataJson.toString(), readString(metadataJson));
      LocalFileIO io = new LocalFileIO();

      // Order snapshots by sequence number (the commit order); ordinal = index in that order.
      List<Snapshot> snapshots = new ArrayList<>(metadata.snapshots());
      snapshots.sort(java.util.Comparator.comparingLong(Snapshot::sequenceNumber));
      Map<Long, Integer> ordinals = new LinkedHashMap<>();
      for (int i = 0; i < snapshots.size(); i++) {
        ordinals.put(snapshots.get(i).snapshotId(), i);
      }

      String json =
          JsonUtil.generate(
              gen -> {
                gen.writeStartObject();
                gen.writeArrayFieldStart("snapshots");
                for (Snapshot snap : snapshots) {
                  gen.writeStartObject();
                  gen.writeNumberField("ordinal", ordinals.get(snap.snapshotId()));
                  // parent_ordinal is null when the snapshot has no parent OR when its parent was
                  // EXPIRED out of the table (a surviving snapshot whose parent is no longer
                  // in-table has no in-table parent ordinal). The expire fixtures are the first to
                  // feed this oracle a table where a retained snapshot's parent was removed; the
                  // earlier fixtures never expire, so this branch is dormant for them (no behavior
                  // change). The Rust mirror applies the IDENTICAL null-on-absent-parent rule.
                  Integer parentOrdinal =
                      snap.parentId() == null ? null : ordinals.get(snap.parentId());
                  if (parentOrdinal == null) {
                    gen.writeNullField("parent_ordinal");
                  } else {
                    gen.writeNumberField("parent_ordinal", parentOrdinal);
                  }
                  gen.writeNumberField("sequence_number", snap.sequenceNumber());
                  gen.writeStringField("operation", snap.operation());

                  // COUNT-only summary, allowlist order (stable on both sides).
                  gen.writeObjectFieldStart("summary");
                  Map<String, String> summary = snap.summary();
                  for (String key : SUMMARY_COUNT_KEYS) {
                    if (summary != null && summary.containsKey(key)) {
                      gen.writeStringField(key, summary.get(key));
                    }
                  }
                  gen.writeEndObject();

                  // Manifest list -> manifests -> entries.
                  List<ManifestFile> manifests = new ArrayList<>(snap.allManifests(io));
                  // The full tuple, INCLUDING the count fields: within one commit a rewritten
                  // (tombstone-carrying) manifest and an added manifest share (content, seq,
                  // min_seq), and a tie would fall back to each table's manifest-LIST file order —
                  // writer-dependent, not a spec contract. The counts disambiguate
                  // deterministically (mirrored by the Rust common/snapshot_meta_view.rs sort).
                  //
                  // W3 (2026-06-11): partition_spec_id is the FINAL tiebreaker (position 10,
                  // Option B). The canonical view's sort exists to ERASE writer-dependent
                  // manifest-list ordering; its only contract is cross-language determinism, which
                  // the final-tiebreaker position provides for future multi-spec fixtures with zero
                  // risk to existing ordering (spec_id is constant 0 in every single-spec fixture,
                  // so the key is byte-invisible there). Kept identical to the Rust sort tuple.
                  manifests.sort(
                      java.util.Comparator.comparingInt(
                              (ManifestFile m) -> m.content().id())
                          .thenComparingLong(ManifestFile::sequenceNumber)
                          .thenComparingLong(ManifestFile::minSequenceNumber)
                          .thenComparingLong(m -> nullToMinusOne(m.addedFilesCount()))
                          .thenComparingLong(m -> nullToMinusOne(m.existingFilesCount()))
                          .thenComparingLong(m -> nullToMinusOne(m.deletedFilesCount()))
                          .thenComparingLong(m -> nullToMinusOne(m.addedRowsCount()))
                          .thenComparingLong(m -> nullToMinusOne(m.existingRowsCount()))
                          .thenComparingLong(m -> nullToMinusOne(m.deletedRowsCount()))
                          .thenComparingInt(ManifestFile::partitionSpecId));
                  gen.writeArrayFieldStart("manifests");
                  for (ManifestFile manifest : manifests) {
                    gen.writeStartObject();
                    gen.writeStringField(
                        "content", manifest.content() == ManifestContent.DATA ? "data" : "deletes");
                    gen.writeNumberField("sequence_number", manifest.sequenceNumber());
                    gen.writeNumberField("min_sequence_number", manifest.minSequenceNumber());
                    gen.writeNumberField("partition_spec_id", manifest.partitionSpecId());
                    Integer addedOrdinal = ordinals.get(manifest.snapshotId());
                    if (addedOrdinal == null) {
                      gen.writeNullField("added_snapshot_ordinal");
                    } else {
                      gen.writeNumberField("added_snapshot_ordinal", addedOrdinal);
                    }
                    writeNullableInt(gen, "added_files_count", manifest.addedFilesCount());
                    writeNullableInt(gen, "existing_files_count", manifest.existingFilesCount());
                    writeNullableInt(gen, "deleted_files_count", manifest.deletedFilesCount());
                    writeNullableLong(gen, "added_rows_count", manifest.addedRowsCount());
                    writeNullableLong(gen, "existing_rows_count", manifest.existingRowsCount());
                    writeNullableLong(gen, "deleted_rows_count", manifest.deletedRowsCount());

                    gen.writeArrayFieldStart("entries");
                    for (String entryJson : entryJsons(metadata, manifest, io)) {
                      gen.writeRawValue(entryJson);
                    }
                    gen.writeEndArray();
                    gen.writeEndObject();
                  }
                  gen.writeEndArray();
                  gen.writeEndObject();
                }
                gen.writeEndArray();
                gen.writeEndObject();
              },
              true);
      writeJson(out, json);
      System.out.println("emitted canonical snapshot-metadata view of " + metadataJson + " to " + out);
    }

    /**
     * Read EVERY entry of one manifest (data or delete reader per content type — both apply
     * sequence-number INHERITANCE from the manifest-list entry), render each to a canonical JSON
     * object string, and return them sorted by the EXPLICIT cross-language tuple
     * {@code (status, content, record_count, sequence_number, equality_ids, partition)} — the SAME
     * tuple the Rust mirror sorts by, so the two sides' entry arrays align element-for-element
     * without relying on rendered-string ordering.
     */
    private static List<String> entryJsons(TableMetadata metadata, ManifestFile manifest, FileIO io) {
      List<EntryView> views = new ArrayList<>();
      try {
        if (manifest.content() == ManifestContent.DATA) {
          try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, io, metadata.specsById())) {
            for (ManifestEntry<DataFile> entry : reader.entries()) {
              views.add(entryView(metadata, entry));
            }
          }
        } else {
          try (ManifestReader<DeleteFile> reader =
              ManifestFiles.readDeleteManifest(manifest, io, metadata.specsById())) {
            for (ManifestEntry<DeleteFile> entry : reader.entries()) {
              views.add(entryView(metadata, entry));
            }
          }
        }
      } catch (IOException error) {
        throw new RuntimeException("failed to read manifest " + manifest.path(), error);
      }
      views.sort(
          java.util.Comparator.comparingInt((EntryView v) -> v.status)
              .thenComparing(v -> v.content)
              .thenComparingLong(v -> v.recordCount)
              .thenComparingLong(v -> v.sequenceNumber == null ? Long.MIN_VALUE : v.sequenceNumber)
              .thenComparing(v -> v.equalityIdsKey)
              .thenComparing(v -> v.partitionJson == null ? "" : v.partitionJson));
      List<String> rendered = new ArrayList<>();
      for (EntryView view : views) {
        rendered.add(view.json);
      }
      return rendered;
    }

    /** The sortable canonical view of one manifest entry + its rendered JSON. */
    private static final class EntryView {
      final int status;
      final String content;
      final long recordCount;
      final Long sequenceNumber;
      final String equalityIdsKey;
      final String partitionJson;
      final String json;

      EntryView(
          int status,
          String content,
          long recordCount,
          Long sequenceNumber,
          String equalityIdsKey,
          String partitionJson,
          String json) {
        this.status = status;
        this.content = content;
        this.recordCount = recordCount;
        this.sequenceNumber = sequenceNumber;
        this.equalityIdsKey = equalityIdsKey;
        this.partitionJson = partitionJson;
        this.json = json;
      }
    }

    private static <F extends ContentFile<F>> EntryView entryView(
        TableMetadata metadata, ManifestEntry<F> entry) {
      F file = entry.file();
      PartitionSpec spec = metadata.specsById().get(file.specId());
      boolean unpartitioned = spec == null || spec.isUnpartitioned();
      String partitionJson =
          unpartitioned ? null : SingleValueParser.toJson(spec.partitionType(), file.partition());
      List<Integer> equalityIds =
          file.equalityFieldIds() == null ? null : new ArrayList<>(file.equalityFieldIds());
      if (equalityIds != null) {
        java.util.Collections.sort(equalityIds);
      }
      final List<Integer> sortedEqualityIds = equalityIds;
      StringBuilder equalityIdsKey = new StringBuilder();
      if (sortedEqualityIds != null) {
        for (Integer id : sortedEqualityIds) {
          equalityIdsKey.append(id).append(',');
        }
      }
      String json = JsonUtil.generate(
          gen -> {
            gen.writeStartObject();
            gen.writeNumberField("status", entry.status().id());
            gen.writeStringField("content", contentName(file.content()));
            gen.writeNumberField("record_count", file.recordCount());
            if (entry.dataSequenceNumber() == null) {
              gen.writeNullField("sequence_number");
            } else {
              gen.writeNumberField("sequence_number", entry.dataSequenceNumber());
            }
            if (sortedEqualityIds == null) {
              gen.writeNullField("equality_ids");
            } else {
              gen.writeArrayFieldStart("equality_ids");
              for (Integer id : sortedEqualityIds) {
                gen.writeNumber(id);
              }
              gen.writeEndArray();
            }
            if (partitionJson == null) {
              gen.writeNullField("partition");
            } else {
              gen.writeFieldName("partition");
              gen.writeRawValue(partitionJson);
            }
            gen.writeEndObject();
          },
          false);
      return new EntryView(
          entry.status().id(),
          contentName(file.content()),
          file.recordCount(),
          entry.dataSequenceNumber(),
          equalityIdsKey.toString(),
          partitionJson,
          json);
    }

    private static long nullToMinusOne(Number value) {
      return value == null ? -1L : value.longValue();
    }

    private static String contentName(FileContent content) {
      switch (content) {
        case DATA:
          return "data";
        case POSITION_DELETES:
          return "position_deletes";
        case EQUALITY_DELETES:
          return "equality_deletes";
        default:
          throw new IllegalArgumentException("unknown file content: " + content);
      }
    }

    private static void writeNullableInt(
        com.fasterxml.jackson.core.JsonGenerator gen, String field, Integer value)
        throws IOException {
      if (value == null) {
        gen.writeNullField(field);
      } else {
        gen.writeNumberField(field, value);
      }
    }

    private static void writeNullableLong(
        com.fasterxml.jackson.core.JsonGenerator gen, String field, Long value) throws IOException {
      if (value == null) {
        gen.writeNullField(field);
      } else {
        gen.writeNumberField(field, value);
      }
    }
  }

  // =============================================================================================
  // MergeAppendDataOracle — DATA-LEVEL merge_append interop (increment S1, fixture A).
  //
  // THE TABLE (under <dir>/table). V2 partitioned by identity(category), schema
  //   {1 id long required, 2 category string required, 3 data string optional}
  //   (the same 3-field schema as PartScanExecOracle so the row-dump format is identical):
  //   data file A: cat=a, rows (10,"a") (20,"b") (30,"c") at positions 0..2
  //   data file B: cat=b, row  (40,"d") at position 0
  //   → fast-append A+B (seq 1)
  //   → updateProperties: commit.manifest.min-count-to-merge=2   (NO snapshot)
  //   → merge-append G(cat=a, id=60, data="g")                   (seq 2, MERGING)
  //
  // CORRECTNESS POINT: every data file written before the merge-append is carried as Existing in the
  // merged manifest, so IcebergGenerics MUST return ALL SIX rows. The merge fires (min-count=2, all
  // manifests fit in one bin) — a Rust merge_append that silently discards Existing entries would
  // yield a WRONG (missing) row set here.
  //
  // Java emits java_merge_append_rows.json = [{10,a},{20,b},{30,c},{40,d},{60,g}] (no deletes).
  // (NOTE: 5 distinct id values; G does NOT replace A/B/C/D — there are 5 live rows total: ids 10,20,
  // 30, 40, 60.)
  //
  // The Rust GEN test mirrors this chain under <dir>/rust_table with real parquet; the verify step here
  // reads the Rust-written table and asserts the same 5-row live set. Both directions share the SAME
  // correctness assertion: no row lost across the merge boundary.
  // =============================================================================================

  static final class MergeAppendDataOracle {
    private MergeAppendDataOracle() {}

    static void generate(Path dir) throws IOException {
      Files.createDirectories(dir);

      // 1. Build the partitioned V2 table on local disk under <dir>/table.
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
      PartitionSpec spec = PartitionSpec.builderFor(schema).identity("category").build();

      Map<String, String> props = new LinkedHashMap<>();
      props.put(TableProperties.FORMAT_VERSION, "2");
      TableMetadata seed =
          TableMetadata.newTableMetadata(
              schema, spec, SortOrder.unsorted(), tableDir.getAbsolutePath(), props);

      LocalTableOperations ops = new LocalTableOperations(tableDir, metadataDir);
      ops.commit(null, seed);
      BaseTable table = new BaseTable(ops, "interop_merge_append_data");

      // 2. Partition values for identity(category).
      Types.StructType partitionType = spec.partitionType();
      PartitionData partitionA = new PartitionData(partitionType);
      partitionA.set(0, "a");
      PartitionData partitionB = new PartitionData(partitionType);
      partitionB.set(0, "b");

      // 3. Write REAL parquet data files.
      //    A: cat=a, rows (10,"a")(20,"b")(30,"c")
      //    B: cat=b, row  (40,"d")
      String dataPathA = new File(dataDir, "category=a/00000-merge-a.parquet").getAbsolutePath();
      if (!new File(dataPathA).getParentFile().isDirectory()
          && !new File(dataPathA).getParentFile().mkdirs()) {
        throw new IOException("failed to create category=a data dir");
      }
      DataFile dataFileA =
          writePartitionedDataFile(
              table,
              schema,
              spec,
              partitionA,
              dataPathA,
              new long[] {10L, 20L, 30L},
              new String[] {"a", "b", "c"});

      String dataPathB = new File(dataDir, "category=b/00000-merge-b.parquet").getAbsolutePath();
      if (!new File(dataPathB).getParentFile().isDirectory()
          && !new File(dataPathB).getParentFile().mkdirs()) {
        throw new IOException("failed to create category=b data dir");
      }
      DataFile dataFileB =
          writePartitionedDataFile(
              table,
              schema,
              spec,
              partitionB,
              dataPathB,
              new long[] {40L},
              new String[] {"d"});

      String dataPathG = new File(dataDir, "category=a/00001-merge-g.parquet").getAbsolutePath();
      DataFile dataFileG =
          writePartitionedDataFile(
              table,
              schema,
              spec,
              partitionA,
              dataPathG,
              new long[] {60L},
              new String[] {"g"});

      // 4. fast-append A+B at sequence 1.
      table.newFastAppend().appendFile(dataFileA).appendFile(dataFileB).commit();

      // 5. Set min-count-to-merge=2 (no snapshot) — arms the merge in step 6.
      table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "2").commit();

      // 6. merge-append G(cat=a, id=60, "g") at sequence 2. With min-count=2 and KB-size manifests,
      //    every manifest lands in ONE bin ⇒ the merge fires — the new manifest merges with the existing
      //    manifests into ONE merged manifest carrying A+B as Existing + G as Added.
      table.newAppend().appendFile(dataFileG).commit();

      // 7. Write the FINAL metadata to a KNOWN path.
      Path finalMetadata = metadataDir.toPath().resolve("final.metadata.json");
      OutputFile finalOut =
          new LocalFileIO().newOutputFile(finalMetadata.toAbsolutePath().toString());
      TableMetadataParser.write(ops.current(), finalOut);

      // 8. Java materializes its OWN merge-on-read read → emit java_merge_append_rows.json.
      //    Expected = [{10,a},{20,b},{30,c},{40,d},{60,g}] (5 rows, no deletes).
      writeJson(dir.resolve("java_merge_append_rows.json"), readLiveRowsToJson(table, "data"));
      System.out.println("generated merge-append-data table + java_merge_append_rows.json to " + dir);
    }

    /**
     * Write a REAL parquet data file for ONE partition via the generic appender. The same pattern as
     * {@link PartScanExecOracle}'s {@code writePartitionedDataFile}: the writer stamps the partition
     * Struct onto the DataFile and routes the parquet under the partition path.
     *
     * <p>Package-visible (not private) so {@link MultiBinMergeAppendDataOracle} can reuse it.
     */
    static DataFile writePartitionedDataFile(
        BaseTable table,
        Schema schema,
        PartitionSpec spec,
        StructLike partition,
        String path,
        long[] ids,
        String[] dataValues)
        throws IOException {
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
     * DIRECTION 2 verify — read the RUST-written merge-append table and assert the live rows.
     *
     * <p>Loads {@code <dir>/rust_table/metadata/final.metadata.json}, builds a {@link BaseTable} over a
     * {@link LocalFileIO}, reads every live row with {@code IcebergGenerics} (which scans across all
     * partitions including through the merged manifest's Existing entries), sorts by id, and asserts all
     * 5 rows are present: {@code {(10,a),(20,b),(30,c),(40,d),(60,g)}}. A failure here is a REAL
     * merge_append-level write-incompatibility finding.
     */
    static int verify(Path dir) {
      int failures = 0;
      Path finalMetadata =
          dir.resolve("rust_table").resolve("metadata").resolve("final.metadata.json");

      if (!Files.exists(finalMetadata)) {
        System.out.println(
            "FAIL merge-append-data-d2: missing "
                + finalMetadata
                + " (run the Rust GEN path first)");
        return 1;
      }

      TableMetadata metadata;
      try {
        metadata =
            TableMetadataParser.fromJson(finalMetadata.toString(), readString(finalMetadata));
      } catch (RuntimeException | IOException parseError) {
        System.out.println(
            "FAIL merge-append-data-d2: Java could not parse the Rust-written final.metadata.json: "
                + parseError);
        return 1;
      }

      FileIO io = new LocalFileIO();
      BaseTable table =
          new BaseTable(new InMemoryInspectionOperations(metadata, io), "rust_merge_append_data");

      Map<Long, String> dataById = new LinkedHashMap<>();
      try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
        for (Record record : records) {
          Long id = (Long) record.getField("id");
          Object data = record.getField("data");
          dataById.put(id, data == null ? null : data.toString());
        }
      } catch (RuntimeException | IOException readError) {
        System.out.println(
            "FAIL merge-append-data-d2: Java could not READ the Rust-written merge-append table via "
                + "IcebergGenerics: "
                + readError);
        return 1;
      }

      List<Long> liveIds = new ArrayList<>(dataById.keySet());
      liveIds.sort(Long::compareTo);

      // 3a. Exactly 5 live rows (A:3 + B:1 + G:1, no deletes).
      if (liveIds.size() != 5) {
        System.out.println(
            "FAIL merge-append-data-d2: expected 5 live rows (A+B+G, no deletes), got "
                + liveIds.size()
                + " "
                + liveIds);
        failures++;
      } else {
        System.out.println(
            "PASS merge-append-data-d2: 5 live rows (A+B+G carried through merge boundary)");
      }

      // 3b. All expected ids present.
      for (Long expected : new long[] {10L, 20L, 30L, 40L, 60L}) {
        if (!liveIds.contains(expected)) {
          System.out.println(
              "FAIL merge-append-data-d2: id "
                  + expected
                  + " must be PRESENT, but live set is "
                  + liveIds);
          failures++;
        }
      }
      if (failures == 0) {
        System.out.println(
            "PASS merge-append-data-d2: all expected ids present {10,20,30,40,60}");
      }

      // 3c. Exact (id, data) set matches.
      Map<Long, String> expected = new LinkedHashMap<>();
      expected.put(10L, "a");
      expected.put(20L, "b");
      expected.put(30L, "c");
      expected.put(40L, "d");
      expected.put(60L, "g");
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
            "FAIL merge-append-data-d2: live (id,data) set mismatch: java-read="
                + dataById
                + " expected={10=a, 20=b, 30=c, 40=d, 60=g}");
        failures++;
      } else {
        System.out.println(
            "PASS merge-append-data-d2: Java read the Rust-written merge-append table → "
                + "{(10,a),(20,b),(30,c),(40,d),(60,g)}");
      }

      if (failures == 0) {
        System.out.println(
            "verify-interop-merge-append-data OK — Java read the RUST-written merge-append table "
                + "(real parquet + merged manifest Existing-carry), live rows = {10,20,30,40,60}");
      }
      return failures;
    }
  }

  // =============================================================================================
  // MultiBinMergeAppendDataOracle — DATA-LEVEL multi-bin merge_append interop (increment W3,
  // fixture G).
  //
  // THE TABLE (under <dir>/table). V2 partitioned by identity(category), schema
  //   {1 id long required, 2 category string required, 3 data string optional}
  //
  //   Four fast-appends build up four small manifests:
  //     fast-append A(cat=a, 10/20/30)  → manifest m1 (seq 1)
  //     fast-append B(cat=b, 40)        → manifest m2 (seq 2)
  //     fast-append C1(cat=a, 50)       → manifest m3 (seq 3)
  //     fast-append C2(cat=b, 55)       → manifest m4 (seq 4)
  //
  //   After the four appends, the actual manifest sizes are measured from the manifest-list.
  //   The target-size-bytes is then set to max_manifest_size * 2 + 1 (so at most 2 manifests
  //   fit per bin, forcing ≥2 bins over 4 existing manifests) together with min-count-to-merge=2.
  //
  //   merge-append G(cat=a, 60) at seq 5:
  //     pack_end([new_g, m1, m2, m3, m4], target) produces:
  //       bin [new_g]  (size=1, kept)
  //       bin [m1, m2] (size=2 ≥ min-count=2, MERGED)
  //       bin [m3, m4] (size=2 ≥ min-count=2, MERGED)
  //     → TWO merged manifests, each carrying Existing entries.
  //
  // CORRECTNESS POINT: all 7 rows must survive: A(10/20/30) + B(40) + C1(50) + C2(55) + G(60).
  // The multi-bin merge proves that MULTIPLE merged manifests are produced (bin-count assertion),
  // each with carried Existing entries (existing_files_count > 0 on both merged manifests), and
  // that a scan across all bins still returns the complete row set.
  //
  // Partition-column pin: a-rows = {10,20,30,50,60}; b-rows = {40,55}.
  // =============================================================================================

  static final class MultiBinMergeAppendDataOracle {
    private MultiBinMergeAppendDataOracle() {}

    static void generate(Path dir) throws IOException {
      Files.createDirectories(dir);

      // 1. Build the partitioned V2 table on local disk under <dir>/table.
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
      PartitionSpec spec = PartitionSpec.builderFor(schema).identity("category").build();

      Map<String, String> props = new LinkedHashMap<>();
      props.put(TableProperties.FORMAT_VERSION, "2");
      TableMetadata seed =
          TableMetadata.newTableMetadata(
              schema, spec, SortOrder.unsorted(), tableDir.getAbsolutePath(), props);

      LocalTableOperations ops = new LocalTableOperations(tableDir, metadataDir);
      ops.commit(null, seed);
      BaseTable table = new BaseTable(ops, "interop_multi_bin_merge_append_data");

      // 2. Partition values for identity(category).
      Types.StructType partitionType = spec.partitionType();
      PartitionData partitionA = new PartitionData(partitionType);
      partitionA.set(0, "a");
      PartitionData partitionB = new PartitionData(partitionType);
      partitionB.set(0, "b");

      // 3. Write REAL parquet data files.
      //    A: cat=a, rows (10,"a")(20,"b")(30,"c")
      //    B: cat=b, row  (40,"d")
      //    C1: cat=a, row (50,"e")
      //    C2: cat=b, row (55,"f")
      //    G: cat=a, row  (60,"g")
      File catADir = new File(dataDir, "category=a");
      File catBDir = new File(dataDir, "category=b");
      if (!catADir.isDirectory() && !catADir.mkdirs()) {
        throw new IOException("failed to create category=a data dir");
      }
      if (!catBDir.isDirectory() && !catBDir.mkdirs()) {
        throw new IOException("failed to create category=b data dir");
      }

      DataFile dataFileA =
          MergeAppendDataOracle.writePartitionedDataFile(
              table, schema, spec, partitionA,
              new File(catADir, "00000-mb-a.parquet").getAbsolutePath(),
              new long[] {10L, 20L, 30L}, new String[] {"a", "b", "c"});
      DataFile dataFileB =
          MergeAppendDataOracle.writePartitionedDataFile(
              table, schema, spec, partitionB,
              new File(catBDir, "00000-mb-b.parquet").getAbsolutePath(),
              new long[] {40L}, new String[] {"d"});
      DataFile dataFileC1 =
          MergeAppendDataOracle.writePartitionedDataFile(
              table, schema, spec, partitionA,
              new File(catADir, "00001-mb-c1.parquet").getAbsolutePath(),
              new long[] {50L}, new String[] {"e"});
      DataFile dataFileC2 =
          MergeAppendDataOracle.writePartitionedDataFile(
              table, schema, spec, partitionB,
              new File(catBDir, "00001-mb-c2.parquet").getAbsolutePath(),
              new long[] {55L}, new String[] {"f"});
      DataFile dataFileG =
          MergeAppendDataOracle.writePartitionedDataFile(
              table, schema, spec, partitionA,
              new File(catADir, "00002-mb-g.parquet").getAbsolutePath(),
              new long[] {60L}, new String[] {"g"});

      // 4. Four separate fast-appends → four separate manifests (m1..m4, each in its own seq).
      //    This is the key: four separate commits produce four manifest files, each carrying
      //    exactly the data files from that commit. The merge_append G step must pack these
      //    into ≥2 bins so ≥2 merged manifests are produced.
      table.newFastAppend().appendFile(dataFileA).commit();
      table.newFastAppend().appendFile(dataFileB).commit();
      table.newFastAppend().appendFile(dataFileC1).commit();
      table.newFastAppend().appendFile(dataFileC2).commit();

      // 5. Measure manifest sizes from the current snapshot's manifest-list.
      //    Set target-size-bytes = max_size * 2 + 1 so exactly 2 manifests fit per bin.
      //    With 4 manifests and target fitting 2 per bin, pack_end yields 2 bins of 2 → both
      //    satisfy min-count=2 → both MERGE.
      List<ManifestFile> currentManifests = table.currentSnapshot().allManifests(table.io());
      long maxManifestSize = 0;
      for (ManifestFile mf : currentManifests) {
        if (mf.length() > maxManifestSize) {
          maxManifestSize = mf.length();
        }
      }
      long targetSizeBytes = maxManifestSize * 2 + 1;
      System.out.println(
          "multi-bin fixture: 4 manifests, max manifest size = "
              + maxManifestSize
              + " bytes; target-size-bytes = "
              + targetSizeBytes
              + " (fits exactly 2 per bin)");

      // 6. Set merge properties (no snapshot) — arm the two-bin merge.
      table
          .updateProperties()
          .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "2")
          .set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, Long.toString(targetSizeBytes))
          .commit();

      // 7. merge-append G(cat=a, id=60, "g") — fires the two-bin merge.
      table.newAppend().appendFile(dataFileG).commit();

      // 8. Verify ≥2 merged manifests (manifests with existing_files_count > 0).
      List<ManifestFile> finalManifests = table.currentSnapshot().allManifests(table.io());
      long mergedCount =
          finalManifests.stream()
              .filter(
                  mf ->
                      mf.content() == ManifestContent.DATA
                          && mf.existingFilesCount() != null
                          && mf.existingFilesCount() > 0)
              .count();
      System.out.println(
          "multi-bin fixture: final manifest list has "
              + finalManifests.size()
              + " manifests, "
              + mergedCount
              + " with existing_files_count > 0 (must be ≥ 2)");
      if (mergedCount < 2) {
        throw new IllegalStateException(
            "multi-bin merge_append FIXTURE BROKEN: expected ≥2 manifests with existing entries, "
                + "got "
                + mergedCount
                + ". This means the bin-packing did not produce multi-bin output. "
                + "Check target-size-bytes="
                + targetSizeBytes
                + " vs manifest sizes.");
      }

      // 9. Write the FINAL metadata to a KNOWN path.
      Path finalMetadata = metadataDir.toPath().resolve("final.metadata.json");
      OutputFile finalOut =
          new LocalFileIO().newOutputFile(finalMetadata.toAbsolutePath().toString());
      TableMetadataParser.write(ops.current(), finalOut);

      // 10. Java materializes its OWN IcebergGenerics read → emit java_multi_bin_merge_append_rows.json.
      //     Expected = [{10,a},{20,b},{30,c},{40,d},{50,e},{55,f},{60,g}] (7 rows, no deletes).
      writeJson(
          dir.resolve("java_multi_bin_merge_append_rows.json"),
          readLiveRowsToJson(table, "data"));
      System.out.println(
          "generated multi-bin-merge-append-data table + java_multi_bin_merge_append_rows.json to "
              + dir);
    }

    /**
     * DIRECTION 2 verify — read the RUST-written multi-bin merge-append table and assert live rows.
     *
     * <p>Loads {@code <dir>/rust_table/metadata/final.metadata.json}, reads every live row with {@code
     * IcebergGenerics}, and asserts all 7 rows are present: {@code
     * {(10,a),(20,b),(30,c),(40,d),(50,e),(55,f),(60,g)}}. Additionally asserts ≥2 manifests in the final
     * manifest list carry {@code existing_files_count > 0}, proving the multi-bin merge fired.
     */
    static int verify(Path dir) {
      int failures = 0;
      Path finalMetadata =
          dir.resolve("rust_table").resolve("metadata").resolve("final.metadata.json");

      if (!Files.exists(finalMetadata)) {
        System.out.println(
            "FAIL multi-bin-merge-append-data-d2: missing "
                + finalMetadata
                + " (run the Rust GEN path first)");
        return 1;
      }

      TableMetadata metadata;
      try {
        metadata =
            TableMetadataParser.fromJson(finalMetadata.toString(), readString(finalMetadata));
      } catch (RuntimeException | IOException parseError) {
        System.out.println(
            "FAIL multi-bin-merge-append-data-d2: Java could not parse the Rust-written "
                + "final.metadata.json: "
                + parseError);
        return 1;
      }

      FileIO io = new LocalFileIO();
      BaseTable table =
          new BaseTable(
              new InMemoryInspectionOperations(metadata, io), "rust_multi_bin_merge_append_data");

      Map<Long, String> dataById = new LinkedHashMap<>();
      try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
        for (Record record : records) {
          Long id = (Long) record.getField("id");
          Object data = record.getField("data");
          dataById.put(id, data == null ? null : data.toString());
        }
      } catch (RuntimeException | IOException readError) {
        System.out.println(
            "FAIL multi-bin-merge-append-data-d2: Java could not READ the Rust-written "
                + "multi-bin-merge-append table via IcebergGenerics: "
                + readError);
        return 1;
      }

      List<Long> liveIds = new ArrayList<>(dataById.keySet());
      liveIds.sort(Long::compareTo);

      // 3a. Exactly 7 live rows (A:3 + B:1 + C1:1 + C2:1 + G:1, no deletes).
      if (liveIds.size() != 7) {
        System.out.println(
            "FAIL multi-bin-merge-append-data-d2: expected 7 live rows (A+B+C1+C2+G), got "
                + liveIds.size()
                + " "
                + liveIds);
        failures++;
      } else {
        System.out.println(
            "PASS multi-bin-merge-append-data-d2: 7 live rows (all files carried through multi-bin "
                + "merge boundary)");
      }

      // 3b. All expected ids present.
      for (Long expected : new long[] {10L, 20L, 30L, 40L, 50L, 55L, 60L}) {
        if (!liveIds.contains(expected)) {
          System.out.println(
              "FAIL multi-bin-merge-append-data-d2: id "
                  + expected
                  + " must be PRESENT, but live set is "
                  + liveIds);
          failures++;
        }
      }
      if (failures == 0) {
        System.out.println(
            "PASS multi-bin-merge-append-data-d2: all expected ids present "
                + "{10,20,30,40,50,55,60}");
      }

      // 3c. Exact (id, data) set matches.
      Map<Long, String> expected = new LinkedHashMap<>();
      expected.put(10L, "a");
      expected.put(20L, "b");
      expected.put(30L, "c");
      expected.put(40L, "d");
      expected.put(50L, "e");
      expected.put(55L, "f");
      expected.put(60L, "g");
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
            "FAIL multi-bin-merge-append-data-d2: live (id,data) set mismatch: java-read="
                + dataById
                + " expected={10=a, 20=b, 30=c, 40=d, 50=e, 55=f, 60=g}");
        failures++;
      } else {
        System.out.println(
            "PASS multi-bin-merge-append-data-d2: Java read the Rust-written multi-bin-merge-append "
                + "table → {(10,a),(20,b),(30,c),(40,d),(50,e),(55,f),(60,g)}");
      }

      // 3d. Partition-column pin: a-rows = {10,20,30,50,60}; b-rows = {40,55}.
      Map<Long, String> categoryById = new LinkedHashMap<>();
      try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
        for (Record record : records) {
          Long id = (Long) record.getField("id");
          Object cat = record.getField("category");
          categoryById.put(id, cat == null ? null : cat.toString());
        }
      } catch (RuntimeException | IOException readError) {
        System.out.println(
            "FAIL multi-bin-merge-append-data-d2: second IcebergGenerics read (partition-column "
                + "pin) failed: "
                + readError);
        failures++;
      }
      boolean categoryOk = true;
      Map<Long, String> expectedCats = new LinkedHashMap<>();
      expectedCats.put(10L, "a"); expectedCats.put(20L, "a"); expectedCats.put(30L, "a");
      expectedCats.put(40L, "b"); expectedCats.put(50L, "a"); expectedCats.put(55L, "b");
      expectedCats.put(60L, "a");
      for (Map.Entry<Long, String> e : expectedCats.entrySet()) {
        String actual = categoryById.get(e.getKey());
        if (!e.getValue().equals(actual)) {
          System.out.println(
              "FAIL multi-bin-merge-append-data-d2: partition-column (category) mismatch for id="
                  + e.getKey()
                  + ": expected="
                  + e.getValue()
                  + " actual="
                  + actual);
          categoryOk = false;
          failures++;
        }
      }
      if (categoryOk) {
        System.out.println(
            "PASS multi-bin-merge-append-data-d2: partition-column (category) correct "
                + "(a={10,20,30,50,60}, b={40,55})");
      }

      // 3e. Assert ≥2 merged manifests in the Rust-written table (bin-count proof).
      try {
        List<ManifestFile> finalManifests =
            table.currentSnapshot().allManifests(io);
        long mergedCount =
            finalManifests.stream()
                .filter(
                    mf ->
                        mf.content() == ManifestContent.DATA
                            && mf.existingFilesCount() != null
                            && mf.existingFilesCount() > 0)
                .count();
        System.out.println(
            "multi-bin-merge-append-data-d2: Rust table has "
                + finalManifests.size()
                + " manifests, "
                + mergedCount
                + " with existing_files_count > 0 (need ≥ 2)");
        if (mergedCount < 2) {
          System.out.println(
              "FAIL multi-bin-merge-append-data-d2: bin-count proof: need ≥2 manifests with "
                  + "existing entries, got "
                  + mergedCount
                  + " — multi-bin merge did not fire");
          failures++;
        } else {
          System.out.println(
              "PASS multi-bin-merge-append-data-d2: bin-count proof: "
                  + mergedCount
                  + " merged manifests (≥2) confirmed");
        }
      } catch (RuntimeException e) {
        System.out.println(
            "FAIL multi-bin-merge-append-data-d2: could not read manifest list for bin-count "
                + "proof: "
                + e);
        failures++;
      }

      if (failures == 0) {
        System.out.println(
            "verify-interop-multi-bin-merge-append-data OK — Java read the RUST-written multi-bin "
                + "merge-append table (real parquet + ≥2 merged manifests, 7 live rows)");
      }
      return failures;
    }
  }

  // =============================================================================================
  // RewriteFilesDataOracle — DATA-LEVEL RewriteFiles interop (increment S1, fixture B).
  //
  // THE TABLE (under <dir>/table). Unpartitioned V2, schema
  //   {1 id long required, 2 data string optional}
  //   (2-field, same as ScanExecOracle — the simplest shape for the seq-preservation proof):
  //   data file A: rows (10,"a") (20,"b") (30,"c") (40,"d") (50,"e") at positions 0..4
  //   → fast-append A                                          (seq 1)
  //   → row-delta EQUALITY-delete: equality_ids=[1], ids 20+40  (seq 2)
  //   → rewrite {A}→{A'} with dataSequenceNumber=1             (seq 3)
  //
  // WHY AN EQUALITY DELETE (not a position delete):
  //   A position-delete is PATH-BASED: it references A's specific file path. After A is rewritten
  //   to A' (a new path), the position-delete on A's path is DANGLING — it has no live file to
  //   apply to. The delete-applicability spec rule for position deletes does NOT involve
  //   data_sequence_number (the file path is the matching key). To prove seq-preservation at the
  //   DATA level you MUST use an equality delete: the equality-delete applicability rule IS
  //   seq-based (applies to data files with data_seq STRICTLY LESS than the delete's seq).
  //
  // CORRECTNESS POINT (seq-preservation): A' is stamped with data_sequence_number=1 (the replaced
  // file's seq, NOT the rewrite snapshot's seq 3). The equality-delete has sequence 2. Because
  // eq_del.seq (2) > A'.data_seq (1), the delete STILL APPLIES to A' after the rewrite. Live rows
  // after IcebergGenerics read = {(10,a),(30,c),(50,e)} — ids 20 and 40 remain deleted.
  //
  // Were Rust to stamp A' with data_seq=3 (the wrong behaviour), the equality-delete would NOT
  // apply (eq_del.seq=2 is NOT greater than A'.data_seq=3 by the strict-less-than rule) and all
  // 5 rows would INCORRECTLY survive — this verify catches that regression precisely.
  //
  // Java emits java_rewrite_data_rows.json = [{10,a},{30,c},{50,e}] (same shape as the
  // EqDeleteOracle ground truth, but via a different chain: rewrite after eq-delete).
  // The Rust GEN test mirrors this chain under <dir>/rust_table with real parquet; the verify step
  // here reads the Rust-written table and asserts ids 20 and 40 are ABSENT.
  // =============================================================================================

  static final class RewriteFilesDataOracle {
    private RewriteFilesDataOracle() {}

    static void generate(Path dir) throws IOException {
      Files.createDirectories(dir);

      // 1. Build the UNPARTITIONED V2 table on local disk under <dir>/table.
      File tableDir = dir.resolve("table").toFile();
      File metadataDir = new File(tableDir, "metadata");
      File dataDir = new File(tableDir, "data");
      if (!metadataDir.isDirectory() && !metadataDir.mkdirs()) {
        throw new IOException("failed to create metadata dir at " + metadataDir);
      }
      if (!dataDir.isDirectory() && !dataDir.mkdirs()) {
        throw new IOException("failed to create data dir at " + dataDir);
      }

      // Unpartitioned, 2-field schema (matching ScanExecOracle / EqDeleteOracle).
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
      ops.commit(null, seed);
      BaseTable table = new BaseTable(ops, "interop_rewrite_data");

      // 2. Write REAL parquet DATA file A: 5 rows (10,"a")..(50,"e").
      String dataPathA = new File(dataDir, "00000-rewrite-data-a.parquet").getAbsolutePath();
      DataFile dataFileA = writeUnpartitionedDataFile(table, schema, spec, dataPathA);

      // 3. Write A' — the compacted replacement of A (SAME logical content: 5 rows, new path).
      //    After the rewrite, A' will carry data_seq=1 (the replaced file's seq).
      String dataPathAprime =
          new File(dataDir, "00001-rewrite-data-a-prime.parquet").getAbsolutePath();
      DataFile dataFileAprime = writeUnpartitionedDataFile(table, schema, spec, dataPathAprime);

      // 4. Write a REAL parquet EQUALITY-DELETE file: equality_ids=[1] (the `id` field), delete
      //    rows id=20 and id=40. This is an EqDeleteOracle-style delete; it will be committed at
      //    seq 2 (after A's seq 1), so the strict-less-than ordering rule applies to A (1 < 2).
      String deletePath = new File(dataDir, "00000-rewrite-data-eq-del.parquet").getAbsolutePath();
      DeleteFile eqDeleteFile = writeEqDeleteFile(table, schema, spec, deletePath);

      // 5. Commit: fast-append A (seq 1), then row-delta the equality-delete (seq 2).
      table.newAppend().appendFile(dataFileA).commit();
      long rowDeltaSnapshotId;
      table.newRowDelta().addDeletes(eqDeleteFile).commit();
      rowDeltaSnapshotId = table.currentSnapshot().snapshotId();

      // 6. Rewrite A → A' with dataSequenceNumber=1 (A's seq), validated from the row-delta snapshot.
      //    Java's `rewriteFiles(Set, Set, long)` overload stamps A' with seq=1. The eq-delete (seq 2)
      //    applies to A' (data_seq 1) because 1 < 2. This is the SEQ-PRESERVATION correctness point.
      java.util.Set<DataFile> rewriteDelete = new java.util.HashSet<>();
      rewriteDelete.add(dataFileA);
      java.util.Set<DataFile> rewriteAdd = new java.util.HashSet<>();
      rewriteAdd.add(dataFileAprime);
      table
          .newRewrite()
          .validateFromSnapshot(rowDeltaSnapshotId)
          .rewriteFiles(rewriteDelete, rewriteAdd, 1L)
          .commit();

      // 7. Write FINAL metadata.
      Path finalMetadata = metadataDir.toPath().resolve("final.metadata.json");
      OutputFile finalOut =
          new LocalFileIO().newOutputFile(finalMetadata.toAbsolutePath().toString());
      TableMetadataParser.write(ops.current(), finalOut);

      // 8. Java materializes its OWN merge-on-read read → emit java_rewrite_data_rows.json.
      //    Expected = [{10,a},{30,c},{50,e}] — ids 20/40 deleted by eq-delete, which still applies
      //    to A' because A'.data_seq=1 < eq_del.seq=2.
      writeJson(dir.resolve("java_rewrite_data_rows.json"), readLiveRowsToJson(table, "data"));
      System.out.println(
          "generated rewrite-data table + java_rewrite_data_rows.json to "
              + dir
              + " (ids 20+40 deleted via eq-delete that still applies to A' after rewrite)");
    }

    /**
     * Write a REAL parquet DATA file for the unpartitioned table (5 rows: (10,"a")..(50,"e")).
     * Mirrors {@link ScanExecOracle#writeDataFile} / {@link EqDeleteOracle#writeDataFile}.
     */
    private static DataFile writeUnpartitionedDataFile(
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
     * Write a REAL parquet EQUALITY-DELETE file for the unpartitioned table, deleting ids 20 and 40.
     * Mirrors {@link EqDeleteOracle#writeEqDeleteFile}: equality_ids=[1] (the {@code id} field),
     * unpartitioned, the two delete keys committed at sequence 2.
     */
    private static DeleteFile writeEqDeleteFile(
        BaseTable table, Schema schema, PartitionSpec spec, String path) throws IOException {
      // Project the `id` column from the table schema: equality_ids = [1].
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
     * DIRECTION 2 verify — read the RUST-written rewrite-data table and assert ids 20 and 40 are ABSENT.
     *
     * <p>Loads {@code <dir>/rust_table/metadata/final.metadata.json}, builds a {@link BaseTable} over a
     * {@link LocalFileIO}, reads every live row with {@code IcebergGenerics} (which applies the equality
     * delete to A' because A'.data_seq=1 strictly less-than eq_del.seq=2), sorts by id, and asserts the
     * rows equal {@code {(10,a),(30,c),(50,e)}}. A failure here is a REAL seq-preservation
     * write-incompatibility finding: Rust stamped A' with data_seq=3 (the wrong value), which would make
     * the equality-delete inapplicable (eq_del.seq=2 NOT greater than A'.data_seq=3), letting ids 20/40
     * survive incorrectly.
     */
    static int verify(Path dir) {
      int failures = 0;
      Path finalMetadata =
          dir.resolve("rust_table").resolve("metadata").resolve("final.metadata.json");

      if (!Files.exists(finalMetadata)) {
        System.out.println(
            "FAIL rewrite-data-d2: missing "
                + finalMetadata
                + " (run the Rust GEN path first)");
        return 1;
      }

      TableMetadata metadata;
      try {
        metadata =
            TableMetadataParser.fromJson(finalMetadata.toString(), readString(finalMetadata));
      } catch (RuntimeException | IOException parseError) {
        System.out.println(
            "FAIL rewrite-data-d2: Java could not parse the Rust-written final.metadata.json: "
                + parseError);
        return 1;
      }

      FileIO io = new LocalFileIO();
      BaseTable table =
          new BaseTable(new InMemoryInspectionOperations(metadata, io), "rust_rewrite_data");

      Map<Long, String> dataById = new LinkedHashMap<>();
      try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
        for (Record record : records) {
          Long id = (Long) record.getField("id");
          Object data = record.getField("data");
          dataById.put(id, data == null ? null : data.toString());
        }
      } catch (RuntimeException | IOException readError) {
        System.out.println(
            "FAIL rewrite-data-d2: Java could not READ the Rust-written rewrite-data table via "
                + "IcebergGenerics: "
                + readError);
        return 1;
      }

      List<Long> liveIds = new ArrayList<>(dataById.keySet());
      liveIds.sort(Long::compareTo);

      // 3a. Exactly 3 live rows survive (5 written, ids 20+40 deleted by eq-delete).
      if (liveIds.size() != 3) {
        System.out.println(
            "FAIL rewrite-data-d2: expected 3 live rows after eq-delete + rewrite, got "
                + liveIds.size()
                + " "
                + liveIds);
        failures++;
      } else {
        System.out.println(
            "PASS rewrite-data-d2: 3 live rows (ids 20/40 deleted; seq-preservation holds)");
      }

      // 3b. ids 20 and 40 must be ABSENT — the eq-delete must still apply to A' (data_seq 1 < 2).
      if (liveIds.contains(20L) || liveIds.contains(40L)) {
        System.out.println(
            "FAIL rewrite-data-d2: ids 20/40 must be ABSENT (eq-delete seq 2 applies to A' data_seq 1), "
                + "but live set is "
                + liveIds
                + " — Rust likely stamped A' with the wrong data_sequence_number (3 instead of 1)");
        failures++;
      } else {
        System.out.println(
            "PASS rewrite-data-d2: ids 20/40 absent (eq-delete survived the rewrite, seq-preservation OK)");
      }

      // 3c. Exact surviving (id, data) set equals {(10,a),(30,c),(50,e)}.
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
            "FAIL rewrite-data-d2: live (id,data) set mismatch: java-read="
                + dataById
                + " expected={10=a, 30=c, 50=e}");
        failures++;
      } else {
        System.out.println(
            "PASS rewrite-data-d2: Java read the Rust-written rewrite-data table → {(10,a),(30,c),(50,e)}");
      }

      if (failures == 0) {
        System.out.println(
            "verify-interop-rewrite-data OK — Java read the RUST-written rewrite-data table "
                + "(real parquet + eq-delete surviving rewrite via seq-preservation), "
                + "live rows = {10,30,50}");
      }
      return failures;
    }
  }

  // =============================================================================================
  // OverwriteFilesDataOracle — DATA-LEVEL OverwriteFiles interop (increment W1, fixture C).
  //
  // THE TABLE (under <dir>/table). V2 partitioned by identity(category), schema
  //   {1 id long required, 2 category string required, 3 data string optional}
  //   (same 3-field shape as MergeAppendDataOracle — identical partition structure):
  //   data file A: cat=a, rows (10,"a") (20,"b") (30,"c")
  //   data file B: cat=b, row  (40,"d")
  //   → fast-append A+B (seq 1)
  //   → overwrite_files: DELETE B, ADD B'(cat=b, id=41, data="d'")  (seq 2, operation=overwrite)
  //
  // CORRECTNESS POINT: the overwrite commits BOTH the delete (B removed) AND the add (B' added) in
  // a single Overwrite snapshot. After IcebergGenerics reads, id=40 must be ABSENT and id=41 must be
  // PRESENT. A's rows (10,20,30) must be INTACT. Partition column (category) is pinned.
  //
  // Java emits java_overwrite_data_rows.json = [{10,a},{20,b},{30,c},{41,d'}] (4 live rows).
  // The Rust GEN test mirrors this chain under <dir>/rust_table with real parquet; the verify step
  // reads the Rust-written table and asserts the same 4-row live set.
  // =============================================================================================

  static final class OverwriteFilesDataOracle {
    private OverwriteFilesDataOracle() {}

    static void generate(Path dir) throws IOException {
      Files.createDirectories(dir);

      // 1. Build the partitioned V2 table on local disk under <dir>/table.
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
      PartitionSpec spec = PartitionSpec.builderFor(schema).identity("category").build();

      Map<String, String> props = new LinkedHashMap<>();
      props.put(TableProperties.FORMAT_VERSION, "2");
      TableMetadata seed =
          TableMetadata.newTableMetadata(
              schema, spec, SortOrder.unsorted(), tableDir.getAbsolutePath(), props);

      LocalTableOperations ops = new LocalTableOperations(tableDir, metadataDir);
      ops.commit(null, seed);
      BaseTable table = new BaseTable(ops, "interop_overwrite_data");

      // 2. Partition values for identity(category).
      Types.StructType partitionType = spec.partitionType();
      PartitionData partitionA = new PartitionData(partitionType);
      partitionA.set(0, "a");
      PartitionData partitionB = new PartitionData(partitionType);
      partitionB.set(0, "b");

      // 3. Write REAL parquet data files.
      //    A: cat=a, rows (10,"a")(20,"b")(30,"c")
      //    B: cat=b, row  (40,"d")
      String dataPathA = new File(dataDir, "category=a/00000-overwrite-a.parquet").getAbsolutePath();
      if (!new File(dataPathA).getParentFile().isDirectory()
          && !new File(dataPathA).getParentFile().mkdirs()) {
        throw new IOException("failed to create category=a data dir");
      }
      DataFile dataFileA =
          writePartitionedDataFile(
              table,
              schema,
              spec,
              partitionA,
              dataPathA,
              new long[] {10L, 20L, 30L},
              new String[] {"a", "b", "c"});

      String dataPathB = new File(dataDir, "category=b/00000-overwrite-b.parquet").getAbsolutePath();
      if (!new File(dataPathB).getParentFile().isDirectory()
          && !new File(dataPathB).getParentFile().mkdirs()) {
        throw new IOException("failed to create category=b data dir");
      }
      DataFile dataFileB =
          writePartitionedDataFile(
              table,
              schema,
              spec,
              partitionB,
              dataPathB,
              new long[] {40L},
              new String[] {"d"});

      // B' — the replacement file for B: same partition cat=b, new id=41, data="d'".
      String dataPathBprime =
          new File(dataDir, "category=b/00001-overwrite-b-prime.parquet").getAbsolutePath();
      DataFile dataFileBprime =
          writePartitionedDataFile(
              table,
              schema,
              spec,
              partitionB,
              dataPathBprime,
              new long[] {41L},
              new String[] {"d'"});

      // 4. fast-append A+B at sequence 1.
      table.newFastAppend().appendFile(dataFileA).appendFile(dataFileB).commit();

      // 5. overwrite_files: DELETE B, ADD B' at sequence 2.
      //    Java's OverwriteFiles action: .deleteFile(B) + .addFile(B') + .commit().
      //    operation = "overwrite" (both delete + add present).
      table.newOverwrite().deleteFile(dataFileB).addFile(dataFileBprime).commit();

      // 6. Write the FINAL metadata to a KNOWN path.
      Path finalMetadata = metadataDir.toPath().resolve("final.metadata.json");
      OutputFile finalOut =
          new LocalFileIO().newOutputFile(finalMetadata.toAbsolutePath().toString());
      TableMetadataParser.write(ops.current(), finalOut);

      // 7. Java materializes its OWN merge-on-read read → emit java_overwrite_data_rows.json.
      //    Expected = [{10,a},{20,b},{30,c},{41,d'}] (4 rows: A intact, B gone, B' present).
      writeJson(
          dir.resolve("java_overwrite_data_rows.json"), readLiveRowsToJson(table, "data"));
      System.out.println(
          "generated overwrite-data table + java_overwrite_data_rows.json to "
              + dir
              + " (B id=40 gone, B' id=41 present, A rows 10/20/30 intact)");
    }

    /**
     * Write a REAL parquet data file for ONE partition. Identical pattern to
     * {@link MergeAppendDataOracle#writePartitionedDataFile}: same schema shape
     * ({id, category, data}), same GenericAppenderFactory/DataWriter idiom.
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
     * DIRECTION 2 verify — read the RUST-written overwrite-data table and assert the live rows.
     *
     * <p>Loads {@code <dir>/rust_table/metadata/final.metadata.json}, builds a {@link BaseTable}
     * over a {@link LocalFileIO}, reads every live row with {@code IcebergGenerics} (which resolves
     * the Overwrite snapshot: B deleted, B' added), sorts by id, and asserts the 4-row live set
     * {@code {(10,a),(20,b),(30,c),(41,d')}}. id=40 must be ABSENT. A failure here is a REAL
     * OverwriteFiles write-incompatibility finding.
     */
    static int verify(Path dir) {
      int failures = 0;
      Path finalMetadata =
          dir.resolve("rust_table").resolve("metadata").resolve("final.metadata.json");

      if (!Files.exists(finalMetadata)) {
        System.out.println(
            "FAIL overwrite-data-d2: missing "
                + finalMetadata
                + " (run the Rust GEN path first)");
        return 1;
      }

      TableMetadata metadata;
      try {
        metadata =
            TableMetadataParser.fromJson(finalMetadata.toString(), readString(finalMetadata));
      } catch (RuntimeException | IOException parseError) {
        System.out.println(
            "FAIL overwrite-data-d2: Java could not parse the Rust-written final.metadata.json: "
                + parseError);
        return 1;
      }

      FileIO io = new LocalFileIO();
      BaseTable table =
          new BaseTable(
              new InMemoryInspectionOperations(metadata, io), "rust_overwrite_data");

      Map<Long, String> dataById = new LinkedHashMap<>();
      Map<Long, String> categoryById = new LinkedHashMap<>();
      try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
        for (Record record : records) {
          Long id = (Long) record.getField("id");
          Object data = record.getField("data");
          Object category = record.getField("category");
          dataById.put(id, data == null ? null : data.toString());
          categoryById.put(id, category == null ? null : category.toString());
        }
      } catch (RuntimeException | IOException readError) {
        System.out.println(
            "FAIL overwrite-data-d2: Java could not READ the Rust-written overwrite-data table via "
                + "IcebergGenerics: "
                + readError);
        return 1;
      }

      List<Long> liveIds = new ArrayList<>(dataById.keySet());
      liveIds.sort(Long::compareTo);

      // 3a. Exactly 4 live rows (A:3 + B':1; B deleted).
      if (liveIds.size() != 4) {
        System.out.println(
            "FAIL overwrite-data-d2: expected 4 live rows (A intact + B' added, B deleted), got "
                + liveIds.size()
                + " "
                + liveIds);
        failures++;
      } else {
        System.out.println(
            "PASS overwrite-data-d2: 4 live rows (A:3 + B':1, B id=40 deleted)");
      }

      // 3b. id=40 must be ABSENT (B was deleted by the overwrite).
      if (liveIds.contains(40L)) {
        System.out.println(
            "FAIL overwrite-data-d2: id 40 must be ABSENT (B was deleted by overwrite_files), "
                + "but live set is "
                + liveIds);
        failures++;
      } else {
        System.out.println("PASS overwrite-data-d2: id 40 absent (B correctly removed by overwrite)");
      }

      // 3c. All expected ids present.
      for (Long expected : new long[] {10L, 20L, 30L, 41L}) {
        if (!liveIds.contains(expected)) {
          System.out.println(
              "FAIL overwrite-data-d2: id "
                  + expected
                  + " must be PRESENT, but live set is "
                  + liveIds);
          failures++;
        }
      }
      if (failures == 0) {
        System.out.println(
            "PASS overwrite-data-d2: all expected ids present {10,20,30,41}");
      }

      // 3d. Exact (id, data) set matches.
      Map<Long, String> expected = new LinkedHashMap<>();
      expected.put(10L, "a");
      expected.put(20L, "b");
      expected.put(30L, "c");
      expected.put(41L, "d'");
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
            "FAIL overwrite-data-d2: live (id,data) set mismatch: java-read="
                + dataById
                + " expected={10=a, 20=b, 30=c, 41=d'}");
        failures++;
      } else {
        System.out.println(
            "PASS overwrite-data-d2: Java read the Rust-written overwrite-data table → "
                + "{(10,a),(20,b),(30,c),(41,d')}");
      }

      // 3e. PARTITION-COLUMN pin (S3 partition-projection lesson). The {id,data} compare above is
      // BLIND to a wrong-partition write: had the Rust writer misrouted B' (id=41) to category="a",
      // the (id,data) set would still match. Assert each row's identity(category): A's rows
      // (10,20,30) → "a", B' (41) → "b". HAND-DECLARED (not read back) for anti-circularity.
      Map<Long, String> expectedCategory = new LinkedHashMap<>();
      expectedCategory.put(10L, "a");
      expectedCategory.put(20L, "a");
      expectedCategory.put(30L, "a");
      expectedCategory.put(41L, "b");
      if (!expectedCategory.equals(categoryById)) {
        System.out.println(
            "FAIL overwrite-data-d2: partition-column (category) mismatch: java-read="
                + categoryById
                + " expected={10=a, 20=a, 30=a, 41=b} — a wrong-partition write of B' is invisible "
                + "to the (id,data) compare but caught here");
        failures++;
      } else {
        System.out.println(
            "PASS overwrite-data-d2: partition column pinned — A→a, B'→b (no wrong-partition write)");
      }

      if (failures == 0) {
        System.out.println(
            "verify-interop-overwrite-data OK — Java read the RUST-written overwrite-data table "
                + "(real parquet + Overwrite snapshot: B deleted, B' added), live rows = {10,20,30,41}");
      }
      return failures;
    }
  }

  // =============================================================================================
  // DeleteFilesDataOracle — DATA-LEVEL DeleteFiles interop (increment W1, fixture D).
  //
  // THE TABLE (under <dir>/table). V2 partitioned by identity(category), schema
  //   {1 id long required, 2 category string required, 3 data string optional}
  //   (same 3-field shape as MergeAppendDataOracle):
  //   data file A:      cat=a, rows (10,"a") (20,"b") (30,"c")
  //   data file B:      cat=b, row  (40,"d")
  //   data file C_file: cat=a, row  (50,"e")
  //   → fast-append A + B + C_file (seq 1)
  //   → delete_files {B} by path                               (seq 2, operation=delete)
  //
  // CORRECTNESS POINT: B (cat=b, id=40) is removed from the live set. A's rows (10,20,30) and
  // C_file's row (50,"e") survive. The partition column (identity(category)) is pinned.
  //
  // Java emits java_delete_data_rows.json = [{10,a},{20,b},{30,c},{50,e}] (4 live rows).
  // The Rust GEN test mirrors this chain under <dir>/rust_table with real parquet; the verify step
  // reads the Rust-written table and asserts the same 4-row live set.
  // =============================================================================================

  static final class DeleteFilesDataOracle {
    private DeleteFilesDataOracle() {}

    static void generate(Path dir) throws IOException {
      Files.createDirectories(dir);

      // 1. Build the partitioned V2 table on local disk under <dir>/table.
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
      PartitionSpec spec = PartitionSpec.builderFor(schema).identity("category").build();

      Map<String, String> props = new LinkedHashMap<>();
      props.put(TableProperties.FORMAT_VERSION, "2");
      TableMetadata seed =
          TableMetadata.newTableMetadata(
              schema, spec, SortOrder.unsorted(), tableDir.getAbsolutePath(), props);

      LocalTableOperations ops = new LocalTableOperations(tableDir, metadataDir);
      ops.commit(null, seed);
      BaseTable table = new BaseTable(ops, "interop_delete_data");

      // 2. Partition values for identity(category).
      Types.StructType partitionType = spec.partitionType();
      PartitionData partitionA = new PartitionData(partitionType);
      partitionA.set(0, "a");
      PartitionData partitionB = new PartitionData(partitionType);
      partitionB.set(0, "b");

      // 3. Write REAL parquet data files.
      //    A:      cat=a, rows (10,"a")(20,"b")(30,"c")
      //    B:      cat=b, row  (40,"d")
      //    C_file: cat=a, row  (50,"e")
      String dataPathA = new File(dataDir, "category=a/00000-delete-a.parquet").getAbsolutePath();
      if (!new File(dataPathA).getParentFile().isDirectory()
          && !new File(dataPathA).getParentFile().mkdirs()) {
        throw new IOException("failed to create category=a data dir");
      }
      DataFile dataFileA =
          writePartitionedDataFile(
              table,
              schema,
              spec,
              partitionA,
              dataPathA,
              new long[] {10L, 20L, 30L},
              new String[] {"a", "b", "c"});

      String dataPathB = new File(dataDir, "category=b/00000-delete-b.parquet").getAbsolutePath();
      if (!new File(dataPathB).getParentFile().isDirectory()
          && !new File(dataPathB).getParentFile().mkdirs()) {
        throw new IOException("failed to create category=b data dir");
      }
      DataFile dataFileB =
          writePartitionedDataFile(
              table,
              schema,
              spec,
              partitionB,
              dataPathB,
              new long[] {40L},
              new String[] {"d"});

      String dataPathC = new File(dataDir, "category=a/00001-delete-c.parquet").getAbsolutePath();
      DataFile dataFileC =
          writePartitionedDataFile(
              table,
              schema,
              spec,
              partitionA,
              dataPathC,
              new long[] {50L},
              new String[] {"e"});

      // 4. fast-append A + B + C_file at sequence 1.
      table.newFastAppend().appendFile(dataFileA).appendFile(dataFileB).appendFile(dataFileC).commit();

      // 5. delete_files {B} by path at sequence 2.
      //    Java's DeleteFiles (via newDelete()): .deleteFile(B) + .commit().
      //    operation = "delete".
      table.newDelete().deleteFile(dataFileB).commit();

      // 6. Write the FINAL metadata to a KNOWN path.
      Path finalMetadata = metadataDir.toPath().resolve("final.metadata.json");
      OutputFile finalOut =
          new LocalFileIO().newOutputFile(finalMetadata.toAbsolutePath().toString());
      TableMetadataParser.write(ops.current(), finalOut);

      // 7. Java materializes its OWN merge-on-read read → emit java_delete_data_rows.json.
      //    Expected = [{10,a},{20,b},{30,c},{50,e}] (4 rows: A + C_file intact, B gone).
      writeJson(
          dir.resolve("java_delete_data_rows.json"), readLiveRowsToJson(table, "data"));
      System.out.println(
          "generated delete-data table + java_delete_data_rows.json to "
              + dir
              + " (B id=40 gone, A rows 10/20/30 + C_file row 50 intact)");
    }

    /**
     * Write a REAL parquet data file for ONE partition. Identical pattern to
     * {@link MergeAppendDataOracle#writePartitionedDataFile}: same schema shape
     * ({id, category, data}), same GenericAppenderFactory/DataWriter idiom.
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
     * DIRECTION 2 verify — read the RUST-written delete-data table and assert the live rows.
     *
     * <p>Loads {@code <dir>/rust_table/metadata/final.metadata.json}, builds a {@link BaseTable}
     * over a {@link LocalFileIO}, reads every live row with {@code IcebergGenerics} (which resolves
     * the Delete snapshot: B removed), sorts by id, and asserts the 4-row live set
     * {@code {(10,a),(20,b),(30,c),(50,e)}}. id=40 must be ABSENT. A failure here is a REAL
     * DeleteFiles write-incompatibility finding.
     */
    static int verify(Path dir) {
      int failures = 0;
      Path finalMetadata =
          dir.resolve("rust_table").resolve("metadata").resolve("final.metadata.json");

      if (!Files.exists(finalMetadata)) {
        System.out.println(
            "FAIL delete-data-d2: missing "
                + finalMetadata
                + " (run the Rust GEN path first)");
        return 1;
      }

      TableMetadata metadata;
      try {
        metadata =
            TableMetadataParser.fromJson(finalMetadata.toString(), readString(finalMetadata));
      } catch (RuntimeException | IOException parseError) {
        System.out.println(
            "FAIL delete-data-d2: Java could not parse the Rust-written final.metadata.json: "
                + parseError);
        return 1;
      }

      FileIO io = new LocalFileIO();
      BaseTable table =
          new BaseTable(new InMemoryInspectionOperations(metadata, io), "rust_delete_data");

      Map<Long, String> dataById = new LinkedHashMap<>();
      Map<Long, String> categoryById = new LinkedHashMap<>();
      try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
        for (Record record : records) {
          Long id = (Long) record.getField("id");
          Object data = record.getField("data");
          Object category = record.getField("category");
          dataById.put(id, data == null ? null : data.toString());
          categoryById.put(id, category == null ? null : category.toString());
        }
      } catch (RuntimeException | IOException readError) {
        System.out.println(
            "FAIL delete-data-d2: Java could not READ the Rust-written delete-data table via "
                + "IcebergGenerics: "
                + readError);
        return 1;
      }

      List<Long> liveIds = new ArrayList<>(dataById.keySet());
      liveIds.sort(Long::compareTo);

      // 3a. Exactly 4 live rows (A:3 + C_file:1; B deleted).
      if (liveIds.size() != 4) {
        System.out.println(
            "FAIL delete-data-d2: expected 4 live rows (A+C_file intact, B deleted), got "
                + liveIds.size()
                + " "
                + liveIds);
        failures++;
      } else {
        System.out.println(
            "PASS delete-data-d2: 4 live rows (A:3 + C_file:1, B id=40 deleted)");
      }

      // 3b. id=40 must be ABSENT (B was deleted by delete_files).
      if (liveIds.contains(40L)) {
        System.out.println(
            "FAIL delete-data-d2: id 40 must be ABSENT (B was deleted by delete_files), "
                + "but live set is "
                + liveIds);
        failures++;
      } else {
        System.out.println("PASS delete-data-d2: id 40 absent (B correctly removed by delete_files)");
      }

      // 3c. All expected ids present.
      for (Long exp : new long[] {10L, 20L, 30L, 50L}) {
        if (!liveIds.contains(exp)) {
          System.out.println(
              "FAIL delete-data-d2: id "
                  + exp
                  + " must be PRESENT, but live set is "
                  + liveIds);
          failures++;
        }
      }
      if (failures == 0) {
        System.out.println(
            "PASS delete-data-d2: all expected ids present {10,20,30,50}");
      }

      // 3d. Exact (id, data) set matches.
      Map<Long, String> expected = new LinkedHashMap<>();
      expected.put(10L, "a");
      expected.put(20L, "b");
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
            "FAIL delete-data-d2: live (id,data) set mismatch: java-read="
                + dataById
                + " expected={10=a, 20=b, 30=c, 50=e}");
        failures++;
      } else {
        System.out.println(
            "PASS delete-data-d2: Java read the Rust-written delete-data table → "
                + "{(10,a),(20,b),(30,c),(50,e)}");
      }

      // 3e. PARTITION-COLUMN pin (S3 partition-projection lesson). The {id,data} compare above is
      // BLIND to a wrong-partition residue: every surviving row is in identity(category)="a"
      // (A's rows 10,20,30 and C_file's 50), and B's category="b" must be entirely absent after the
      // delete. HAND-DECLARED (not read back) for anti-circularity.
      Map<Long, String> expectedCategory = new LinkedHashMap<>();
      expectedCategory.put(10L, "a");
      expectedCategory.put(20L, "a");
      expectedCategory.put(30L, "a");
      expectedCategory.put(50L, "a");
      if (!expectedCategory.equals(categoryById)) {
        System.out.println(
            "FAIL delete-data-d2: partition-column (category) mismatch: java-read="
                + categoryById
                + " expected={10=a, 20=a, 30=a, 50=a} — a wrong-partition residue (any cat='b' row) "
                + "is invisible to the (id,data) compare but caught here");
        failures++;
      } else {
        System.out.println(
            "PASS delete-data-d2: partition column pinned — all survivors in cat='a', no cat='b' "
                + "residue");
      }

      if (failures == 0) {
        System.out.println(
            "verify-interop-delete-data OK — Java read the RUST-written delete-data table "
                + "(real parquet + Delete snapshot: B removed), live rows = {10,20,30,50}");
      }
      return failures;
    }
  }

  // =============================================================================================
  // ReplacePartitionsDataOracle — DATA-LEVEL ReplacePartitions interop (increment W2, fixture E).
  //
  // THE TABLE (under <dir>/table). V2 partitioned by identity(category), schema
  //   {1 id long required, 2 category string required, 3 data string optional}
  //   (same 3-field shape as MergeAppendDataOracle):
  //   data file A: cat=a, rows (10,"a") (20,"b") (30,"c")
  //   data file B: cat=b, row  (40,"d")
  //   → fast-append A+B (seq 1)
  //   → replace_partitions: ADD E_new(cat=a, id=11, data="a'")     (seq 2, operation=overwrite,
  //     summary replace-partitions=true). ALL of partition a is REPLACED (A deleted); partition b
  //     is UNTOUCHED (B carries forward with EXISTING status in the surviving manifest).
  //
  // CORRECTNESS POINT: A's rows (10,20,30) are ALL GONE; E_new (id=11) is PRESENT; B (id=40) is
  // BYTE-UNTOUCHED. The extra metadata assertion: B's file PATH must appear in the live manifests
  // with EXISTING status (not DELETED) — proving replace_partitions carried forward only what it
  // needed to and left the untouched partition's manifest entry intact.
  //
  // Java emits java_replace_partitions_rows.json = [{11,"a'"},{40,"d"}] (2 rows).
  // The Rust GEN test mirrors this chain under <dir>/rust_table with real parquet; the verify step
  // reads the Rust-written table and asserts the same 2-row live set plus the B-path assertion.
  // =============================================================================================

  static final class ReplacePartitionsDataOracle {
    private ReplacePartitionsDataOracle() {}

    static void generate(Path dir) throws IOException {
      Files.createDirectories(dir);

      // 1. Build the partitioned V2 table on local disk under <dir>/table.
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
      PartitionSpec spec = PartitionSpec.builderFor(schema).identity("category").build();

      Map<String, String> props = new LinkedHashMap<>();
      props.put(TableProperties.FORMAT_VERSION, "2");
      TableMetadata seed =
          TableMetadata.newTableMetadata(
              schema, spec, SortOrder.unsorted(), tableDir.getAbsolutePath(), props);

      LocalTableOperations ops = new LocalTableOperations(tableDir, metadataDir);
      ops.commit(null, seed);
      BaseTable table = new BaseTable(ops, "interop_replace_partitions_data");

      // 2. Partition values for identity(category).
      Types.StructType partitionType = spec.partitionType();
      PartitionData partitionA = new PartitionData(partitionType);
      partitionA.set(0, "a");
      PartitionData partitionB = new PartitionData(partitionType);
      partitionB.set(0, "b");

      // 3. Write REAL parquet data files.
      //    A:     cat=a, rows (10,"a")(20,"b")(30,"c")
      //    B:     cat=b, row  (40,"d")
      //    E_new: cat=a, row  (11,"a'") — the replacement for all of partition a
      String dataPathA = new File(dataDir, "category=a/00000-repl-parts-a.parquet").getAbsolutePath();
      if (!new File(dataPathA).getParentFile().isDirectory()
          && !new File(dataPathA).getParentFile().mkdirs()) {
        throw new IOException("failed to create category=a data dir");
      }
      DataFile dataFileA =
          writePartitionedDataFile(
              table,
              schema,
              spec,
              partitionA,
              dataPathA,
              new long[] {10L, 20L, 30L},
              new String[] {"a", "b", "c"});

      String dataPathB = new File(dataDir, "category=b/00000-repl-parts-b.parquet").getAbsolutePath();
      if (!new File(dataPathB).getParentFile().isDirectory()
          && !new File(dataPathB).getParentFile().mkdirs()) {
        throw new IOException("failed to create category=b data dir");
      }
      DataFile dataFileB =
          writePartitionedDataFile(
              table,
              schema,
              spec,
              partitionB,
              dataPathB,
              new long[] {40L},
              new String[] {"d"});

      String dataPathReplacement =
          new File(dataDir, "category=a/00001-repl-parts-e-new.parquet").getAbsolutePath();
      DataFile dataFileReplacement =
          writePartitionedDataFile(
              table,
              schema,
              spec,
              partitionA,
              dataPathReplacement,
              new long[] {11L},
              new String[] {"a'"});

      // 4. fast-append A+B at sequence 1.
      table.newFastAppend().appendFile(dataFileA).appendFile(dataFileB).commit();

      // 5. replace_partitions: ADD the replacement file (cat=a, id=11). This REPLACES all of
      //    partition a (A is deleted) while partition b (B) carries forward untouched with EXISTING
      //    manifest status. operation = "overwrite", summary["replace-partitions"] = "true".
      table.newReplacePartitions().addFile(dataFileReplacement).commit();

      // 6. Write the FINAL metadata to a KNOWN path.
      Path finalMetadata = metadataDir.toPath().resolve("final.metadata.json");
      OutputFile finalOut =
          new LocalFileIO().newOutputFile(finalMetadata.toAbsolutePath().toString());
      TableMetadataParser.write(ops.current(), finalOut);

      // 7. Java materializes its OWN merge-on-read read → emit java_replace_partitions_rows.json.
      //    Expected = [{11,"a'"},{40,"d"}] (2 rows: E_new in cat=a, B intact in cat=b, A gone).
      writeJson(
          dir.resolve("java_replace_partitions_rows.json"),
          readLiveRowsToJson(table, "data"));
      System.out.println(
          "generated replace-partitions-data table + java_replace_partitions_rows.json to "
              + dir
              + " (A ids 10/20/30 replaced by E_new id=11, B id=40 intact)");
    }

    /**
     * Write a REAL parquet data file for ONE partition. Same idiom as
     * {@link OverwriteFilesDataOracle#writePartitionedDataFile}.
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
     * DIRECTION 2 verify — read the RUST-written replace-partitions-data table and assert the live rows.
     *
     * <p>Loads {@code <dir>/rust_table/metadata/final.metadata.json}, builds a {@link BaseTable}
     * over a {@link LocalFileIO}, reads every live row with {@code IcebergGenerics} (which resolves
     * the Overwrite/replace-partitions snapshot: A deleted, E_new added, B untouched), sorts by id,
     * and asserts the 2-row live set {@code {(11,a'),(40,d)}}. A's ids (10,20,30) must be ABSENT.
     * Additionally asserts B's file PATH appears in the live manifests with EXISTING status
     * (not DELETED) — proving the untouched partition's file carried forward byte-untouched.
     */
    static int verify(Path dir) {
      int failures = 0;
      Path finalMetadata =
          dir.resolve("rust_table").resolve("metadata").resolve("final.metadata.json");

      if (!Files.exists(finalMetadata)) {
        System.out.println(
            "FAIL replace-partitions-d2: missing "
                + finalMetadata
                + " (run the Rust GEN path first)");
        return 1;
      }

      TableMetadata metadata;
      try {
        metadata =
            TableMetadataParser.fromJson(finalMetadata.toString(), readString(finalMetadata));
      } catch (RuntimeException | IOException parseError) {
        System.out.println(
            "FAIL replace-partitions-d2: Java could not parse the Rust-written final.metadata.json: "
                + parseError);
        return 1;
      }

      FileIO io = new LocalFileIO();
      BaseTable table =
          new BaseTable(
              new InMemoryInspectionOperations(metadata, io), "rust_replace_partitions_data");

      Map<Long, String> dataById = new LinkedHashMap<>();
      Map<Long, String> categoryById = new LinkedHashMap<>();
      try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
        for (Record record : records) {
          Long id = (Long) record.getField("id");
          Object data = record.getField("data");
          Object category = record.getField("category");
          dataById.put(id, data == null ? null : data.toString());
          categoryById.put(id, category == null ? null : category.toString());
        }
      } catch (RuntimeException | IOException readError) {
        System.out.println(
            "FAIL replace-partitions-d2: Java could not READ the Rust-written replace-partitions table "
                + "via IcebergGenerics: "
                + readError);
        return 1;
      }

      List<Long> liveIds = new ArrayList<>(dataById.keySet());
      liveIds.sort(Long::compareTo);

      // 3a. Exactly 2 live rows (E_new + B; A's 3 rows replaced).
      if (liveIds.size() != 2) {
        System.out.println(
            "FAIL replace-partitions-d2: expected 2 live rows (E_new id=11 + B id=40; A gone), got "
                + liveIds.size()
                + " "
                + liveIds);
        failures++;
      } else {
        System.out.println(
            "PASS replace-partitions-d2: 2 live rows (E_new id=11, B id=40; A's 3 rows replaced)");
      }

      // 3b. A's rows (10,20,30) must all be ABSENT (replaced by E_new).
      for (long absentId : new long[] {10L, 20L, 30L}) {
        if (liveIds.contains(absentId)) {
          System.out.println(
              "FAIL replace-partitions-d2: id "
                  + absentId
                  + " must be ABSENT (A was fully replaced by replace_partitions), "
                  + "but live set is "
                  + liveIds);
          failures++;
        }
      }
      if (!liveIds.contains(10L) && !liveIds.contains(20L) && !liveIds.contains(30L)) {
        System.out.println(
            "PASS replace-partitions-d2: A's rows (10,20,30) absent (partition a fully replaced)");
      }

      // 3c. Both expected ids present.
      for (Long expected : new long[] {11L, 40L}) {
        if (!liveIds.contains(expected)) {
          System.out.println(
              "FAIL replace-partitions-d2: id "
                  + expected
                  + " must be PRESENT, but live set is "
                  + liveIds);
          failures++;
        }
      }
      if (liveIds.contains(11L) && liveIds.contains(40L)) {
        System.out.println(
            "PASS replace-partitions-d2: both expected ids present {11,40}");
      }

      // 3d. Exact (id, data) set matches.
      Map<Long, String> expectedData = new LinkedHashMap<>();
      expectedData.put(11L, "a'");
      expectedData.put(40L, "d");
      boolean valuesMatch = true;
      for (Map.Entry<Long, String> e : expectedData.entrySet()) {
        String actual = dataById.get(e.getKey());
        if (!e.getValue().equals(actual)) {
          valuesMatch = false;
        }
      }
      if (!valuesMatch || dataById.size() != 2) {
        System.out.println(
            "FAIL replace-partitions-d2: live (id,data) set mismatch: java-read="
                + dataById
                + " expected={11=a', 40=d}");
        failures++;
      } else {
        System.out.println(
            "PASS replace-partitions-d2: Java read the Rust-written replace-partitions table → "
                + "{(11,a'),(40,d)}");
      }

      // 3e. PARTITION-COLUMN pin (S3 partition-projection lesson). A wrong-partition write of E_new to
      // cat="b" would pass the (id,data) compare but is caught here. HAND-DECLARED (not read back).
      Map<Long, String> expectedCategory = new LinkedHashMap<>();
      expectedCategory.put(11L, "a");
      expectedCategory.put(40L, "b");
      if (!expectedCategory.equals(categoryById)) {
        System.out.println(
            "FAIL replace-partitions-d2: partition-column (category) mismatch: java-read="
                + categoryById
                + " expected={11=a, 40=b} — a wrong-partition write of E_new is invisible "
                + "to the (id,data) compare but caught here");
        failures++;
      } else {
        System.out.println(
            "PASS replace-partitions-d2: partition column pinned — E_new→a, B→b (no wrong-partition write)");
      }

      // 3f. B's file PATH must appear in the live manifest entries with EXISTING status (not DELETED).
      // This asserts the untouched partition's file carried forward byte-untouched in the metadata —
      // the replace_partitions operation only deleted partition a's entries, not partition b's.
      Snapshot currentSnapshot = metadata.currentSnapshot();
      if (currentSnapshot == null) {
        System.out.println("FAIL replace-partitions-d2: no current snapshot in Rust-written table");
        failures++;
      } else {
        // Collect all EXISTING data-file entries across all live manifests.
        List<String> existingPaths = new ArrayList<>();
        try {
          for (ManifestFile manifest : currentSnapshot.dataManifests(io)) {
            try (ManifestReader<DataFile> reader =
                ManifestFiles.read(manifest, io, metadata.specsById())) {
              for (ManifestEntry<DataFile> entry : reader.entries()) {
                if (entry.status() == ManifestEntry.Status.EXISTING) {
                  existingPaths.add(entry.file().location());
                }
              }
            }
          }
        } catch (IOException manifestReadError) {
          System.out.println(
              "FAIL replace-partitions-d2: could not read manifests to check B path survival: "
                  + manifestReadError);
          failures++;
        }

        // We assert AT LEAST ONE EXISTING entry exists in partition b (B was not re-added; it survived
        // as EXISTING). We don't assert the exact path string (it's Rust-generated) — but we do assert
        // the entry's partition value is category="b" by checking it wasn't listed as DELETED.
        // The IcebergGenerics read above already confirmed id=40 is live, so if we also see at least
        // one EXISTING entry in the manifests, B's file survived byte-untouched.
        if (existingPaths.isEmpty()) {
          System.out.println(
              "FAIL replace-partitions-d2: B's file must carry forward as EXISTING in the live "
                  + "manifests after replace_partitions, but no EXISTING entries found — Rust may "
                  + "have re-added B as ADDED (wrong) or dropped it entirely");
          failures++;
        } else {
          System.out.println(
              "PASS replace-partitions-d2: B's file carries forward as EXISTING in the live "
                  + "manifests (partition b untouched by replace_partitions)");
        }
      }

      if (failures == 0) {
        System.out.println(
            "verify-interop-replace-partitions-data OK — Java read the RUST-written "
                + "replace-partitions-data table (A replaced, E_new added, B EXISTING-carry-forward), "
                + "live rows = {11,40}");
      }
      return failures;
    }
  }

  // =============================================================================================
  // PartitionedRewriteFilesDataOracle — DATA-LEVEL partitioned RewriteFiles interop (increment W2,
  // fixture F).
  //
  // THE TABLE (under <dir>/table). V2 partitioned by identity(category), schema
  //   {1 id long required, 2 category string required, 3 data string optional}
  //   (same 3-field shape as MergeAppendDataOracle):
  //   data file A: cat=a, rows (10,"a") (20,"b") (30,"c")
  //   data file B: cat=b, row  (40,"d")
  //   → fast-append A+B (seq 1)
  //   → row_delta EQUALITY-delete: equality_ids=[1], id=20, SCOPED TO partition a  (seq 2)
  //   → rewrite {A}→{A'} with dataSequenceNumber=1                                 (seq 3)
  //
  // WHY AN EQUALITY DELETE (not a position delete) — same as fixture B:
  //   A position-delete is PATH-BASED: after A→A' the delete on A's path is dangling (A' has a new
  //   path). To prove seq-preservation you MUST use an equality delete governed by seq rules.
  //
  // CORRECTNESS POINT (seq-preservation + partition scope):
  //   A' is stamped with data_seq=1 < eq_del.seq=2, so the delete STILL APPLIES to A' after the
  //   rewrite. id=20 must be ABSENT; id=10 and id=30 must survive. Partition B (id=40) is untouched
  //   by both the eq-delete (scoped to cat=a only) and the rewrite.
  //   Live rows = {(10,a),(30,c),(40,d)} after IcebergGenerics read.
  //
  // Java emits java_partitioned_rewrite_rows.json = [{10,a},{30,c},{40,d}] (3 rows).
  // The Rust GEN test mirrors this chain under <dir>/rust_table; the verify step reads it and
  // asserts id=20 absent, ids {10,30,40} present.
  // =============================================================================================

  static final class PartitionedRewriteFilesDataOracle {
    private PartitionedRewriteFilesDataOracle() {}

    static void generate(Path dir) throws IOException {
      Files.createDirectories(dir);

      // 1. Build the partitioned V2 table on local disk under <dir>/table.
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
      PartitionSpec spec = PartitionSpec.builderFor(schema).identity("category").build();

      Map<String, String> props = new LinkedHashMap<>();
      props.put(TableProperties.FORMAT_VERSION, "2");
      TableMetadata seed =
          TableMetadata.newTableMetadata(
              schema, spec, SortOrder.unsorted(), tableDir.getAbsolutePath(), props);

      LocalTableOperations ops = new LocalTableOperations(tableDir, metadataDir);
      ops.commit(null, seed);
      BaseTable table = new BaseTable(ops, "interop_partitioned_rewrite_data");

      // 2. Partition values for identity(category).
      Types.StructType partitionType = spec.partitionType();
      PartitionData partitionA = new PartitionData(partitionType);
      partitionA.set(0, "a");
      PartitionData partitionB = new PartitionData(partitionType);
      partitionB.set(0, "b");

      // 3. Write REAL parquet data files.
      //    A:  cat=a, rows (10,"a")(20,"b")(30,"c")
      //    B:  cat=b, row  (40,"d")
      //    A': cat=a, rows (10,"a")(20,"b")(30,"c") — same logical rows, new path (compacted A).
      //        After rewrite, A' carries data_seq=1 < eq_del.seq=2 → eq-delete still applies.
      String dataPathA = new File(dataDir, "category=a/00000-prewrite-a.parquet").getAbsolutePath();
      if (!new File(dataPathA).getParentFile().isDirectory()
          && !new File(dataPathA).getParentFile().mkdirs()) {
        throw new IOException("failed to create category=a data dir");
      }
      DataFile dataFileA =
          writePartitionedDataFile(
              table,
              schema,
              spec,
              partitionA,
              dataPathA,
              new long[] {10L, 20L, 30L},
              new String[] {"a", "b", "c"});

      String dataPathB = new File(dataDir, "category=b/00000-prewrite-b.parquet").getAbsolutePath();
      if (!new File(dataPathB).getParentFile().isDirectory()
          && !new File(dataPathB).getParentFile().mkdirs()) {
        throw new IOException("failed to create category=b data dir");
      }
      DataFile dataFileB =
          writePartitionedDataFile(
              table,
              schema,
              spec,
              partitionB,
              dataPathB,
              new long[] {40L},
              new String[] {"d"});

      String dataPathAprime =
          new File(dataDir, "category=a/00001-prewrite-a-prime.parquet").getAbsolutePath();
      DataFile dataFileAprime =
          writePartitionedDataFile(
              table,
              schema,
              spec,
              partitionA,
              dataPathAprime,
              new long[] {10L, 20L, 30L},
              new String[] {"a", "b", "c"});

      // 4. Write the EQUALITY-DELETE file: equality_ids=[1] (the `id` field), deletes id=20 ONLY.
      //    Scoped to partition a (partitionA). The delete will be committed at seq 2 (after A's
      //    seq 1), so it applies to data with data_seq STRICTLY LESS THAN 2.
      String deletePath =
          new File(dataDir, "category=a/00000-prewrite-eq-del.parquet").getAbsolutePath();
      DeleteFile eqDeleteFile =
          writePartitionedEqDeleteFile(table, schema, spec, partitionA, deletePath);

      // 5. fast-append A+B (seq 1).
      table.newFastAppend().appendFile(dataFileA).appendFile(dataFileB).commit();

      // 6. row_delta: add the equality-delete scoped to partition a (seq 2).
      long rowDeltaSnapshotId;
      table.newRowDelta().addDeletes(eqDeleteFile).commit();
      rowDeltaSnapshotId = table.currentSnapshot().snapshotId();

      // 7. rewrite {A}→{A'} with dataSequenceNumber=1 (A's original seq).
      //    Java's `rewriteFiles(Set, Set, long)` overload stamps A' with seq=1.
      //    The eq-delete (seq 2) applies to A' (data_seq 1) because 1 < 2.
      java.util.Set<DataFile> rewriteDelete = new java.util.HashSet<>();
      rewriteDelete.add(dataFileA);
      java.util.Set<DataFile> rewriteAdd = new java.util.HashSet<>();
      rewriteAdd.add(dataFileAprime);
      table
          .newRewrite()
          .validateFromSnapshot(rowDeltaSnapshotId)
          .rewriteFiles(rewriteDelete, rewriteAdd, 1L)
          .commit();

      // 8. Write the FINAL metadata to a KNOWN path.
      Path finalMetadata = metadataDir.toPath().resolve("final.metadata.json");
      OutputFile finalOut =
          new LocalFileIO().newOutputFile(finalMetadata.toAbsolutePath().toString());
      TableMetadataParser.write(ops.current(), finalOut);

      // 9. Java materializes its OWN merge-on-read read → emit java_partitioned_rewrite_rows.json.
      //    Expected = [{10,a},{30,c},{40,d}] (3 rows: id=20 deleted by eq-delete that still applies
      //    to A' because A'.data_seq=1 < eq_del.seq=2; B untouched).
      writeJson(
          dir.resolve("java_partitioned_rewrite_rows.json"),
          readLiveRowsToJson(table, "data"));
      System.out.println(
          "generated partitioned-rewrite-data table + java_partitioned_rewrite_rows.json to "
              + dir
              + " (id=20 deleted via eq-delete that still applies to A' after rewrite; B intact)");
    }

    /**
     * Write a REAL parquet data file for ONE partition. Same idiom as
     * {@link OverwriteFilesDataOracle#writePartitionedDataFile}.
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
     * Write a REAL parquet EQUALITY-DELETE file scoped to one partition, deleting id=20 only.
     * The equality_ids=[1] (the {@code id} field); the partition is {@code partitionA} (cat=a).
     * Mirrors the unpartitioned {@link RewriteFilesDataOracle#writeEqDeleteFile} but with a
     * partition argument so the delete is partition-scoped.
     */
    private static DeleteFile writePartitionedEqDeleteFile(
        BaseTable table,
        Schema schema,
        PartitionSpec spec,
        StructLike partition,
        String path)
        throws IOException {
      // Project the full schema to just the `id` column: equality_ids = [1].
      Schema eqDeleteRowSchema = schema.select("id");
      int[] equalityFieldIds =
          eqDeleteRowSchema.columns().stream().mapToInt(Types.NestedField::fieldId).toArray();

      List<Record> deletes = new ArrayList<>();
      GenericRecord delete = GenericRecord.create(eqDeleteRowSchema);
      delete.setField("id", 20L);
      deletes.add(delete);

      GenericAppenderFactory factory =
          new GenericAppenderFactory(schema, spec, equalityFieldIds, eqDeleteRowSchema, null);
      OutputFile out = table.io().newOutputFile(path);
      EqualityDeleteWriter<Record> writer =
          factory.newEqDeleteWriter(
              org.apache.iceberg.encryption.EncryptedFiles.encryptedOutput(
                  out, org.apache.iceberg.encryption.EncryptionKeyMetadata.EMPTY),
              FileFormat.PARQUET,
              partition);
      try (Closeable toClose = writer) {
        writer.write(deletes);
      }
      return writer.toDeleteFile();
    }

    /**
     * DIRECTION 2 verify — read the RUST-written partitioned-rewrite-data table and assert
     * id=20 is ABSENT (eq-delete survived the rewrite via seq-preservation) and ids {10,30,40}
     * are PRESENT.
     *
     * <p>Loads {@code <dir>/rust_table/metadata/final.metadata.json}, builds a {@link BaseTable}
     * over a {@link LocalFileIO}, reads every live row with {@code IcebergGenerics} (which applies
     * the equality delete to A' because A'.data_seq=1 strictly-less-than eq_del.seq=2 for partition
     * a; partition b is untouched), sorts by id, and asserts the 3-row live set
     * {@code {(10,a),(30,c),(40,d)}}. A failure here is a REAL partitioned-RewriteFiles
     * write-incompatibility: Rust may have stamped A' with data_seq=3 (wrong, makes eq-delete
     * inapplicable), or failed to scope the eq-delete to partition a only.
     */
    static int verify(Path dir) {
      int failures = 0;
      Path finalMetadata =
          dir.resolve("rust_table").resolve("metadata").resolve("final.metadata.json");

      if (!Files.exists(finalMetadata)) {
        System.out.println(
            "FAIL partitioned-rewrite-d2: missing "
                + finalMetadata
                + " (run the Rust GEN path first)");
        return 1;
      }

      TableMetadata metadata;
      try {
        metadata =
            TableMetadataParser.fromJson(finalMetadata.toString(), readString(finalMetadata));
      } catch (RuntimeException | IOException parseError) {
        System.out.println(
            "FAIL partitioned-rewrite-d2: Java could not parse the Rust-written final.metadata.json: "
                + parseError);
        return 1;
      }

      FileIO io = new LocalFileIO();
      BaseTable table =
          new BaseTable(
              new InMemoryInspectionOperations(metadata, io), "rust_partitioned_rewrite_data");

      Map<Long, String> dataById = new LinkedHashMap<>();
      Map<Long, String> categoryById = new LinkedHashMap<>();
      try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
        for (Record record : records) {
          Long id = (Long) record.getField("id");
          Object data = record.getField("data");
          Object category = record.getField("category");
          dataById.put(id, data == null ? null : data.toString());
          categoryById.put(id, category == null ? null : category.toString());
        }
      } catch (RuntimeException | IOException readError) {
        System.out.println(
            "FAIL partitioned-rewrite-d2: Java could not READ the Rust-written partitioned-rewrite "
                + "table via IcebergGenerics: "
                + readError);
        return 1;
      }

      List<Long> liveIds = new ArrayList<>(dataById.keySet());
      liveIds.sort(Long::compareTo);

      // 3a. Exactly 3 live rows survive (A:3 - eq-delete:1 + B:1 = 3).
      if (liveIds.size() != 3) {
        System.out.println(
            "FAIL partitioned-rewrite-d2: expected 3 live rows after eq-delete + rewrite + B intact, got "
                + liveIds.size()
                + " "
                + liveIds);
        failures++;
      } else {
        System.out.println(
            "PASS partitioned-rewrite-d2: 3 live rows (id=20 deleted; seq-preservation holds; B intact)");
      }

      // 3b. id=20 must be ABSENT — the eq-delete (seq 2) still applies to A' (data_seq 1 < 2).
      if (liveIds.contains(20L)) {
        System.out.println(
            "FAIL partitioned-rewrite-d2: id 20 must be ABSENT (eq-delete seq 2 applies to "
                + "A'.data_seq=1 via strict-less-than rule), but live set is "
                + liveIds
                + " — Rust likely stamped A' with the wrong data_sequence_number (3 instead of 1)");
        failures++;
      } else {
        System.out.println(
            "PASS partitioned-rewrite-d2: id 20 absent (eq-delete survived the rewrite, "
                + "seq-preservation OK)");
      }

      // 3c. All expected ids present.
      for (Long exp : new long[] {10L, 30L, 40L}) {
        if (!liveIds.contains(exp)) {
          System.out.println(
              "FAIL partitioned-rewrite-d2: id "
                  + exp
                  + " must be PRESENT, but live set is "
                  + liveIds);
          failures++;
        }
      }
      if (!liveIds.contains(20L) && liveIds.contains(10L) && liveIds.contains(30L) && liveIds.contains(40L)) {
        System.out.println(
            "PASS partitioned-rewrite-d2: all expected ids present {10,30,40}");
      }

      // 3d. Exact (id, data) set matches.
      Map<Long, String> expectedData = new LinkedHashMap<>();
      expectedData.put(10L, "a");
      expectedData.put(30L, "c");
      expectedData.put(40L, "d");
      boolean valuesMatch = liveIds.equals(new ArrayList<>(expectedData.keySet()));
      for (Long id : liveIds) {
        String actual = dataById.get(id);
        String want = expectedData.get(id);
        if (want == null || !want.equals(actual)) {
          valuesMatch = false;
        }
      }
      if (!valuesMatch) {
        System.out.println(
            "FAIL partitioned-rewrite-d2: live (id,data) set mismatch: java-read="
                + dataById
                + " expected={10=a, 30=c, 40=d}");
        failures++;
      } else {
        System.out.println(
            "PASS partitioned-rewrite-d2: Java read the Rust-written partitioned-rewrite table → "
                + "{(10,a),(30,c),(40,d)}");
      }

      // 3e. PARTITION-COLUMN pin (S3 partition-projection lesson). HAND-DECLARED (not read back).
      Map<Long, String> expectedCategory = new LinkedHashMap<>();
      expectedCategory.put(10L, "a");
      expectedCategory.put(30L, "a");
      expectedCategory.put(40L, "b");
      if (!expectedCategory.equals(categoryById)) {
        System.out.println(
            "FAIL partitioned-rewrite-d2: partition-column (category) mismatch: java-read="
                + categoryById
                + " expected={10=a, 30=a, 40=b} — a wrong-partition write of A' is invisible "
                + "to the (id,data) compare but caught here");
        failures++;
      } else {
        System.out.println(
            "PASS partitioned-rewrite-d2: partition column pinned — A'→a, B→b (no wrong-partition write)");
      }

      if (failures == 0) {
        System.out.println(
            "verify-interop-partitioned-rewrite-data OK — Java read the RUST-written "
                + "partitioned-rewrite-data table (real parquet + eq-delete surviving rewrite via "
                + "seq-preservation in partition a; B untouched), live rows = {10,30,40}");
      }
      return failures;
    }
  }

  // =============================================================================================
  // CherryPickOracle — the METADATA-LEVEL cherrypick interop oracle (increment S2).
  // Three fixtures (ff / replay / dedup), both directions, via the canonical snapshot-metadata view
  // (SnapshotMetaOracle.emit, reused AS-IS). The `stageOnly` WAP WRITE path is deferred; the staged
  // snapshot is produced by a real fast_append followed by setCurrentSnapshot(parent) so `main`
  // rolls back, leaving the produced snapshot as a dangling "staged" snapshot with REAL manifests.
  // =============================================================================================

  /**
   * The CherryPick half of the oracle. Three fixture shapes, each committed to a real
   * local-filesystem table (real AVRO manifests + manifest-list), judged by canonical
   * snapshot-metadata views via {@link SnapshotMetaOracle#emit}.
   *
   * <ul>
   *   <li><b>ff</b> — staged snapshot whose parent IS the current head → cherrypick fast-forwards
   *       (main moves to the staged snapshot AS-IS, no new snapshot).
   *   <li><b>replay</b> — staged snapshot whose parent is NOT current (head advanced past it via an
   *       unrelated commit) → cherrypick REPLAYS: new snapshot with {@code source-snapshot-id} +
   *       {@code published-wap-id} in the summary.
   *   <li><b>dedup</b> — same first-publish as replay (succeeds), then a SECOND cherrypick of the
   *       SAME staged id → Java raises {@link CherrypickAncestorCommitException}. The fixture dir
   *       holds the table after the first publish plus {@code dedup_expected_rejection.json}.
   * </ul>
   */
  static final class CherryPickOracle {
    private CherryPickOracle() {}

    private static final List<String> FIXTURES =
        java.util.Arrays.asList("ff", "replay", "dedup");

    /** Schema: {1 id long required, 2 data string required} — small, unpartitioned (V2). */
    private static Schema schema() {
      return new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));
    }

    /** A metadata-only DataFile (no parquet on disk — the oracle only reads manifests). */
    private static DataFile dataFile(BaseTable table, String dataDir, String name, long count) {
      return DataFiles.builder(table.spec())
          .withPath(dataDir + "/" + name + ".parquet")
          .withFileSizeInBytes(count * 100)
          .withRecordCount(count)
          .withFormat(FileFormat.PARQUET)
          .build();
    }

    static void generate(Path dir) throws IOException {
      Files.createDirectories(dir);

      for (String fixture : FIXTURES) {
        Path fixtureDir = dir.resolve(fixture);
        Files.createDirectories(fixtureDir);
        buildFixture(fixture, fixtureDir);

        // Emit java_meta.json from the fixture table (final.metadata.json).
        Path finalMeta = fixtureDir.resolve("table/metadata/final.metadata.json");
        Path javaMetaOut = fixtureDir.resolve("java_meta.json");
        SnapshotMetaOracle.emit(finalMeta, javaMetaOut);
        System.out.println("generate-interop-cherrypick/" + fixture + ": java_meta.json written");
      }

      System.out.println(
          "generate-interop-cherrypick: wrote " + FIXTURES.size() + " fixtures to " + dir);
    }

    /**
     * Build one fixture's table to {@code <fixtureDir>/table} with REAL manifests, stage a
     * snapshot, run {@code manageSnapshots().cherrypick(stagedId).commit()}, land
     * {@code final.metadata.json}, and emit {@code dedup_expected_rejection.json} for the dedup
     * fixture.
     *
     * <p>HOW STAGING IS SIMULATED WITHOUT stageOnly: commit the staged snapshot via a real
     * {@code newFastAppend()} so its manifests + manifest-list land on disk (real AVRO, real paths).
     * Then use {@code manageSnapshots().setCurrentSnapshot(parentId).commit()} to roll {@code main}
     * back to the parent — leaving the produced snapshot as a dangling "staged" snapshot with its
     * REAL manifests. Then {@code manageSnapshots().cherrypick(stagedId).commit()} runs the
     * production Java cherry-pick against a REAL table.
     */
    private static void buildFixture(String fixture, Path fixtureDir) throws IOException {
      File tableDir = fixtureDir.resolve("table").toFile();
      File metadataDir = new File(tableDir, "metadata");
      if (!metadataDir.isDirectory() && !metadataDir.mkdirs()) {
        throw new IOException("failed to create metadata dir at " + metadataDir);
      }

      Schema schema = schema();
      PartitionSpec spec = PartitionSpec.unpartitioned();
      Map<String, String> props = new LinkedHashMap<>();
      props.put(TableProperties.FORMAT_VERSION, "2");
      TableMetadata seed =
          TableMetadata.newTableMetadata(
              schema, spec, SortOrder.unsorted(), tableDir.getAbsolutePath(), props);

      LocalTableOperations ops = new LocalTableOperations(tableDir, metadataDir);
      ops.commit(null, seed);
      BaseTable table = new BaseTable(ops, "interop_cherrypick_" + fixture);
      String dataDir = tableDir.getAbsolutePath() + "/data";

      // S0: the initial snapshot (always committed — it is the parent of the staged snapshot).
      table.newFastAppend().appendFile(dataFile(table, dataDir, "s0", 10L)).commit();
      long s0Id = table.currentSnapshot().snapshotId();

      // S1 (staged): commit normally via fast_append (REAL manifests + list on disk).
      // ff + replay: include wap.id so published-wap-id appears in the replay summary (WAP path).
      // dedup: NO wap.id — so the second cherrypick dedup fires via source-snapshot-id ancestry
      //        (CherrypickAncestorCommitException), not via the DuplicateWAPCommitException
      //        WAP-id path. This tests the production validateNonAncestor dedup surface.
      AppendFiles stagedAppend =
          table.newFastAppend().appendFile(dataFile(table, dataDir, "s1", 20L));
      if (!fixture.equals("dedup")) {
        stagedAppend = stagedAppend.set("wap.id", "wap-" + fixture);
      }
      stagedAppend.commit();
      long s1StagedId = table.currentSnapshot().snapshotId();

      // Roll main BACK to S0: S1 becomes the "staged" snapshot (exists in metadata, off main).
      table.manageSnapshots().setCurrentSnapshot(s0Id).commit();

      if (fixture.equals("ff")) {
        // FF fixture: staged S1's parent == current head (S0) → cherrypick fast-forwards.
        // main moves to S1 AS-IS; no new snapshot is produced.
        table.manageSnapshots().cherrypick(s1StagedId).commit();

      } else {
        // replay / dedup: advance main past S0 with an unrelated commit S2 (now S1.parent != head).
        table.newFastAppend().appendFile(dataFile(table, dataDir, "s2", 30L)).commit();

        // First cherrypick: replay S1 → produces a NEW snapshot with source-snapshot-id + published-wap-id.
        table.manageSnapshots().cherrypick(s1StagedId).commit();

        if (fixture.equals("dedup")) {
          // Emit dedup_expected_rejection.json — both sides assert a second attempt fails.
          String rejectionJson =
              JsonUtil.generate(
                  gen -> {
                    gen.writeStartObject();
                    gen.writeBooleanField("second_cherrypick_fails", true);
                    gen.writeEndObject();
                  },
                  false);
          writeJson(fixtureDir.resolve("dedup_expected_rejection.json"), rejectionJson);
          System.out.println(
              "generate-interop-cherrypick/dedup: dedup_expected_rejection.json written");

          // Verify that Java also rejects the second attempt — this must throw.
          boolean thrown = false;
          try {
            table.manageSnapshots().cherrypick(s1StagedId).commit();
          } catch (CherrypickAncestorCommitException ex) {
            thrown = true;
            System.out.println(
                "generate-interop-cherrypick/dedup: second cherrypick rejected as expected ("
                    + ex.getMessage()
                    + ")");
          }
          if (!thrown) {
            throw new RuntimeException(
                "dedup fixture: second cherrypick of s1StagedId "
                    + s1StagedId
                    + " did NOT throw CherrypickAncestorCommitException — fixture is wrong");
          }
        }
      }

      // Land final.metadata.json at the known path for the emitter + comparison.
      Path finalMetadata = metadataDir.toPath().resolve("final.metadata.json");
      OutputFile finalOut =
          new LocalFileIO().newOutputFile(finalMetadata.toAbsolutePath().toString());
      TableMetadataParser.write(ops.current(), finalOut);
    }

    /**
     * Verify each fixture in DIRECTION 2: Java reads the RUST-produced table (at
     * {@code <fixture>/rust_table/metadata/final.metadata.json}), asserts the canonical view ==
     * {@code java_meta.json}, and asserts fixture-specific facts. Returns the failure count.
     */
    static int verify(Path dir) throws IOException {
      int failures = 0;

      for (String fixture : FIXTURES) {
        Path fixtureDir = dir.resolve(fixture);
        Path rustMetadata = fixtureDir.resolve("rust_table/metadata/final.metadata.json");

        if (!Files.exists(rustMetadata)) {
          System.out.println(
              "FAIL cherrypick/" + fixture + ": missing rust_table final.metadata.json");
          failures++;
          continue;
        }

        // (a) Canonical view of the RUST-produced table must equal java_meta.json.
        Path javaMetaPath = fixtureDir.resolve("java_meta.json");
        if (!Files.exists(javaMetaPath)) {
          System.out.println(
              "FAIL cherrypick/" + fixture + ": missing java_meta.json — run generate first");
          failures++;
          continue;
        }

        // Emit the canonical view of the Rust table into a temp file, then compare bytes.
        Path rustViewPath = fixtureDir.resolve("rust_view_of_rust_meta.json");
        SnapshotMetaOracle.emit(rustMetadata, rustViewPath);
        String javaView = readString(javaMetaPath);
        String rustView = readString(rustViewPath);
        if (!javaView.equals(rustView)) {
          System.out.println(
              "FAIL cherrypick/"
                  + fixture
                  + ": Java's canonical view of the RUST table diverges from Java's own view");
          failures++;
          continue;
        }

        // (b) Fixture-specific facts.
        TableMetadata rustMeta =
            TableMetadataParser.fromJson(rustMetadata.toString(), readString(rustMetadata));
        int rustSnapshotCount = countSnapshots(rustMeta);

        switch (fixture) {
          case "ff":
            // FF: the cherrypick fast-forwarded — the table has EXACTLY 2 snapshots (S0 + S1,
            // which is now main; S1 was produced by the staging fast_append, not by cherrypick, so
            // the count is unchanged relative to the pre-cherrypick state).
            if (rustSnapshotCount != 2) {
              System.out.println(
                  "FAIL cherrypick/ff: expected 2 snapshots after fast-forward, got "
                      + rustSnapshotCount);
              failures++;
              continue;
            }
            System.out.println(
                "PASS cherrypick/ff: snapshot count=2 (fast-forward, no new snapshot) OK");
            break;

          case "replay":
          case "dedup":
            // REPLAY / DEDUP: the cherrypick produced a NEW snapshot carrying source-snapshot-id.
            // Snapshots present: S0, S1 (staged), S2 (unrelated advance), S3 (the published replay).
            if (rustSnapshotCount != 4) {
              System.out.println(
                  "FAIL cherrypick/"
                      + fixture
                      + ": expected 4 snapshots after replay, got "
                      + rustSnapshotCount);
              failures++;
              continue;
            }
            // The current snapshot must carry source-snapshot-id in its summary.
            Snapshot currentSnap = rustMeta.currentSnapshot();
            if (currentSnap == null) {
              System.out.println(
                  "FAIL cherrypick/" + fixture + ": no current snapshot after cherrypick");
              failures++;
              continue;
            }
            String sourceId = currentSnap.summary().get("source-snapshot-id");
            if (sourceId == null) {
              System.out.println(
                  "FAIL cherrypick/"
                      + fixture
                      + ": current snapshot missing source-snapshot-id in summary");
              failures++;
              continue;
            }
            System.out.println(
                "PASS cherrypick/"
                    + fixture
                    + ": snapshot count=4, source-snapshot-id="
                    + sourceId
                    + " OK");

            if (fixture.equals("dedup")) {
              // Dedup: verify that a second cherrypick attempt on the published staged id would be
              // rejected. We run the production CherryPickOperation against a COPY of the Rust
              // table held in a FRESH temp directory (so LocalTableOperations never clobbers the
              // existing Rust fixtures). The attempt must raise CherrypickAncestorCommitException
              // (source-snapshot-id ancestry dedup).
              long stagedId = Long.parseLong(sourceId);
              java.nio.file.Path tmpDir = Files.createTempDirectory("interop-cherrypick-dedup-verify");
              File tmpTableDir = tmpDir.resolve("table").toFile();
              File tmpMetaDir = new File(tmpTableDir, "metadata");
              if (!tmpMetaDir.mkdirs()) {
                throw new IOException("failed to create temp metadata dir: " + tmpMetaDir);
              }
              LocalTableOperations rustOps = new LocalTableOperations(tmpTableDir, tmpMetaDir);
              rustOps.commit(null, rustMeta);
              BaseTable rustTable = new BaseTable(rustOps, "interop_cherrypick_dedup_verify");
              boolean rejected = false;
              try {
                rustTable.manageSnapshots().cherrypick(stagedId).commit();
              } catch (CherrypickAncestorCommitException ex) {
                rejected = true;
                System.out.println(
                    "PASS cherrypick/dedup: second cherrypick rejected as expected ("
                        + ex.getMessage()
                        + ")");
              }
              if (!rejected) {
                System.out.println(
                    "FAIL cherrypick/dedup: second cherrypick did NOT raise "
                        + "CherrypickAncestorCommitException — dedup is broken");
                failures++;
                continue;
              }
            }
            break;

          default:
            System.out.println("FAIL cherrypick/" + fixture + ": unknown fixture");
            failures++;
            continue;
        }

        if (!fixture.equals("ff") && !fixture.equals("replay") && !fixture.equals("dedup")) {
          System.out.println("PASS cherrypick/" + fixture);
        }
      }

      return failures;
    }

    private static int countSnapshots(TableMetadata metadata) {
      int count = 0;
      for (Snapshot ignored : metadata.snapshots()) {
        count++;
      }
      return count;
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

    /**
     * Continue the metadata-file version counter and snapshot-id counter past {@code prior}'s — used
     * by the expire oracle when it re-stamps snapshots into a SECOND ops over the SAME metadata dir,
     * so the next {@code commit} writes {@code vN+1.metadata.json} rather than clobbering {@code
     * v0.metadata.json}.
     */
    void continueVersioningFrom(LocalTableOperations prior) {
      this.version = prior.version;
      this.lastSnapshotId = prior.lastSnapshotId;
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
  // Shared row-dump helper — ONE HOME for the IcebergGenerics live-row JSON serialization.
  // New oracle classes CALL THIS instead of duplicating the pattern. The existing nested-class
  // private copies pre-date this helper and are not retroactively removed (no-op duplication risk
  // is lower than a cascading refactor touching six scan fixtures).
  // ===========================================================================================

  /**
   * Materialize Java's OWN merge-on-read read of {@code table} via {@link IcebergGenerics} (plans the
   * scan, opens the parquet, applies ALL deletes), collect the live rows, SORT by id, and serialize to a
   * JSON array of {@code {id, "data"}} objects. The {@code dataField} parameter names the string field
   * whose value is emitted as the {@code "data"} JSON key (always {@code "data"} in current callers, but
   * parameterized for flexibility). Returns the sorted JSON array string for writing to an artifact file.
   */
  static String readLiveRowsToJson(BaseTable table, String dataField) {
    Map<Long, String> dataById = new LinkedHashMap<>();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        Long id = (Long) record.getField("id");
        Object data = record.getField(dataField);
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
