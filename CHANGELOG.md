<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## [v0.3.0] - 2024-08-14

* Smooth out release steps by @Fokko in https://github.com/apache/iceberg-rust/pull/197
* refactor: remove support of manifest list format as a list of file path by @Dysprosium0626 in https://github.com/apache/iceberg-rust/pull/201
* refactor: remove unwraps by @odysa in https://github.com/apache/iceberg-rust/pull/196
* Fix: add required rust version in cargo.toml by @dp-0 in https://github.com/apache/iceberg-rust/pull/193
* Fix the REST spec version by @Fokko in https://github.com/apache/iceberg-rust/pull/198
* feat: Add Sync + Send to Catalog trait by @ZhengLin-Li in https://github.com/apache/iceberg-rust/pull/202
* feat: Make thrift transport configurable by @DeaconDesperado in https://github.com/apache/iceberg-rust/pull/194
* Add UnboundSortOrder by @fqaiser94 in https://github.com/apache/iceberg-rust/pull/115
* ci: Add workflow for publish by @Xuanwo in https://github.com/apache/iceberg-rust/pull/218
* Add workflow for cargo audit by @sdd in https://github.com/apache/iceberg-rust/pull/217
* docs: Add basic README for all crates by @Xuanwo in https://github.com/apache/iceberg-rust/pull/215
* Follow naming convention from Iceberg's Java and Python implementations by @s-akhtar-baig in https://github.com/apache/iceberg-rust/pull/204
* doc: Add download page by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/219
* chore(deps): Update derive_builder requirement from 0.13.0 to 0.20.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/203
* test: add FileIO s3 test by @odysa in https://github.com/apache/iceberg-rust/pull/220
* ci: Ignore RUSTSEC-2023-0071 for no actions to take by @Xuanwo in https://github.com/apache/iceberg-rust/pull/222
* feat: Add expression builder and display. by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/169
* chord:  Add IssueNavigationLink for RustRover by @stream2000 in https://github.com/apache/iceberg-rust/pull/230
* minor: Fix `double` API doc by @viirya in https://github.com/apache/iceberg-rust/pull/226
* feat: add `UnboundPredicate::negate()` by @sdd in https://github.com/apache/iceberg-rust/pull/228
* fix: Remove deprecated methods to pass ci by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/234
* Implement basic Parquet data file reading capability by @sdd in https://github.com/apache/iceberg-rust/pull/207
* chore: doc-test as a target by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/235
* feat: add parquet writer by @ZENOTME in https://github.com/apache/iceberg-rust/pull/176
* Add hive metastore catalog support (part 1/2) by @marvinlanhenke in https://github.com/apache/iceberg-rust/pull/237
* chore: Enable projects. by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/247
* refactor: Make plan_files as asynchronous stream by @viirya in https://github.com/apache/iceberg-rust/pull/243
* feat: Implement binding expression by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/231
* Implement Display instead of ToString by @lewiszlw in https://github.com/apache/iceberg-rust/pull/256
* add rewrite_not by @marvinlanhenke in https://github.com/apache/iceberg-rust/pull/263
* feat: init TableMetadataBuilder by @ZENOTME in https://github.com/apache/iceberg-rust/pull/262
* Rename stat_table to table_exists in Catalog trait by @lewiszlw in https://github.com/apache/iceberg-rust/pull/257
* feat (static table): implement a read-only table struct loaded from metadata by @a-agmon in https://github.com/apache/iceberg-rust/pull/259
* feat: implement OAuth for catalog rest client by @TennyZhuang in https://github.com/apache/iceberg-rust/pull/254
* docs: annotate precision and length to primitive types by @waynexia in https://github.com/apache/iceberg-rust/pull/270
* build: Restore CI by making parquet and arrow version consistent by @viirya in https://github.com/apache/iceberg-rust/pull/280
* Metadata Serde + default partition_specs and sort_orders by @marvinlanhenke in https://github.com/apache/iceberg-rust/pull/272
* feat: make optional oauth param configurable by @himadripal in https://github.com/apache/iceberg-rust/pull/278
* fix: enable public access to ManifestEntry properties by @a-agmon in https://github.com/apache/iceberg-rust/pull/284
* feat: Implement the conversion from Arrow Schema to Iceberg Schema by @viirya in https://github.com/apache/iceberg-rust/pull/258
* Rename function name to `add_manifests` by @viirya in https://github.com/apache/iceberg-rust/pull/293
* Modify `Bind` calls so that they don't consume `self` and instead return a new struct, leaving the original unmoved by @sdd in https://github.com/apache/iceberg-rust/pull/290
* Add hive metastore catalog support (part 2/2) by @marvinlanhenke in https://github.com/apache/iceberg-rust/pull/285
* feat: implement prune column for schema by @Dysprosium0626 in https://github.com/apache/iceberg-rust/pull/261
* chore(deps): Update reqwest requirement from ^0.11 to ^0.12 by @dependabot in https://github.com/apache/iceberg-rust/pull/296
* Glue Catalog: Basic Setup + Test Infra (1/3) by @marvinlanhenke in https://github.com/apache/iceberg-rust/pull/294
* feat: rest client respect prefix prop by @TennyZhuang in https://github.com/apache/iceberg-rust/pull/297
* fix: HMS Catalog missing properties `fn create_namespace` by @marvinlanhenke in https://github.com/apache/iceberg-rust/pull/303
* fix: renaming FileScanTask.data_file to data_manifest_entry by @a-agmon in https://github.com/apache/iceberg-rust/pull/300
* feat: Make OAuth token server configurable by @whynick1 in https://github.com/apache/iceberg-rust/pull/305
* feat: Glue Catalog - namespace operations (2/3) by @marvinlanhenke in https://github.com/apache/iceberg-rust/pull/304
* feat: add transform_literal by @ZENOTME in https://github.com/apache/iceberg-rust/pull/287
* feat: Complete predicate builders for all operators. by @QuakeWang in https://github.com/apache/iceberg-rust/pull/276
* feat: Support customized header in Rest catalog client by @whynick1 in https://github.com/apache/iceberg-rust/pull/306
* fix: chrono dep by @odysa in https://github.com/apache/iceberg-rust/pull/274
* feat: Read Parquet data file with projection by @viirya in https://github.com/apache/iceberg-rust/pull/245
* Fix day timestamp micro by @marvinlanhenke in https://github.com/apache/iceberg-rust/pull/312
* feat: support uri redirect in rest client by @TennyZhuang in https://github.com/apache/iceberg-rust/pull/310
* refine: seperate parquet reader and arrow convert by @ZENOTME in https://github.com/apache/iceberg-rust/pull/313
* chore: upgrade to rust-version 1.77.1 by @marvinlanhenke in https://github.com/apache/iceberg-rust/pull/316
* Support identifier warehouses by @Fokko in https://github.com/apache/iceberg-rust/pull/308
* feat: Project transform by @marvinlanhenke in https://github.com/apache/iceberg-rust/pull/309
* Add Struct Accessors to BoundReferences by @sdd in https://github.com/apache/iceberg-rust/pull/317
* Use `str` args rather than `String` in transform to avoid needing to clone strings by @sdd in https://github.com/apache/iceberg-rust/pull/325
* chore(deps): Update pilota requirement from 0.10.0 to 0.11.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/327
* chore(deps): Bump peaceiris/actions-mdbook from 1 to 2 by @dependabot in https://github.com/apache/iceberg-rust/pull/332
* chore(deps): Bump peaceiris/actions-gh-pages from 3.9.3 to 4.0.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/333
* chore(deps): Bump apache/skywalking-eyes from 0.5.0 to 0.6.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/328
* Add `BoundPredicateVisitor` (alternate version) by @sdd in https://github.com/apache/iceberg-rust/pull/334
* add `InclusiveProjection` Visitor by @sdd in https://github.com/apache/iceberg-rust/pull/335
* feat: Implement the conversion from Iceberg Schema to Arrow Schema by @ZENOTME in https://github.com/apache/iceberg-rust/pull/277
* Simplify expression when doing `{and,or}` operations by @Fokko in https://github.com/apache/iceberg-rust/pull/339
* feat: Glue Catalog - table operations (3/3) by @marvinlanhenke in https://github.com/apache/iceberg-rust/pull/314
* chore: update roadmap by @marvinlanhenke in https://github.com/apache/iceberg-rust/pull/336
* Add `ManifestEvaluator`, used to filter manifests in table scans by @sdd in https://github.com/apache/iceberg-rust/pull/322
* feat: init iceberg writer by @ZENOTME in https://github.com/apache/iceberg-rust/pull/275
* Implement manifest filtering in `TableScan` by @sdd in https://github.com/apache/iceberg-rust/pull/323
* Refactor: Extract `partition_filters` from `ManifestEvaluator` by @marvinlanhenke in https://github.com/apache/iceberg-rust/pull/360
* Basic Integration with Datafusion by @marvinlanhenke in https://github.com/apache/iceberg-rust/pull/324
* refactor: cache partition_schema in `fn plan_files()` by @marvinlanhenke in https://github.com/apache/iceberg-rust/pull/362
* fix (manifest-list): added serde aliases to support both forms conventions by @a-agmon in https://github.com/apache/iceberg-rust/pull/365
* feat: Extract FileRead and FileWrite trait by @Xuanwo in https://github.com/apache/iceberg-rust/pull/364
* feat: Convert predicate to arrow filter and push down to parquet reader by @viirya in https://github.com/apache/iceberg-rust/pull/295
* chore(deps): Update datafusion requirement from 37.0.0 to 38.0.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/369
* chore(deps): Update itertools requirement from 0.12 to 0.13 by @dependabot in https://github.com/apache/iceberg-rust/pull/376
* Add `InclusiveMetricsEvaluator` by @sdd in https://github.com/apache/iceberg-rust/pull/347
* Rename V2 spec names by @gupteaj in https://github.com/apache/iceberg-rust/pull/380
* feat: make file scan task serializable by @ZENOTME in https://github.com/apache/iceberg-rust/pull/377
* Feature: Schema into_builder method by @c-thiel in https://github.com/apache/iceberg-rust/pull/381
* replaced `i32` in `TableUpdate::SetDefaultSortOrder` to `i64` by @rwwwx in https://github.com/apache/iceberg-rust/pull/387
* fix: make PrimitiveLiteral and Literal not be Ord by @ZENOTME in https://github.com/apache/iceberg-rust/pull/386
* docs(writer/docker): fix small typos and wording by @jdockerty in https://github.com/apache/iceberg-rust/pull/389
* feat: `StructAccessor.get` returns `Result<Option<Datum>>` instead of `Result<Datum>` by @sdd in https://github.com/apache/iceberg-rust/pull/390
* feat: add `ExpressionEvaluator` by @marvinlanhenke in https://github.com/apache/iceberg-rust/pull/363
* Derive Clone for TableUpdate by @c-thiel in https://github.com/apache/iceberg-rust/pull/402
* Add accessor for Schema identifier_field_ids by @c-thiel in https://github.com/apache/iceberg-rust/pull/388
* deps: Bump arrow related crates to 52 by @Dysprosium0626 in https://github.com/apache/iceberg-rust/pull/403
* SnapshotRetention::Tag max_ref_age_ms should be optional by @c-thiel in https://github.com/apache/iceberg-rust/pull/391
* feat: Add storage features for iceberg by @Xuanwo in https://github.com/apache/iceberg-rust/pull/400
* Implement BoundPredicateVisitor trait for ManifestFilterVisitor by @s-akhtar-baig in https://github.com/apache/iceberg-rust/pull/367
* Add missing arrow predicate pushdown implementations for `StartsWith`, `NotStartsWith`, `In`, and `NotIn` by @sdd in https://github.com/apache/iceberg-rust/pull/404
* feat: make BoundPredicate,Datum serializable by @ZENOTME in https://github.com/apache/iceberg-rust/pull/406
* refactor: Upgrade hive_metastore to 0.1 by @Xuanwo in https://github.com/apache/iceberg-rust/pull/409
* fix: Remove duplicate filter by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/414
* Enhancement: refine the reader interface by @ZENOTME in https://github.com/apache/iceberg-rust/pull/401
* refactor(catalog/rest): Split http client logic to seperate mod by @Xuanwo in https://github.com/apache/iceberg-rust/pull/423
* Remove #[allow(dead_code)] from the codebase by @vivek378521 in https://github.com/apache/iceberg-rust/pull/421
* ci: use official typos github action by @shoothzj in https://github.com/apache/iceberg-rust/pull/426
* feat: support lower_bound&&upper_bound for parquet writer by @ZENOTME in https://github.com/apache/iceberg-rust/pull/383
* refactor: Implement ArrowAsyncFileWriter directly to remove tokio by @Xuanwo in https://github.com/apache/iceberg-rust/pull/427
* chore: Don't enable reqwest default features by @Xuanwo in https://github.com/apache/iceberg-rust/pull/432
* refactor(catalogs/rest): Split user config and runtime config by @Xuanwo in https://github.com/apache/iceberg-rust/pull/431
* feat: runtime module by @odysa in https://github.com/apache/iceberg-rust/pull/233
* fix: Fix namespace identifier in url by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/435
* refactor(io): Split io into smaller mods by @Xuanwo in https://github.com/apache/iceberg-rust/pull/438
* chore: Use once_cell to replace lazy_static by @Xuanwo in https://github.com/apache/iceberg-rust/pull/443
* fix: Fix build while no-default-features enabled by @Xuanwo in https://github.com/apache/iceberg-rust/pull/442
* chore(deps): Bump crate-ci/typos from 1.22.9 to 1.23.1 by @dependabot in https://github.com/apache/iceberg-rust/pull/447
* docs: Refactor the README to be more user-oriented by @Xuanwo in https://github.com/apache/iceberg-rust/pull/444
* feat: Add cargo machete by @vaibhawvipul in https://github.com/apache/iceberg-rust/pull/448
* chore: Use nightly toolchain for check by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/445
* reuse docker container to save compute resources by @thexiay in https://github.com/apache/iceberg-rust/pull/428
* feat: Add macos runner for ci by @QuakeWang in https://github.com/apache/iceberg-rust/pull/441
* chore: remove compose obsolete version (#452) by @yinheli in https://github.com/apache/iceberg-rust/pull/454
* Refactor file_io_s3_test.rs by @fqaiser94 in https://github.com/apache/iceberg-rust/pull/455
* chore(deps): Bump crate-ci/typos from 1.23.1 to 1.23.2 by @dependabot in https://github.com/apache/iceberg-rust/pull/457
* refine: move binary serialize in literal to datum by @ZENOTME in https://github.com/apache/iceberg-rust/pull/456
* fix: Hms test on macos should use correct arch by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/461
* Fix ManifestFile length calculation by @nooberfsh in https://github.com/apache/iceberg-rust/pull/466
* chore(deps): Update typed-builder requirement from ^0.18 to ^0.19 by @dependabot in https://github.com/apache/iceberg-rust/pull/473
* fix: use avro fixed to represent decimal by @xxchan in https://github.com/apache/iceberg-rust/pull/472
* feat(catalog!): Deprecate rest.authorization-url in favor of oauth2-server-uri by @ndrluis in https://github.com/apache/iceberg-rust/pull/480
* Alter `Transform::Day` to map partition types to `Date` rather than `Int` for consistency with reference implementation by @sdd in https://github.com/apache/iceberg-rust/pull/479
* feat(iceberg): Add memory file IO support by @Xuanwo in https://github.com/apache/iceberg-rust/pull/481
* Add memory catalog implementation by @fqaiser94 in https://github.com/apache/iceberg-rust/pull/475
* chore: Enable new rust code format settings by @Xuanwo in https://github.com/apache/iceberg-rust/pull/483
* docs: Generate rust API docs by @Xuanwo in https://github.com/apache/iceberg-rust/pull/486
* chore: Fix format of recent PRs by @Xuanwo in https://github.com/apache/iceberg-rust/pull/487
* Rename folder to memory by @fqaiser94 in https://github.com/apache/iceberg-rust/pull/490
* chore(deps): Bump crate-ci/typos from 1.23.2 to 1.23.5 by @dependabot in https://github.com/apache/iceberg-rust/pull/493
* View Spec implementation by @c-thiel in https://github.com/apache/iceberg-rust/pull/331
* fix: Return error on reader task by @ndrluis in https://github.com/apache/iceberg-rust/pull/498
* chore: Bump OpenDAL to 0.48 by @Xuanwo in https://github.com/apache/iceberg-rust/pull/500
* feat: add check compatible func for primitive type by @ZENOTME in https://github.com/apache/iceberg-rust/pull/492
* refactor(iceberg): Remove an extra config parse logic by @Xuanwo in https://github.com/apache/iceberg-rust/pull/499
* feat: permit Datum Date<->Int type conversion by @sdd in https://github.com/apache/iceberg-rust/pull/496
* Add additional S3 FileIO Attributes by @c-thiel in https://github.com/apache/iceberg-rust/pull/505
* docs: Add links to dev docs by @Xuanwo in https://github.com/apache/iceberg-rust/pull/508
* chore: Remove typo in README by @Xuanwo in https://github.com/apache/iceberg-rust/pull/509
* feat: podman support by @alexyin1 in https://github.com/apache/iceberg-rust/pull/489
* feat(table): Add debug and clone trait to static table struct by @ndrluis in https://github.com/apache/iceberg-rust/pull/510
* Use namespace location or warehouse location if table location is missing by @fqaiser94 in https://github.com/apache/iceberg-rust/pull/511
* chore(deps): Bump crate-ci/typos from 1.23.5 to 1.23.6 by @dependabot in https://github.com/apache/iceberg-rust/pull/521
* Concurrent table scans by @sdd in https://github.com/apache/iceberg-rust/pull/373
* refactor: replace num_cpus with thread::available_parallelism by @SteveLauC in https://github.com/apache/iceberg-rust/pull/526
* Fix: MappedLocalTime should not be exposed by @c-thiel in https://github.com/apache/iceberg-rust/pull/529
* feat: Establish subproject pyiceberg_core by @Xuanwo in https://github.com/apache/iceberg-rust/pull/518
* fix: complete miss attribute for map && list in avro schema by @ZENOTME in https://github.com/apache/iceberg-rust/pull/411
* arrow/schema.rs: refactor tests by @AndreMouche in https://github.com/apache/iceberg-rust/pull/531
* feat: initialise SQL Catalog by @callum-ryan in https://github.com/apache/iceberg-rust/pull/524
* chore(deps): Bump actions/setup-python from 4 to 5 by @dependabot in https://github.com/apache/iceberg-rust/pull/536
* feat(storage): support aws session token by @twuebi in https://github.com/apache/iceberg-rust/pull/530
* Simplify PrimitiveLiteral by @ZENOTME in https://github.com/apache/iceberg-rust/pull/502
* chore: bump opendal to 0.49 by @jdockerty in https://github.com/apache/iceberg-rust/pull/540
* feat: support timestamp columns in row filters by @sdd in https://github.com/apache/iceberg-rust/pull/533
* fix: don't silently drop errors encountered in table scan file planning by @sdd in https://github.com/apache/iceberg-rust/pull/535
* chore(deps): Update sqlx requirement from 0.7.4 to 0.8.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/537
* Fix main branch building break by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/541
* feat: support for gcs storage by @jdockerty in https://github.com/apache/iceberg-rust/pull/520
* feat: Allow FileIO to reuse http client by @Xuanwo in https://github.com/apache/iceberg-rust/pull/544
* docs: Add an example to scan an iceberg table by @Xuanwo in https://github.com/apache/iceberg-rust/pull/545
* Concurrent data file fetching and parallel RecordBatch processing by @sdd in https://github.com/apache/iceberg-rust/pull/515
* doc: Add statement for contributors to avoid force push as much as possible by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/546

## v0.2.0 - 2024-02-20

* chore: Setup project layout by @Xuanwo in https://github.com/apache/iceberg-rust/pull/1
* ci: Fix version for apache/skywalking-eyes/header by @Xuanwo in https://github.com/apache/iceberg-rust/pull/4
* feat: Implement serialize/deserialize for datatypes by @JanKaul in https://github.com/apache/iceberg-rust/pull/6
* docs: Add CONTRIBUTING and finish project setup by @Xuanwo in https://github.com/apache/iceberg-rust/pull/7
* feat: Add lookup tables to StructType by @JanKaul in https://github.com/apache/iceberg-rust/pull/12
* feat: Implement error handling by @Xuanwo in https://github.com/apache/iceberg-rust/pull/13
* chore: Use HashMap instead of BTreeMap for storing fields by id in StructType by @amogh-jahagirdar in https://github.com/apache/iceberg-rust/pull/14
* chore: Change iceberg into workspace by @Xuanwo in https://github.com/apache/iceberg-rust/pull/15
* feat: Use macro to define from error. by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/17
* feat: Introduce schema definition. by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/19
* refactor: Align data type with other implementation. by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/21
* chore: Ignore .idea by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/27
* feat: Implement Iceberg values by @JanKaul in https://github.com/apache/iceberg-rust/pull/20
* feat: Define schema post order visitor. by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/25
* feat: Add transform by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/26
* fix: Fix build break in main branch by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/30
* fix: Update github configuration to avoid conflicting merge by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/31
* chore(deps): Bump apache/skywalking-eyes from 0.4.0 to 0.5.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/35
* feat: Table metadata by @JanKaul in https://github.com/apache/iceberg-rust/pull/29
* feat: Add utility methods to help conversion between literals. by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/38
* [comment] should be IEEE 754 rather than 753 by @zhjwpku in https://github.com/apache/iceberg-rust/pull/39
* fix: Add doc test action by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/44
* chore: Ping toolchain version by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/48
* feat: Introduce conversion between iceberg schema and avro schema by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/40
* feat: Allow Schema Serialization/deserialization by @y0psolo in https://github.com/apache/iceberg-rust/pull/46
* chore: Add cargo sort check by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/51
* chore(deps): Bump actions/checkout from 3 to 4 by @dependabot in https://github.com/apache/iceberg-rust/pull/58
* Metadata integration tests by @JanKaul in https://github.com/apache/iceberg-rust/pull/57
* feat: Introduce FileIO by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/53
* feat: Add Catalog API by @Xuanwo in https://github.com/apache/iceberg-rust/pull/54
* feat: support transform function by @ZENOTME in https://github.com/apache/iceberg-rust/pull/42
* chore(deps): Update ordered-float requirement from 3.7.0 to 4.0.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/64
* feat: Add public methods for catalog related structs by @Xuanwo in https://github.com/apache/iceberg-rust/pull/63
* minor: Upgrade to latest toolchain by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/68
* chore(deps): Update opendal requirement from 0.39 to 0.40 by @dependabot in https://github.com/apache/iceberg-rust/pull/65
* refactor: Make directory for catalog by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/69
* feat: support read Manifest List by @ZENOTME in https://github.com/apache/iceberg-rust/pull/56
* chore(deps): Update apache-avro requirement from 0.15 to 0.16 by @dependabot in https://github.com/apache/iceberg-rust/pull/71
* fix: avro bytes test for Literal by @JanKaul in https://github.com/apache/iceberg-rust/pull/80
* chore(deps): Update opendal requirement from 0.40 to 0.41 by @dependabot in https://github.com/apache/iceberg-rust/pull/84
* feat: manifest list writer by @barronw in https://github.com/apache/iceberg-rust/pull/76
* feat: First version of rest catalog. by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/78
* chore(deps): Update typed-builder requirement from ^0.17 to ^0.18 by @dependabot in https://github.com/apache/iceberg-rust/pull/87
* feat: Implement load table api. by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/89
* chroes:Manage dependencies using workspace. by @my-vegetable-has-exploded in https://github.com/apache/iceberg-rust/pull/93
* minor: Provide Debug impl for pub structs #73 by @DeaconDesperado in https://github.com/apache/iceberg-rust/pull/92
* feat: support ser/deser of value  by @ZENOTME in https://github.com/apache/iceberg-rust/pull/82
* fix: Migrate from tempdir to tempfile crate by @cdaudt in https://github.com/apache/iceberg-rust/pull/91
* chore(deps): Update opendal requirement from 0.41 to 0.42 by @dependabot in https://github.com/apache/iceberg-rust/pull/101
* chore(deps): Update itertools requirement from 0.11 to 0.12 by @dependabot in https://github.com/apache/iceberg-rust/pull/102
* Replace i64 with DateTime by @fqaiser94 in https://github.com/apache/iceberg-rust/pull/94
* feat: Implement create table and update table api for rest catalog. by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/97
* Fix compile failures by @fqaiser94 in https://github.com/apache/iceberg-rust/pull/105
* feat: replace 'Builder' with 'TypedBuilder' for 'Snapshot' by @xiaoyang-sde in https://github.com/apache/iceberg-rust/pull/110
* chore: Upgrade uuid manually and remove pinned version by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/108
* chore: Add cargo build and build guide by @manuzhang in https://github.com/apache/iceberg-rust/pull/111
* feat: Add hms catalog layout by @Xuanwo in https://github.com/apache/iceberg-rust/pull/112
* feat: support UnboundPartitionSpec by @my-vegetable-has-exploded in https://github.com/apache/iceberg-rust/pull/106
* test: Add integration tests for rest catalog. by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/109
* chore(deps): Update opendal requirement from 0.42 to 0.43 by @dependabot in https://github.com/apache/iceberg-rust/pull/116
* feat: suport read/write Manifest by @ZENOTME in https://github.com/apache/iceberg-rust/pull/79
* test: Remove binary manifest list avro file by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/118
* refactor: Conversion between literal and json should depends on type. by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/120
* fix: fix parse partitions in manifest_list by @ZENOTME in https://github.com/apache/iceberg-rust/pull/122
* feat: Add website layout by @Xuanwo in https://github.com/apache/iceberg-rust/pull/130
* feat: Expression system. by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/132
* website: Fix typo in book.toml by @Xuanwo in https://github.com/apache/iceberg-rust/pull/136
* Set `ghp_{pages,path}` properties by @Fokko in https://github.com/apache/iceberg-rust/pull/138
* chore: Upgrade toolchain to 1.75.0 by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/140
* feat: Add roadmap and features status in README.md by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/134
* Remove `publish:` section from `.asf.yaml` by @Fokko in https://github.com/apache/iceberg-rust/pull/141
* chore(deps): Bump peaceiris/actions-gh-pages from 3.9.2 to 3.9.3 by @dependabot in https://github.com/apache/iceberg-rust/pull/143
* chore(deps): Update opendal requirement from 0.43 to 0.44 by @dependabot in https://github.com/apache/iceberg-rust/pull/142
* docs: Change homepage to rust.i.a.o by @Xuanwo in https://github.com/apache/iceberg-rust/pull/146
* feat: Introduce basic file scan planning. by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/129
* chore: Update contributing guide. by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/163
* chore: Update reader api status by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/162
* #154 : Add homepage to Cargo.toml by @hiirrxnn in https://github.com/apache/iceberg-rust/pull/160
* Add formatting for toml files by @Tyler-Sch in https://github.com/apache/iceberg-rust/pull/167
* chore(deps): Update env_logger requirement from 0.10.0 to 0.11.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/170
* feat: init file writer interface by @ZENOTME in https://github.com/apache/iceberg-rust/pull/168
* fix: Manifest parsing should consider schema evolution. by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/171
* docs: Add release guide for iceberg-rust by @Xuanwo in https://github.com/apache/iceberg-rust/pull/147
* fix: Ignore negative statistics value by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/173
* feat: Add user guide for website. by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/178
* chore(deps): Update derive_builder requirement from 0.12.0 to 0.13.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/175
* refactor: Replace unwrap by @odysa in https://github.com/apache/iceberg-rust/pull/183
* feat: add handwritten serialize by @odysa in https://github.com/apache/iceberg-rust/pull/185
* Fix: avro schema names for manifest and manifest_list by @JanKaul in https://github.com/apache/iceberg-rust/pull/182
* feat: Bump hive_metastore to use pure rust thrift impl `volo` by @Xuanwo in https://github.com/apache/iceberg-rust/pull/174
* feat: Bump version 0.2.0 to prepare for release. by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/181
* fix: default_partition_spec using the partion_spec_id set by @odysa in https://github.com/apache/iceberg-rust/pull/190
* Docs: Add required Cargo version to install guide by @manuzhang in https://github.com/apache/iceberg-rust/pull/191
* chore(deps): Update opendal requirement from 0.44 to 0.45 by @dependabot in https://github.com/apache/iceberg-rust/pull/195

[v0.3.0]: https://github.com/apache/iceberg-rust/compare/v0.2.0...v0.3.0