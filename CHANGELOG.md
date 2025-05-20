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

## [v0.5.0] - 2025-05-19
* io: add support for role arn and external id s3 props by @mattheusv in https://github.com/apache/iceberg-rust/pull/553
* fix: ensure S3 and GCS integ tests are conditionally compiled only when the storage-s3 and storage-gcs features are enabled by @sdd in https://github.com/apache/iceberg-rust/pull/552
* docs: fix main iceberg example by @jdockerty in https://github.com/apache/iceberg-rust/pull/554
* io: add support to set assume role session name by @mattheusv in https://github.com/apache/iceberg-rust/pull/555
* test: refactor datafusion test with memory catalog by @FANNG1 in https://github.com/apache/iceberg-rust/pull/557
* chore: add clean job in Makefile by @ChinoUkaegbu in https://github.com/apache/iceberg-rust/pull/561
* docs: Fix build website permission changed by @Xuanwo in https://github.com/apache/iceberg-rust/pull/564
* Object Cache: caches parsed Manifests and ManifestLists for performance by @sdd in https://github.com/apache/iceberg-rust/pull/512
* Update the paths by @Fokko in https://github.com/apache/iceberg-rust/pull/569
* docs: Add links for released crates by @Xuanwo in https://github.com/apache/iceberg-rust/pull/570
* Python: Use hatch for dependency management by @sungwy in https://github.com/apache/iceberg-rust/pull/572
* Ensure that RestCatalog passes user config to FileIO by @sdd in https://github.com/apache/iceberg-rust/pull/476
* Move `zlib` and `unicode` licenses to `allow` by @Fokko in https://github.com/apache/iceberg-rust/pull/566
* website: Update links for 0.3.0 by @Xuanwo in https://github.com/apache/iceberg-rust/pull/573
* feat(timestamp_ns): Implement timestamps with nanosecond precision by @Sl1mb0 in https://github.com/apache/iceberg-rust/pull/542
* fix: correct partition-id to field-id in UnboundPartitionField by @FANNG1 in https://github.com/apache/iceberg-rust/pull/576
* fix: Update sqlx from 0.8.0 to 0.8.1 by @FANNG1 in https://github.com/apache/iceberg-rust/pull/584
* chore(deps): Update typed-builder requirement from 0.19 to 0.20 by @dependabot in https://github.com/apache/iceberg-rust/pull/582
* Expose Transforms to Python Binding by @sungwy in https://github.com/apache/iceberg-rust/pull/556
* chore(deps): Bump crate-ci/typos from 1.23.6 to 1.24.1 by @dependabot in https://github.com/apache/iceberg-rust/pull/583
* Table Scan: Add Row Group Skipping by @sdd in https://github.com/apache/iceberg-rust/pull/558
* chore: bump crate-ci/typos to 1.24.3 by @sdlarsen in https://github.com/apache/iceberg-rust/pull/598
* feat: SQL Catalog - namespaces by @callum-ryan in https://github.com/apache/iceberg-rust/pull/534
* feat: Add more fields in FileScanTask by @Xuanwo in https://github.com/apache/iceberg-rust/pull/609
* chore(deps): Bump crate-ci/typos from 1.24.3 to 1.24.5 by @dependabot in https://github.com/apache/iceberg-rust/pull/616
* fix: Less Panics for Snapshot timestamps by @c-thiel in https://github.com/apache/iceberg-rust/pull/614
* feat: partition compatibility by @c-thiel in https://github.com/apache/iceberg-rust/pull/612
* feat: SortOrder methods should take schema ref if possible by @c-thiel in https://github.com/apache/iceberg-rust/pull/613
* feat: add `client.region` by @jdockerty in https://github.com/apache/iceberg-rust/pull/623
* fix: Correctly calculate highest_field_id in schema by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/590
* Feat: Normalize TableMetadata by @c-thiel in https://github.com/apache/iceberg-rust/pull/611
* refactor(python): Expose transform as a submodule for pyiceberg_core by @Xuanwo in https://github.com/apache/iceberg-rust/pull/628
* feat: support projection pushdown for datafusion iceberg by @FANNG1 in https://github.com/apache/iceberg-rust/pull/594
* chore: Bump opendal to 0.50 by @Xuanwo in https://github.com/apache/iceberg-rust/pull/634
* feat: add Sync to TransformFunction by @xxchan in https://github.com/apache/iceberg-rust/pull/638
* feat: expose arrow type <-> iceberg type by @xxchan in https://github.com/apache/iceberg-rust/pull/637
* doc: improve FileIO doc by @xxchan in https://github.com/apache/iceberg-rust/pull/642
* chore(deps): Bump crate-ci/typos from 1.24.5 to 1.24.6 by @dependabot in https://github.com/apache/iceberg-rust/pull/640
* Migrate to arrow-* v53 by @sdd in https://github.com/apache/iceberg-rust/pull/626
* feat: expose remove_all in FileIO by @xxchan in https://github.com/apache/iceberg-rust/pull/643
* feat (datafusion integration): convert datafusion expr filters to Iceberg Predicate by @a-agmon in https://github.com/apache/iceberg-rust/pull/588
* feat: Add NamespaceIdent.parent() by @c-thiel in https://github.com/apache/iceberg-rust/pull/641
* Table Scan: Add Row Selection Filtering by @sdd in https://github.com/apache/iceberg-rust/pull/565
* fix: compile error due to merge stale PR by @xxchan in https://github.com/apache/iceberg-rust/pull/646
* scan: change ErrorKind when table dont have spanshots by @mattheusv in https://github.com/apache/iceberg-rust/pull/608
* fix: avoid to create operator of memory storage every time by @ZENOTME in https://github.com/apache/iceberg-rust/pull/635
* feat (datafusion): making IcebergTableProvider public to be used without a catalog by @a-agmon in https://github.com/apache/iceberg-rust/pull/650
* test (datafusion): add test for table provider creation by @a-agmon in https://github.com/apache/iceberg-rust/pull/651
* fix: page index evaluator min/max args inverted by @sdd in https://github.com/apache/iceberg-rust/pull/648
* chore: fix typo in FileIO Schemes  by @wcy-fdu in https://github.com/apache/iceberg-rust/pull/653
* fix: TableUpdate Snapshot deserialization for v1 by @c-thiel in https://github.com/apache/iceberg-rust/pull/656
* feat: Reassign field ids for schema by @c-thiel in https://github.com/apache/iceberg-rust/pull/615
* feat: add gcp oauth support by @twuebi in https://github.com/apache/iceberg-rust/pull/654
* fix(arrow): Use new ParquetMetaDataReader instead by @Xuanwo in https://github.com/apache/iceberg-rust/pull/661
* chore(deps): bump typos crate to 1.25.0 by @matthewwillian in https://github.com/apache/iceberg-rust/pull/662
* RecordBatchTransformer: Handle schema migration and column re-ordering in table scans by @sdd in https://github.com/apache/iceberg-rust/pull/602
* docs: installation of the new `iceberg_catalog_rest` added to the docs by @nishant-sachdeva in https://github.com/apache/iceberg-rust/pull/355
* feat(datafusion): Support pushdown more datafusion exprs to Iceberg by @FANNG1 in https://github.com/apache/iceberg-rust/pull/649
* feat: Derive PartialEq for FileScanTask by @Xuanwo in https://github.com/apache/iceberg-rust/pull/660
* feat: SQL Catalog - Tables by @callum-ryan in https://github.com/apache/iceberg-rust/pull/610
* ci: Allow install a non-debian-packaged Python package by @Xuanwo in https://github.com/apache/iceberg-rust/pull/666
* docs: README uses iceberg-rust instead of we by @caicancai in https://github.com/apache/iceberg-rust/pull/667
* chore(deps): Bump crate-ci/typos from 1.25.0 to 1.26.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/668
* feat: Add equality delete writer by @Dysprosium0626 in https://github.com/apache/iceberg-rust/pull/372
* Revert "feat: Add equality delete writer (#372)" by @Xuanwo in https://github.com/apache/iceberg-rust/pull/672
* ci: Fix CI for bindings python by @Xuanwo in https://github.com/apache/iceberg-rust/pull/678
* fix: OpenDAL `is_exist` => `exists` by @sdd in https://github.com/apache/iceberg-rust/pull/680
* feat: Expose ManifestEntry status by @zheilbron in https://github.com/apache/iceberg-rust/pull/681
* feat: allow empty projection in table scan by @sundy-li in https://github.com/apache/iceberg-rust/pull/677
* chore(deps): Bump crate-ci/typos from 1.26.0 to 1.26.8 by @dependabot in https://github.com/apache/iceberg-rust/pull/683
* fix: bump parquet minor version by @xxchan in https://github.com/apache/iceberg-rust/pull/684
* fix(type): fix type promote to ignore field name. by @chenzl25 in https://github.com/apache/iceberg-rust/pull/685
* feat: implement IcebergTableProviderFactory for datafusion by @yukkit in https://github.com/apache/iceberg-rust/pull/600
* feat: Safer PartitionSpec & SchemalessPartitionSpec by @c-thiel in https://github.com/apache/iceberg-rust/pull/645
* chore(deps): Bump crate-ci/typos from 1.26.8 to 1.27.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/687
* feat: TableMetadata accessors for current ids of Schema, Snapshot and SortOrder by @c-thiel in https://github.com/apache/iceberg-rust/pull/688
* chore: upgrade to DataFusion 43 by @gruuya in https://github.com/apache/iceberg-rust/pull/691
* chore(deps): Bump crate-ci/typos from 1.27.0 to 1.27.3 by @dependabot in https://github.com/apache/iceberg-rust/pull/693
* feat: Expose length of Iterators by @c-thiel in https://github.com/apache/iceberg-rust/pull/692
* feat: Implement TableRequirement checks by @c-thiel in https://github.com/apache/iceberg-rust/pull/689
* feat: Add ViewUpdate to catalog by @c-thiel in https://github.com/apache/iceberg-rust/pull/690
* chore: update .asf.yaml by @c-thiel in https://github.com/apache/iceberg-rust/pull/701
* datafusion: Create table provider for a snapshot. by @ryzhyk in https://github.com/apache/iceberg-rust/pull/707
* Add Python Release Action to publish `pyiceberg_core` dist to Pypi by @sungwy in https://github.com/apache/iceberg-rust/pull/705
* chore: Mark `last-field-id` as deprecated by @Fokko in https://github.com/apache/iceberg-rust/pull/715
* TableMetadataBuilder by @c-thiel in https://github.com/apache/iceberg-rust/pull/587
* Add `fallback` attribute to all `strip_option`s. by @ryzhyk in https://github.com/apache/iceberg-rust/pull/708
* fix: Remove check of last_column_id by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/717
* Fix error running data fusion queries - Physical input schema should be the same as the one converted from logical input schema by @FANNG1 in https://github.com/apache/iceberg-rust/pull/664
* chore: Typo in test :) by @Fokko in https://github.com/apache/iceberg-rust/pull/727
* Derive Clone for IcebergTableProvider by @SergeiPatiakin in https://github.com/apache/iceberg-rust/pull/722
* fix: expand arrow to iceberg schema to handle nanosecond timestamp by @jdockerty in https://github.com/apache/iceberg-rust/pull/710
* feat: Add equality delete writer by @ZENOTME in https://github.com/apache/iceberg-rust/pull/703
* chore: Bump upload-artifact@v3 to v4 by @sungwy in https://github.com/apache/iceberg-rust/pull/725
* feat: support append data file and add e2e test by @ZENOTME in https://github.com/apache/iceberg-rust/pull/349
* chore(deps): Bump actions/setup-python from 4 to 5 by @dependabot in https://github.com/apache/iceberg-rust/pull/746
* chore: Align argument name with doc comment by @SergeiPatiakin in https://github.com/apache/iceberg-rust/pull/750
* fix: equality delete writer field id project by @ZENOTME in https://github.com/apache/iceberg-rust/pull/751
* feat: expose opendal S3 options for anonymous access by @gruuya in https://github.com/apache/iceberg-rust/pull/757
* fix: current-snapshot-id serialized to -1 in TableMetadata.json by @c-thiel in https://github.com/apache/iceberg-rust/pull/755
* feat(puffin): Add Puffin crate and CompressionCodec by @fqaiser94 in https://github.com/apache/iceberg-rust/pull/745
* Build: Delete branch automatically on PR merge by @manuzhang in https://github.com/apache/iceberg-rust/pull/764
* chore(deps): bump crate-ci/typos from 1.27.3 to 1.28.1 by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/769
* chore(deps): Bump crate-ci/typos from 1.27.3 to 1.28.2 by @dependabot in https://github.com/apache/iceberg-rust/pull/767
* Add Spark for integration tests by @Fokko in https://github.com/apache/iceberg-rust/pull/766
* refine: refine writer interface by @ZENOTME in https://github.com/apache/iceberg-rust/pull/741
* Clean up docker Docker by @Fokko in https://github.com/apache/iceberg-rust/pull/770
* name mapping serde by @barronw in https://github.com/apache/iceberg-rust/pull/740
* docker: The `archive` seems unstable by @Fokko in https://github.com/apache/iceberg-rust/pull/773
* doc: add RisingWave to users by @xxchan in https://github.com/apache/iceberg-rust/pull/775
* feat: Expose disable_config_load opendal S3 option by @gruuya in https://github.com/apache/iceberg-rust/pull/782
* Suport conversion of Arrow Int8 and Int16 to Iceberg Int by @gruuya in https://github.com/apache/iceberg-rust/pull/787
* infra: Dismiss stale reviews by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/779
* fix: return type for year and month transform should be int by @xxchan in https://github.com/apache/iceberg-rust/pull/776
* feat: Allow for schema evolution by @Fokko in https://github.com/apache/iceberg-rust/pull/786
* Retry object store reads on temporary errors. by @ryzhyk in https://github.com/apache/iceberg-rust/pull/788
* refactor(puffin): Move puffin crate contents inside iceberg crate by @fqaiser94 in https://github.com/apache/iceberg-rust/pull/789
* feat: Implement Decimal from/to bytes represents by @Xuanwo in https://github.com/apache/iceberg-rust/pull/665
* feat: eagerly project the arrow schema to scope out non-selected fields by @gruuya in https://github.com/apache/iceberg-rust/pull/785
* fix: wrong compute of partitions in manifest by @ZENOTME in https://github.com/apache/iceberg-rust/pull/794
* fix: set key_metadata to Null by default by @feniljain in https://github.com/apache/iceberg-rust/pull/800
* test: append partition data file by @feniljain in https://github.com/apache/iceberg-rust/pull/742
* chore: Add more debug message inside error by @Xuanwo in https://github.com/apache/iceberg-rust/pull/793
* fix: Error source from cache has been shadowed by @Xuanwo in https://github.com/apache/iceberg-rust/pull/792
* fix(catalog/rest): Ensure token been reused correctly by @Xuanwo in https://github.com/apache/iceberg-rust/pull/801
* feat!: Remove `BoundPartitionSpec` by @c-thiel in https://github.com/apache/iceberg-rust/pull/771
* chore(deps): Bump crate-ci/typos from 1.28.2 to 1.28.3 by @dependabot in https://github.com/apache/iceberg-rust/pull/805
* feat: add `DataFileWriter` tests for schema and partition by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/768
* fix: day transform compute by @ZENOTME in https://github.com/apache/iceberg-rust/pull/796
* feat: TableMetadata Statistic Files by @c-thiel in https://github.com/apache/iceberg-rust/pull/799
* Bump `pyiceberg_core` to 0.4.0 by @sungwy in https://github.com/apache/iceberg-rust/pull/808
* chore(docs): Update Readme - Lakekeeper repository moved by @c-thiel in https://github.com/apache/iceberg-rust/pull/810
* Prep 0.4.0 release by @sungwy in https://github.com/apache/iceberg-rust/pull/809
* feat: Add RemovePartitionSpecs table update by @c-thiel in https://github.com/apache/iceberg-rust/pull/804
* feat: Store file io props to allow re-build it by @Xuanwo in https://github.com/apache/iceberg-rust/pull/802
* chore: Generate Changelog Dependencies for 0.4.0 release by @sungwy in https://github.com/apache/iceberg-rust/pull/812
* chore: chmod +x on `verify.py` script by @sungwy in https://github.com/apache/iceberg-rust/pull/817
* Chore: Add `AboveMax` and `BelowMin` by @Fokko in https://github.com/apache/iceberg-rust/pull/820
* fix: Reading a table with positional deletes should fail by @Fokko in https://github.com/apache/iceberg-rust/pull/826
* chore: Updated Changelog for 0.4.0-rc3 by @sungwy in https://github.com/apache/iceberg-rust/pull/830
* fix(datafusion): Align schemas for DataFusion plan and stream by @gruuya in https://github.com/apache/iceberg-rust/pull/829
* Add crate for sqllogictest. by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/827
* chore(deps): Bump crate-ci/typos from 1.28.3 to 1.28.4 by @dependabot in https://github.com/apache/iceberg-rust/pull/832
* chore: update download link to 0.4.0 by @sungwy in https://github.com/apache/iceberg-rust/pull/836
* refactor: Remove spawn and channel inside arrow reader by @Xuanwo in https://github.com/apache/iceberg-rust/pull/806
* chore: improve and fix the rest example by @goldmedal in https://github.com/apache/iceberg-rust/pull/842
* feat: Bump opendal to 0.51 by @Xuanwo in https://github.com/apache/iceberg-rust/pull/839
* fix: support both gs and gcs schemes for google cloud storage by @chenzl25 in https://github.com/apache/iceberg-rust/pull/845
* feat: Expose disable_config_load opendal GCS option by @chenzl25 in https://github.com/apache/iceberg-rust/pull/847
* fix: project_bacth to project_batch by @feniljain in https://github.com/apache/iceberg-rust/pull/848
* build: check in Cargo.lock by @xxchan in https://github.com/apache/iceberg-rust/pull/851
* ci: use officail rustsec/audit-check action by @xxchan in https://github.com/apache/iceberg-rust/pull/843
* feat: add s3tables catalog by @flaneur2020 in https://github.com/apache/iceberg-rust/pull/807
* fix: fix sql catalog drop table by @Li0k in https://github.com/apache/iceberg-rust/pull/853
* ci: add rust-cache action by @xxchan in https://github.com/apache/iceberg-rust/pull/844
* chore: Fix cargo.lock not updated by @Xuanwo in https://github.com/apache/iceberg-rust/pull/855
* fix(catalog): delete metadata file when droping table in MemoryCatalog by @lewiszlw in https://github.com/apache/iceberg-rust/pull/854
* chore(deps): Bump serde from 1.0.216 to 1.0.217 by @dependabot in https://github.com/apache/iceberg-rust/pull/860
* chore(deps): Bump reqwest from 0.12.10 to 0.12.11 by @dependabot in https://github.com/apache/iceberg-rust/pull/859
* chore(deps): Bump aws-sdk-s3tables from 1.1.0 to 1.2.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/858
* Add orbstack guide by @lewiszlw in https://github.com/apache/iceberg-rust/pull/856
* feat: support metadata table "snapshots" by @xxchan in https://github.com/apache/iceberg-rust/pull/822
* feat: Support metadata table "Manifests"  by @flaneur2020 in https://github.com/apache/iceberg-rust/pull/861
* feat: support serialize/deserialize DataFile into avro bytes by @ZENOTME in https://github.com/apache/iceberg-rust/pull/797
* [doc] Remove registry mirror recommendations by @kevinjqliu in https://github.com/apache/iceberg-rust/pull/866
* fix: valid identifier id in nested map fail by @ZENOTME in https://github.com/apache/iceberg-rust/pull/864
* chore: datafusion 44 upgrade by @gruuya in https://github.com/apache/iceberg-rust/pull/867
* fix: parse var len of decimal for parquet statistic by @ZENOTME in https://github.com/apache/iceberg-rust/pull/837
* ci: use taiki-e/install-action to install tools from binary by @xxchan in https://github.com/apache/iceberg-rust/pull/852
* chore(deps): Bump crate-ci/typos from 1.28.4 to 1.29.4 by @dependabot in https://github.com/apache/iceberg-rust/pull/873
* chore(deps): Bump aws-sdk-s3tables from 1.2.0 to 1.3.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/874
* chore(deps): Bump reqwest from 0.12.11 to 0.12.12 by @dependabot in https://github.com/apache/iceberg-rust/pull/875
* chore(deps): Bump moka from 0.12.8 to 0.12.9 by @dependabot in https://github.com/apache/iceberg-rust/pull/876
* chore(deps): Bump async-trait from 0.1.83 to 0.1.84 by @dependabot in https://github.com/apache/iceberg-rust/pull/877
* chore(deps): Bump tempfile from 3.14.0 to 3.15.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/878
* Split metadata tables into separate modules by @rshkv in https://github.com/apache/iceberg-rust/pull/872
* Rename 'metadata_table' to 'inspect' by @rshkv in https://github.com/apache/iceberg-rust/pull/881
* Metadata table scans as streams by @rshkv in https://github.com/apache/iceberg-rust/pull/870
* chore(deps): Bump tokio from 1.42.0 to 1.43.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/885
* chore(deps): Bump moka from 0.12.9 to 0.12.10 by @dependabot in https://github.com/apache/iceberg-rust/pull/886
* chore(deps): Bump aws-sdk-glue from 1.74.0 to 1.76.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/887
* chore(deps): Bump serde_json from 1.0.134 to 1.0.135 by @dependabot in https://github.com/apache/iceberg-rust/pull/889
* chore(deps): Bump aws-config from 1.5.11 to 1.5.13 by @dependabot in https://github.com/apache/iceberg-rust/pull/888
* Handle converting Utf8View & BinaryView to Iceberg schema by @phillipleblanc in https://github.com/apache/iceberg-rust/pull/831
* feat(puffin): Parse Puffin FileMetadata by @fqaiser94 in https://github.com/apache/iceberg-rust/pull/765
* ci: check MSRV correctly by @xxchan in https://github.com/apache/iceberg-rust/pull/849
* fix: spark version in integration_tests by @feniljain in https://github.com/apache/iceberg-rust/pull/894
* refine: refine interface of ManifestWriter by @ZENOTME in https://github.com/apache/iceberg-rust/pull/738
* feat(datafusion): Support cast operations by @Fokko in https://github.com/apache/iceberg-rust/pull/821
* chore(deps): Bump opendal from 0.51.0 to 0.51.1 by @dependabot in https://github.com/apache/iceberg-rust/pull/898
* chore(deps): Bump async-trait from 0.1.84 to 0.1.85 by @dependabot in https://github.com/apache/iceberg-rust/pull/897
* chore(deps): Bump arrow-schema from 53.3.0 to 53.4.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/900
* chore(deps): Bump aws-sdk-s3tables from 1.3.0 to 1.4.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/899
* fix: fix timesmtap_ns serde name by @ZENOTME in https://github.com/apache/iceberg-rust/pull/905
* add python version support range to pyproject.toml by @trim21 in https://github.com/apache/iceberg-rust/pull/903
* feat: support scan nested type(struct, map, list) by @ZENOTME in https://github.com/apache/iceberg-rust/pull/882
* fix: Sort Order ID in TableMetadataBuilder changes should be updated by @c-thiel in https://github.com/apache/iceberg-rust/pull/909
* Add pyspark DataFusion integration test by @gruuya in https://github.com/apache/iceberg-rust/pull/850
* test: replace `assert!(<actual> == <real>)` by `assert_eq!(<actual>, <real>)` in some tests by @hussein-awala in https://github.com/apache/iceberg-rust/pull/910
* refactor: fix a typo in manifest_entries field name by @hussein-awala in https://github.com/apache/iceberg-rust/pull/911
* chore(deps): Bump aws-sdk-s3tables from 1.4.0 to 1.6.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/912
* chore(deps): Bump uuid from 1.12.0 to 1.12.1 by @dependabot in https://github.com/apache/iceberg-rust/pull/913
* chore(deps): Bump aws-config from 1.5.13 to 1.5.15 by @dependabot in https://github.com/apache/iceberg-rust/pull/914
* chore(deps): Bump arrow-array from 53.3.0 to 53.4.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/915
* Add Truncate for Binary type by @Fokko in https://github.com/apache/iceberg-rust/pull/920
* Bump number of open Dependabot PRs by @Fokko in https://github.com/apache/iceberg-rust/pull/921
* chore(deps): Bump tempfile from 3.15.0 to 3.16.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/931
* chore(deps): Bump arrow-select from 53.3.0 to 53.4.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/927
* chore(deps): Bump aws-sdk-s3tables from 1.6.0 to 1.7.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/926
* chore(deps): Bump arrow-ord from 53.3.0 to 53.4.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/930
* chore(deps): Bump serde_json from 1.0.135 to 1.0.138 by @dependabot in https://github.com/apache/iceberg-rust/pull/929
* chore(deps): Bump arrow-cast from 53.3.0 to 53.4.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/925
* chore(deps): Bump aws-sdk-s3tables from 1.7.0 to 1.8.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/938
* chore(deps): Bump arrow-string from 53.3.0 to 53.4.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/937
* chore(deps): Bump crate-ci/typos from 1.29.4 to 1.29.5 by @dependabot in https://github.com/apache/iceberg-rust/pull/934
* chore(deps): Bump parquet from 53.3.0 to 53.4.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/935
* chore(deps): Bump async-trait from 0.1.85 to 0.1.86 by @dependabot in https://github.com/apache/iceberg-rust/pull/936
* fix(s3): path-style-access means no virtual-host by @twuebi in https://github.com/apache/iceberg-rust/pull/944
* Table Scan Delete File Handling: Positional and Equality Delete Support by @sdd in https://github.com/apache/iceberg-rust/pull/652
* feat(glue): use the same props for creating aws sdk and for FileIO by @omerhadari in https://github.com/apache/iceberg-rust/pull/947
* chore: use shared containers for integration tests by @gruuya in https://github.com/apache/iceberg-rust/pull/924
* feat(puffin): Add PuffinReader by @fqaiser94 in https://github.com/apache/iceberg-rust/pull/892
* fix: Make s3tables catalog public by @zilder in https://github.com/apache/iceberg-rust/pull/918
* fix(metadata): export iceberg schema in manifests table by @flaneur2020 in https://github.com/apache/iceberg-rust/pull/871
* chore: fix Cargo.lock diff always present after `cargo build` by @VVKot in https://github.com/apache/iceberg-rust/pull/952
* chore(deps): Bump once_cell from 1.20.2 to 1.20.3 by @dependabot in https://github.com/apache/iceberg-rust/pull/954
* chore(deps): Bump aws-sdk-s3tables from 1.8.0 to 1.9.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/956
* chore(deps): Bump uuid from 1.12.1 to 1.13.1 by @dependabot in https://github.com/apache/iceberg-rust/pull/957
* chore(deps): Bump opendal from 0.51.1 to 0.51.2 by @dependabot in https://github.com/apache/iceberg-rust/pull/958
* fix: allow nullable field of equality delete writer by @ZENOTME in https://github.com/apache/iceberg-rust/pull/834
* fix: Misleading error messages in `iceberg-catalog-rest` and allow `StatusCode::OK` in responses by @connortsui20 in https://github.com/apache/iceberg-rust/pull/962
* chore: use RowSelection::union from arrow-rs by @VVKot in https://github.com/apache/iceberg-rust/pull/953
* fix: Fix typos upgrade to 1.29.7 by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/974
* chore(deps): Bump aws-config from 1.5.15 to 1.5.16 by @dependabot in https://github.com/apache/iceberg-rust/pull/973
* chore(deps): Bump aws-sdk-s3tables from 1.9.0 to 1.10.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/970
* chore(deps): Bump apache/skywalking-eyes from 0.6.0 to 0.7.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/969
* chore: datafusion 45 upgrade by @kevinjqliu in https://github.com/apache/iceberg-rust/pull/943
* chore(ci): upgrade to manylinux_2_28 for aarch64 Python wheels by @kevinjqliu in https://github.com/apache/iceberg-rust/pull/975
* fix: Do not extract expression from cast to date by @omerhadari in https://github.com/apache/iceberg-rust/pull/977
* fix: TableMetadata `last_updated_ms` not increased for all operations by @c-thiel in https://github.com/apache/iceberg-rust/pull/978
* [infra] nightly pypi build for `pyiceberg_core` by @kevinjqliu in https://github.com/apache/iceberg-rust/pull/948
* [fix] nightly pypi build for `pyiceberg_core` by @kevinjqliu in https://github.com/apache/iceberg-rust/pull/983
* Check binary array length when applying truncate by @Fokko in https://github.com/apache/iceberg-rust/pull/984
* fix: speficy the version of munge for msrv check by @ZENOTME in https://github.com/apache/iceberg-rust/pull/987
* chore: fix edition 2024 compile errors by @xxchan in https://github.com/apache/iceberg-rust/pull/998
* refactor: Split schema module to multi file module by @xxchan in https://github.com/apache/iceberg-rust/pull/989
* make predicate accessor functions public by @Nathan-Fenner in https://github.com/apache/iceberg-rust/pull/1005
* fix: fix version of mechete by @ZENOTME in https://github.com/apache/iceberg-rust/pull/1006
* feat: View Metadata Builder by @c-thiel in https://github.com/apache/iceberg-rust/pull/908
* feat: Add `StrictMetricsEvaluator` by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/963
* feat: support `arrow_struct_to_iceberg_struct` by @ZENOTME in https://github.com/apache/iceberg-rust/pull/731
* chore(spec): add accessor methods for ManifestMetadata by @mnpw in https://github.com/apache/iceberg-rust/pull/1013
* feat: Pull Request Template by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/1009
* fix: upgrade spark version by @ZENOTME in https://github.com/apache/iceberg-rust/pull/1015
* feat: Add Issue Template by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/1008
* chore: Point user questions to github discussion by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/1016
* chore(deps): fix bump typos to v1.30.0 by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/1029
* ci(dependabot): Ignore all patch updates for iceberg-rust by @Xuanwo in https://github.com/apache/iceberg-rust/pull/1001
* feat: Add existing parquet files by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/960
* fix: Remove license from pull request template by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/1032
* chore(spell): skd -> sdk by @feniljain in https://github.com/apache/iceberg-rust/pull/1037
* fix(ci): fix audit break due to toolchain by @xxchan in https://github.com/apache/iceberg-rust/pull/1042
* feat: support delete if empty for parquet writer by @ZENOTME in https://github.com/apache/iceberg-rust/pull/838
* fix: kleene logic bug by @sdd in https://github.com/apache/iceberg-rust/pull/1045
* fix: upgrade ring to v0.17.13 fix Security audit by @ZENOTME in https://github.com/apache/iceberg-rust/pull/1050
* feat: implement display trait for Ident by @ZENOTME in https://github.com/apache/iceberg-rust/pull/1049
* feat: add construct_ref for table_metadata by @ZENOTME in https://github.com/apache/iceberg-rust/pull/1043
* refine: make commit more general by @ZENOTME in https://github.com/apache/iceberg-rust/pull/1048
* refactor: REST `Catalog` implementation by @connortsui20 in https://github.com/apache/iceberg-rust/pull/965
* chore: Ignore paste crate in rust crate audit check. by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/1063
* chore(deps): Bump either from 1.13.0 to 1.15.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/1060
* chore(deps): Bump tempfile from 3.17.1 to 3.18.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/1059
* fix: refine doc for write support by @ZENOTME in https://github.com/apache/iceberg-rust/pull/999
* Update dependabot to update lock file only by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/1068
* chore(deps): Bump crate-ci/typos from 1.30.0 to 1.30.2 by @dependabot in https://github.com/apache/iceberg-rust/pull/1069
* feat: Make duplicate check optional for adding parquet files by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/1034
* Make willingness to contribute in pr template a dropdown by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/1076
* refactor: Split transaction module by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/1080
* feat: Add conversion from `FileMetaData` to `ParquetMetadata` by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/1074
* Add context to `PopulatedDeleteFileIndex` by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/1084
* chore(deps): Bump tokio from 1.43.0 to 1.44.1 by @dependabot in https://github.com/apache/iceberg-rust/pull/1094
* chore(deps): Bump tempfile from 3.18.0 to 3.19.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/1093
* chore(deps): Bump http from 1.2.0 to 1.3.1 by @dependabot in https://github.com/apache/iceberg-rust/pull/1090
* chore(deps): Bump once_cell from 1.20.3 to 1.21.1 by @dependabot in https://github.com/apache/iceberg-rust/pull/1089
* chore(deps): Bump uuid from 1.13.2 to 1.16.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/1092
* chore(deps): Bump aws-config from 1.5.16 to 1.5.18 by @dependabot in https://github.com/apache/iceberg-rust/pull/1091
* feat: include spec id in DataFile by @ZENOTME in https://github.com/apache/iceberg-rust/pull/1098
* Enable discussion. by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/1103
* fix: fix delete files sequence comparison by @chenzl25 in https://github.com/apache/iceberg-rust/pull/1077
* fix(views): make timestamp take &self & fix set_current_version_id by @twuebi in https://github.com/apache/iceberg-rust/pull/1101
* Handle pagination via `next-page-token` in REST Catalog by @phillipleblanc in https://github.com/apache/iceberg-rust/pull/1097
* Support transforms with datetime timezones by @kevinjqliu in https://github.com/apache/iceberg-rust/pull/1086
* fix: Rust doc fix by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/1113
* feat: Add byte hint for fetching parquet metadata by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/1108
* fix: fix http custom headers for rest catalog by @chenzl25 in https://github.com/apache/iceberg-rust/pull/1010
* Scan Delete Support Part 2: introduce `DeleteFileManager` skeleton. Use in `ArrowReader` by @sdd in https://github.com/apache/iceberg-rust/pull/950
* feat: cache `calc_row_counts` by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/1107
* doc: add MSRV and dependency policy doc by @xxchan in https://github.com/apache/iceberg-rust/pull/1114
* refactor: Split `manifest` module into multiple modules by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/1119
* feat: Add `SnapshotSummaries` by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/1085
* refine: refine ManifestFile by @ZENOTME in https://github.com/apache/iceberg-rust/pull/1117
* fix: chore cargo lock and fix two warning for python bindings by @yihong0618 in https://github.com/apache/iceberg-rust/pull/1121
* chore(deps): Bump arrow-buffer from 54.2.0 to 54.3.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/1127
* Fix rounding of negative hour transform by @Fokko in https://github.com/apache/iceberg-rust/pull/1128
* chore(deps): Bump typed-builder from 0.20.0 to 0.20.1 by @dependabot in https://github.com/apache/iceberg-rust/pull/1125
* fix: safety ci using static check zizmor by @yihong0618 in https://github.com/apache/iceberg-rust/pull/1123
* chore(deps): Bump arrow-schema from 54.2.0 to 54.3.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/1126
* chore(deps): Bump rust_decimal from 1.36.0 to 1.37.1 by @dependabot in https://github.com/apache/iceberg-rust/pull/1124
* chore(catalog/rest): Add response headers in error for debug by @Xuanwo in https://github.com/apache/iceberg-rust/pull/1129
* chore: group `arrow*` and `parquet` dependabot updates by @mbrobbel in https://github.com/apache/iceberg-rust/pull/1132
* chore(deps): Bump the arrow-parquet group with 3 updates by @dependabot in https://github.com/apache/iceberg-rust/pull/1133
* Rename `pyiceberg_core` to `pyiceberg-core` by @Fokko in https://github.com/apache/iceberg-rust/pull/1134
* feat: Add merge summary and add manifest functionality to `SnapshotSummary` by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/1122
* refactor: Split scan module by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/1120
* feat: nan_value_counts support by @feniljain in https://github.com/apache/iceberg-rust/pull/907
* Remove `paste` dependency by expanding previously macro-generated code by @hendrikmakait in https://github.com/apache/iceberg-rust/pull/1138
* Remove deprecated code by @Fokko in https://github.com/apache/iceberg-rust/pull/1141
* Fix hour transform by @Fokko in https://github.com/apache/iceberg-rust/pull/1146
* chore(deps): Bump crate-ci/typos from 1.30.2 to 1.31.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/1147
* chore(deps): Bump the arrow-parquet group with 3 updates by @dependabot in https://github.com/apache/iceberg-rust/pull/1148
* Make `schema` and `partition_spec` optional for TableMetadataV1 by @phillipleblanc in https://github.com/apache/iceberg-rust/pull/1087
* fix(metadata): export iceberg schema in snapshots table by @xxchan in https://github.com/apache/iceberg-rust/pull/1135
* feat(puffin): Add PuffinWriter by @fqaiser94 in https://github.com/apache/iceberg-rust/pull/959
* doc: Clarify `arrow_schema_to_schema` requires fields with field id by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/1151
* doc: Add implementation status to `README` by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/1152
* feat: Support `TimestampNs` and TimestampTzNs` in bucket transform by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/1150
* refactor: simplify NestedField constructors  by @xxchan in https://github.com/apache/iceberg-rust/pull/1136
* feat: Add summary functionality to `SnapshotProduceAction` by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/1139
* fix: support empty scans by @danking in https://github.com/apache/iceberg-rust/pull/1166
* chore(deps): Bump crate-ci/typos from 1.31.0 to 1.31.1 by @dependabot in https://github.com/apache/iceberg-rust/pull/1171
* Scan Delete Support Part 3: `ArrowReader::build_deletes_row_selection` implementation by @sdd in https://github.com/apache/iceberg-rust/pull/951
* chore(deps): Bump tokio from 1.44.1 to 1.44.2 by @dependabot in https://github.com/apache/iceberg-rust/pull/1179
* chore(deps): Bump tokio from 1.43.0 to 1.44.2 in /bindings/python by @dependabot in https://github.com/apache/iceberg-rust/pull/1180
* feat: Infer partition values from bounds by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/1079
* fix: TableMetadata max sequence number validation by @c-thiel in https://github.com/apache/iceberg-rust/pull/1167
* Change tokio feature by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/1173
* refactor: Bump MSRV to 1.84 for preparing next release by @Xuanwo in https://github.com/apache/iceberg-rust/pull/1185
* refactor: Bump OpenDAL to 0.53 by @Xuanwo in https://github.com/apache/iceberg-rust/pull/1182
* ci: Use taplo to replace cargo-sort by @Xuanwo in https://github.com/apache/iceberg-rust/pull/1186
* refactor: Use tracing to replace log by @Xuanwo in https://github.com/apache/iceberg-rust/pull/1183
* feat(puffin): Make Puffin APIs public by @fqaiser94 in https://github.com/apache/iceberg-rust/pull/1165
* feat(io): add OSS storage implementation by @divinerapier in https://github.com/apache/iceberg-rust/pull/1153
* doc: `add_parquet_files` is not fully supported for version 0.5.0 by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/1187
* chore(deps): Bump crossbeam-channel from 0.5.14 to 0.5.15 by @dependabot in https://github.com/apache/iceberg-rust/pull/1190
* chore(deps): Bump crossbeam-channel from 0.5.14 to 0.5.15 in /bindings/python by @dependabot in https://github.com/apache/iceberg-rust/pull/1191
* refactor: use the same MSRV for datafusion integration by @xxchan in https://github.com/apache/iceberg-rust/pull/1197
* ci: optimize build space by @xxchan in https://github.com/apache/iceberg-rust/pull/1204
* refactor: iceberg::spec::values::Struct to remove bitvec by @xxchan in https://github.com/apache/iceberg-rust/pull/1203
* Add cli for iceberg by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/1194
* Add epic issue type by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/1200
* chore(deps): Bump roaring from `6cfeb88` to `9496afe` by @dependabot in https://github.com/apache/iceberg-rust/pull/1205
* doc: Clarify use of default map field name by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/1208
* chore(deps): Bump the arrow-parquet group across 1 directory with 2 updates by @dependabot in https://github.com/apache/iceberg-rust/pull/1206
* chore(deps): refine minimal deps by @xxchan in https://github.com/apache/iceberg-rust/pull/1209
* feat(cli): use fs_err to provide better err msg by @xxchan in https://github.com/apache/iceberg-rust/pull/1210
* feat(iceberg): introduce remove schemas by @Li0k in https://github.com/apache/iceberg-rust/pull/1115
* feat: support strict projection by @ZENOTME in https://github.com/apache/iceberg-rust/pull/946
* docs: fix typo in docstrings by @floscha in https://github.com/apache/iceberg-rust/pull/1219
* feat: Allow reuse http client in rest catalog by @Xuanwo in https://github.com/apache/iceberg-rust/pull/1221
* feat: Add trait for ObjectCache and ObjectCacheProvider by @Xuanwo in https://github.com/apache/iceberg-rust/pull/1222
* fix(catalog/rest): Using async lock in token to avoid blocking runtime by @Xuanwo in https://github.com/apache/iceberg-rust/pull/1223
* Introduce datafusion engine for sqllogictests. by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/1215
* fix: Update view version log timestamp for historical accuracy by @c-thiel in https://github.com/apache/iceberg-rust/pull/1218
* feat: Implement ObjectCache for moka by @Xuanwo in https://github.com/apache/iceberg-rust/pull/1225
* feat: re-export name mapping by @jdockerty in https://github.com/apache/iceberg-rust/pull/1116
* Skip producing empty parquet files by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/1230
* refactor: TableCreation::builder()::properties accept an `IntoIterator` by @drmingdrmer in https://github.com/apache/iceberg-rust/pull/1233
* feat: add apply in transaction to support stack action by @ZENOTME in https://github.com/apache/iceberg-rust/pull/949
* Add `equality_ids` to `FileScanTaskDeleteFile` by @sdd in https://github.com/apache/iceberg-rust/pull/1235
* refactor(s3tables): avoid misleading FileIO::from_path by @xxchan in https://github.com/apache/iceberg-rust/pull/1240
* [catalog] Fix namespace creation error status by @dentiny in https://github.com/apache/iceberg-rust/pull/1248
* [easy] Add comment on non-existent namespace/table at drop by @dentiny in https://github.com/apache/iceberg-rust/pull/1245
* fix(catalog/rest): Allow deserialize error with empty response by @Xuanwo in https://github.com/apache/iceberg-rust/pull/1266
* feat(core/catalog): Add more error kinds by @dentiny in https://github.com/apache/iceberg-rust/pull/1265
* chore: Pin roaring to released version by @Xuanwo in https://github.com/apache/iceberg-rust/pull/1269
* feat: Add deletion vector related fields in spec types by @dentiny in https://github.com/apache/iceberg-rust/pull/1276
* feat: support arrow dictionary in schema conversion by @jdockerty in https://github.com/apache/iceberg-rust/pull/1293
* chore(deps): Bump ring from 0.17.9 to 0.17.14 in /bindings/python by @dependabot in https://github.com/apache/iceberg-rust/pull/1309
* chore: define deletion vector type constant by @dentiny in https://github.com/apache/iceberg-rust/pull/1310
* chore: Add assertion for empty data files for append action by @dentiny in https://github.com/apache/iceberg-rust/pull/1301
* feat: expand arrow type conversion test by @jdockerty in https://github.com/apache/iceberg-rust/pull/1295
* chore(deps): Bump crate-ci/typos from 1.31.1 to 1.32.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/1292
* chore(deps): Bump tokio from 1.44.2 to 1.45.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/1312
* Fix predicates not matching the Arrow type of columns read from parquet files by @phillipleblanc in https://github.com/apache/iceberg-rust/pull/1308
* fix: Fix compilation failure when only storage-fs feature included by @dentiny in https://github.com/apache/iceberg-rust/pull/1304
* chore: minor update to manifest sanity check error message by @dentiny in https://github.com/apache/iceberg-rust/pull/1278
* chore: bump up arrow/parquet/datafusion by @sundy-li in https://github.com/apache/iceberg-rust/pull/1294
* fix: doc typo for `Schema.name_by_field_id()` by @burmecia in https://github.com/apache/iceberg-rust/pull/1321
* chore: declare `FileRead` trait to be `Sync`-safe by @dentiny in https://github.com/apache/iceberg-rust/pull/1319
* feat: Add API to set location in the transacation by @CTTY in https://github.com/apache/iceberg-rust/pull/1317
* feat: Add optional prefetch hint for parsing Puffin Footer by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/1207
* chore: Expose puffin blob constructor by @dentiny in https://github.com/apache/iceberg-rust/pull/1320
* Add doc for TableCommit by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/1263
* Expose datafusion table provider as python binding by @kevinjqliu in https://github.com/apache/iceberg-rust/pull/1324
* refactor: Add FileIO::remove_dir_all to deprecate remove_dir by @Xuanwo in https://github.com/apache/iceberg-rust/pull/1275
* chore: hms/glue catalog create table should respect default location by @sundy-li in https://github.com/apache/iceberg-rust/pull/1302
* ci: Fix python bindings rust code not checked by @Xuanwo in https://github.com/apache/iceberg-rust/pull/1338
* chore: Ignore .zed settings dir by @Xuanwo in https://github.com/apache/iceberg-rust/pull/1337
* feat: Add EncryptedKey struct by @c-thiel in https://github.com/apache/iceberg-rust/pull/1326
* fix: small typo for transaction test by @jdockerty in https://github.com/apache/iceberg-rust/pull/1341
* docs: fix typo in `expr.Reference` doc by @burmecia in https://github.com/apache/iceberg-rust/pull/1343
* Bump iceberg-rust version to 0.5.0 (Round 1) by @Xuanwo in https://github.com/apache/iceberg-rust/pull/1342
* chore(deps): Bump tempfile from 3.19.0 to 3.20.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/1351
* chore(deps): Bump aws-sdk-glue from 1.82.0 to 1.94.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/1350
* chore(deps): Bump aws-sdk-s3tables from 1.10.0 to 1.20.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/1349
* feat: expose `Error::backtrace()` by @xxchan in https://github.com/apache/iceberg-rust/pull/1352
* chore(deps): Bump the arrow-parquet group with 9 updates by @dependabot in https://github.com/apache/iceberg-rust/pull/1348

## [v0.4.0] - 2024-12-16
* io: add support for role arn and external id s3 props by @mattheusv in https://github.com/apache/iceberg-rust/pull/553
* fix: ensure S3 and GCS integ tests are conditionally compiled only when the storage-s3 and storage-gcs features are enabled by @sdd in https://github.com/apache/iceberg-rust/pull/552
* docs: fix main iceberg example by @jdockerty in https://github.com/apache/iceberg-rust/pull/554
* io: add support to set assume role session name by @mattheusv in https://github.com/apache/iceberg-rust/pull/555
* test: refactor datafusion test with memory catalog by @FANNG1 in https://github.com/apache/iceberg-rust/pull/557
* chore: add clean job in Makefile by @ChinoUkaegbu in https://github.com/apache/iceberg-rust/pull/561
* docs: Fix build website permission changed by @Xuanwo in https://github.com/apache/iceberg-rust/pull/564
* Object Cache: caches parsed Manifests and ManifestLists for performance by @sdd in https://github.com/apache/iceberg-rust/pull/512
* Update the paths by @Fokko in https://github.com/apache/iceberg-rust/pull/569
* docs: Add links for released crates by @Xuanwo in https://github.com/apache/iceberg-rust/pull/570
* Python: Use hatch for dependency management by @sungwy in https://github.com/apache/iceberg-rust/pull/572
* Ensure that RestCatalog passes user config to FileIO by @sdd in https://github.com/apache/iceberg-rust/pull/476
* Move `zlib` and `unicode` licenses to `allow` by @Fokko in https://github.com/apache/iceberg-rust/pull/566
* website: Update links for 0.3.0 by @Xuanwo in https://github.com/apache/iceberg-rust/pull/573
* feat(timestamp_ns): Implement timestamps with nanosecond precision by @Sl1mb0 in https://github.com/apache/iceberg-rust/pull/542
* fix: correct partition-id to field-id in UnboundPartitionField by @FANNG1 in https://github.com/apache/iceberg-rust/pull/576
* fix: Update sqlx from 0.8.0 to 0.8.1 by @FANNG1 in https://github.com/apache/iceberg-rust/pull/584
* chore(deps): Update typed-builder requirement from 0.19 to 0.20 by @dependabot in https://github.com/apache/iceberg-rust/pull/582
* Expose Transforms to Python Binding by @sungwy in https://github.com/apache/iceberg-rust/pull/556
* chore(deps): Bump crate-ci/typos from 1.23.6 to 1.24.1 by @dependabot in https://github.com/apache/iceberg-rust/pull/583
* Table Scan: Add Row Group Skipping by @sdd in https://github.com/apache/iceberg-rust/pull/558
* chore: bump crate-ci/typos to 1.24.3 by @sdlarsen in https://github.com/apache/iceberg-rust/pull/598
* feat: SQL Catalog - namespaces by @callum-ryan in https://github.com/apache/iceberg-rust/pull/534
* feat: Add more fields in FileScanTask by @Xuanwo in https://github.com/apache/iceberg-rust/pull/609
* chore(deps): Bump crate-ci/typos from 1.24.3 to 1.24.5 by @dependabot in https://github.com/apache/iceberg-rust/pull/616
* fix: Less Panics for Snapshot timestamps by @c-thiel in https://github.com/apache/iceberg-rust/pull/614
* feat: partition compatibility by @c-thiel in https://github.com/apache/iceberg-rust/pull/612
* feat: SortOrder methods should take schema ref if possible by @c-thiel in https://github.com/apache/iceberg-rust/pull/613
* feat: add `client.region` by @jdockerty in https://github.com/apache/iceberg-rust/pull/623
* fix: Correctly calculate highest_field_id in schema by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/590
* Feat: Normalize TableMetadata by @c-thiel in https://github.com/apache/iceberg-rust/pull/611
* refactor(python): Expose transform as a submodule for pyiceberg_core by @Xuanwo in https://github.com/apache/iceberg-rust/pull/628
* feat: support projection pushdown for datafusion iceberg by @FANNG1 in https://github.com/apache/iceberg-rust/pull/594
* chore: Bump opendal to 0.50 by @Xuanwo in https://github.com/apache/iceberg-rust/pull/634
* feat: add Sync to TransformFunction by @xxchan in https://github.com/apache/iceberg-rust/pull/638
* feat: expose arrow type <-> iceberg type by @xxchan in https://github.com/apache/iceberg-rust/pull/637
* doc: improve FileIO doc by @xxchan in https://github.com/apache/iceberg-rust/pull/642
* chore(deps): Bump crate-ci/typos from 1.24.5 to 1.24.6 by @dependabot in https://github.com/apache/iceberg-rust/pull/640
* Migrate to arrow-* v53 by @sdd in https://github.com/apache/iceberg-rust/pull/626
* feat: expose remove_all in FileIO by @xxchan in https://github.com/apache/iceberg-rust/pull/643
* feat (datafusion integration): convert datafusion expr filters to Iceberg Predicate by @a-agmon in https://github.com/apache/iceberg-rust/pull/588
* feat: Add NamespaceIdent.parent() by @c-thiel in https://github.com/apache/iceberg-rust/pull/641
* Table Scan: Add Row Selection Filtering by @sdd in https://github.com/apache/iceberg-rust/pull/565
* fix: compile error due to merge stale PR by @xxchan in https://github.com/apache/iceberg-rust/pull/646
* scan: change ErrorKind when table dont have spanshots by @mattheusv in https://github.com/apache/iceberg-rust/pull/608
* fix: avoid to create operator of memory storage every time by @ZENOTME in https://github.com/apache/iceberg-rust/pull/635
* feat (datafusion): making IcebergTableProvider public to be used without a catalog by @a-agmon in https://github.com/apache/iceberg-rust/pull/650
* test (datafusion): add test for table provider creation by @a-agmon in https://github.com/apache/iceberg-rust/pull/651
* fix: page index evaluator min/max args inverted by @sdd in https://github.com/apache/iceberg-rust/pull/648
* chore: fix typo in FileIO Schemes  by @wcy-fdu in https://github.com/apache/iceberg-rust/pull/653
* fix: TableUpdate Snapshot deserialization for v1 by @c-thiel in https://github.com/apache/iceberg-rust/pull/656
* feat: Reassign field ids for schema by @c-thiel in https://github.com/apache/iceberg-rust/pull/615
* feat: add gcp oauth support by @twuebi in https://github.com/apache/iceberg-rust/pull/654
* fix(arrow): Use new ParquetMetaDataReader instead by @Xuanwo in https://github.com/apache/iceberg-rust/pull/661
* chore(deps): bump typos crate to 1.25.0 by @matthewwillian in https://github.com/apache/iceberg-rust/pull/662
* RecordBatchTransformer: Handle schema migration and column re-ordering in table scans by @sdd in https://github.com/apache/iceberg-rust/pull/602
* docs: installation of the new `iceberg_catalog_rest` added to the docs by @nishant-sachdeva in https://github.com/apache/iceberg-rust/pull/355
* feat(datafusion): Support pushdown more datafusion exprs to Iceberg by @FANNG1 in https://github.com/apache/iceberg-rust/pull/649
* feat: Derive PartialEq for FileScanTask by @Xuanwo in https://github.com/apache/iceberg-rust/pull/660
* feat: SQL Catalog - Tables by @callum-ryan in https://github.com/apache/iceberg-rust/pull/610
* ci: Allow install a non-debian-packaged Python package by @Xuanwo in https://github.com/apache/iceberg-rust/pull/666
* docs: README uses iceberg-rust instead of we by @caicancai in https://github.com/apache/iceberg-rust/pull/667
* chore(deps): Bump crate-ci/typos from 1.25.0 to 1.26.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/668
* feat: Add equality delete writer by @Dysprosium0626 in https://github.com/apache/iceberg-rust/pull/372
* Revert "feat: Add equality delete writer (#372)" by @Xuanwo in https://github.com/apache/iceberg-rust/pull/672
* ci: Fix CI for bindings python by @Xuanwo in https://github.com/apache/iceberg-rust/pull/678
* fix: OpenDAL `is_exist` => `exists` by @sdd in https://github.com/apache/iceberg-rust/pull/680
* feat: Expose ManifestEntry status by @zheilbron in https://github.com/apache/iceberg-rust/pull/681
* feat: allow empty projection in table scan by @sundy-li in https://github.com/apache/iceberg-rust/pull/677
* chore(deps): Bump crate-ci/typos from 1.26.0 to 1.26.8 by @dependabot in https://github.com/apache/iceberg-rust/pull/683
* fix: bump parquet minor version by @xxchan in https://github.com/apache/iceberg-rust/pull/684
* fix(type): fix type promote to ignore field name. by @chenzl25 in https://github.com/apache/iceberg-rust/pull/685
* feat: implement IcebergTableProviderFactory for datafusion by @yukkit in https://github.com/apache/iceberg-rust/pull/600
* feat: Safer PartitionSpec & SchemalessPartitionSpec by @c-thiel in https://github.com/apache/iceberg-rust/pull/645
* chore(deps): Bump crate-ci/typos from 1.26.8 to 1.27.0 by @dependabot in https://github.com/apache/iceberg-rust/pull/687
* feat: TableMetadata accessors for current ids of Schema, Snapshot and SortOrder by @c-thiel in https://github.com/apache/iceberg-rust/pull/688
* chore: upgrade to DataFusion 43 by @gruuya in https://github.com/apache/iceberg-rust/pull/691
* chore(deps): Bump crate-ci/typos from 1.27.0 to 1.27.3 by @dependabot in https://github.com/apache/iceberg-rust/pull/693
* feat: Expose length of Iterators by @c-thiel in https://github.com/apache/iceberg-rust/pull/692
* feat: Implement TableRequirement checks by @c-thiel in https://github.com/apache/iceberg-rust/pull/689
* feat: Add ViewUpdate to catalog by @c-thiel in https://github.com/apache/iceberg-rust/pull/690
* chore: update .asf.yaml by @c-thiel in https://github.com/apache/iceberg-rust/pull/701
* datafusion: Create table provider for a snapshot. by @ryzhyk in https://github.com/apache/iceberg-rust/pull/707
* Add Python Release Action to publish `pyiceberg_core` dist to Pypi by @sungwy in https://github.com/apache/iceberg-rust/pull/705
* chore: Mark `last-field-id` as deprecated by @Fokko in https://github.com/apache/iceberg-rust/pull/715
* TableMetadataBuilder by @c-thiel in https://github.com/apache/iceberg-rust/pull/587
* Add `fallback` attribute to all `strip_option`s. by @ryzhyk in https://github.com/apache/iceberg-rust/pull/708
* fix: Remove check of last_column_id by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/717
* Fix error running data fusion queries - Physical input schema should be the same as the one converted from logical input schema by @FANNG1 in https://github.com/apache/iceberg-rust/pull/664
* chore: Typo in test :) by @Fokko in https://github.com/apache/iceberg-rust/pull/727
* Derive Clone for IcebergTableProvider by @SergeiPatiakin in https://github.com/apache/iceberg-rust/pull/722
* fix: expand arrow to iceberg schema to handle nanosecond timestamp by @jdockerty in https://github.com/apache/iceberg-rust/pull/710
* feat: Add equality delete writer by @ZENOTME in https://github.com/apache/iceberg-rust/pull/703
* chore: Bump upload-artifact@v3 to v4 by @sungwy in https://github.com/apache/iceberg-rust/pull/725
* feat: support append data file and add e2e test by @ZENOTME in https://github.com/apache/iceberg-rust/pull/349
* chore(deps): Bump actions/setup-python from 4 to 5 by @dependabot in https://github.com/apache/iceberg-rust/pull/746
* chore: Align argument name with doc comment by @SergeiPatiakin in https://github.com/apache/iceberg-rust/pull/750
* fix: equality delete writer field id project by @ZENOTME in https://github.com/apache/iceberg-rust/pull/751
* feat: expose opendal S3 options for anonymous access by @gruuya in https://github.com/apache/iceberg-rust/pull/757
* fix: current-snapshot-id serialized to -1 in TableMetadata.json by @c-thiel in https://github.com/apache/iceberg-rust/pull/755
* feat(puffin): Add Puffin crate and CompressionCodec by @fqaiser94 in https://github.com/apache/iceberg-rust/pull/745
* Build: Delete branch automatically on PR merge by @manuzhang in https://github.com/apache/iceberg-rust/pull/764
* chore(deps): bump crate-ci/typos from 1.27.3 to 1.28.1 by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/769
* chore(deps): Bump crate-ci/typos from 1.27.3 to 1.28.2 by @dependabot in https://github.com/apache/iceberg-rust/pull/767
* Add Spark for integration tests by @Fokko in https://github.com/apache/iceberg-rust/pull/766
* refine: refine writer interface by @ZENOTME in https://github.com/apache/iceberg-rust/pull/741
* Clean up docker Docker by @Fokko in https://github.com/apache/iceberg-rust/pull/770
* name mapping serde by @barronw in https://github.com/apache/iceberg-rust/pull/740
* docker: The `archive` seems unstable by @Fokko in https://github.com/apache/iceberg-rust/pull/773
* doc: add RisingWave to users by @xxchan in https://github.com/apache/iceberg-rust/pull/775
* feat: Expose disable_config_load opendal S3 option by @gruuya in https://github.com/apache/iceberg-rust/pull/782
* Support conversion of Arrow Int8 and Int16 to Iceberg Int by @gruuya in https://github.com/apache/iceberg-rust/pull/787
* infra: Dismiss stale reviews by @liurenjie1024 in https://github.com/apache/iceberg-rust/pull/779
* fix: return type for year and month transform should be int by @xxchan in https://github.com/apache/iceberg-rust/pull/776
* feat: Allow for schema evolution by @Fokko in https://github.com/apache/iceberg-rust/pull/786
* Retry object store reads on temporary errors. by @ryzhyk in https://github.com/apache/iceberg-rust/pull/788
* refactor(puffin): Move puffin crate contents inside iceberg crate by @fqaiser94 in https://github.com/apache/iceberg-rust/pull/789
* feat: Implement Decimal from/to bytes represents by @Xuanwo in https://github.com/apache/iceberg-rust/pull/665
* feat: eagerly project the arrow schema to scope out non-selected fields by @gruuya in https://github.com/apache/iceberg-rust/pull/785
* fix: wrong compute of partitions in manifest by @ZENOTME in https://github.com/apache/iceberg-rust/pull/794
* fix: set key_metadata to Null by default by @feniljain in https://github.com/apache/iceberg-rust/pull/800
* test: append partition data file by @feniljain in https://github.com/apache/iceberg-rust/pull/742
* chore: Add more debug message inside error by @Xuanwo in https://github.com/apache/iceberg-rust/pull/793
* fix: Error source from cache has been shadowed by @Xuanwo in https://github.com/apache/iceberg-rust/pull/792
* fix(catalog/rest): Ensure token been reused correctly by @Xuanwo in https://github.com/apache/iceberg-rust/pull/801
* feat!: Remove `BoundPartitionSpec` by @c-thiel in https://github.com/apache/iceberg-rust/pull/771
* chore(deps): Bump crate-ci/typos from 1.28.2 to 1.28.3 by @dependabot in https://github.com/apache/iceberg-rust/pull/805
* feat: add `DataFileWriter` tests for schema and partition by @jonathanc-n in https://github.com/apache/iceberg-rust/pull/768
* fix: day transform compute by @ZENOTME in https://github.com/apache/iceberg-rust/pull/796
* feat: TableMetadata Statistic Files by @c-thiel in https://github.com/apache/iceberg-rust/pull/799
* Bump `pyiceberg_core` to 0.4.0 by @sungwy in https://github.com/apache/iceberg-rust/pull/808
* chore(docs): Update Readme - Lakekeeper repository moved by @c-thiel in https://github.com/apache/iceberg-rust/pull/810
* Prep 0.4.0 release by @sungwy in https://github.com/apache/iceberg-rust/pull/809
* feat: Add RemovePartitionSpecs table update by @c-thiel in https://github.com/apache/iceberg-rust/pull/804
* feat: Store file io props to allow re-build it by @Xuanwo in https://github.com/apache/iceberg-rust/pull/802
* chore: chmod +x on `verify.py` script by @sungwy in https://github.com/apache/iceberg-rust/pull/817
* fix: Reading a table with positional deletes should fail by @Fokko in https://github.com/apache/iceberg-rust/pull/826

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
* refine: separate parquet reader and arrow convert by @ZENOTME in https://github.com/apache/iceberg-rust/pull/313
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
* refactor(catalog/rest): Split http client logic to separate mod by @Xuanwo in https://github.com/apache/iceberg-rust/pull/423
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
* feat: Partition Binding and safe PartitionSpecBuilder by @c-thiel in https://github.com/apache/iceberg-rust/pull/491

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
* feat: support read/write Manifest by @ZENOTME in https://github.com/apache/iceberg-rust/pull/79
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
* fix: default_partition_spec using the partition_spec_id set by @odysa in https://github.com/apache/iceberg-rust/pull/190
* Docs: Add required Cargo version to install guide by @manuzhang in https://github.com/apache/iceberg-rust/pull/191
* chore(deps): Update opendal requirement from 0.44 to 0.45 by @dependabot in https://github.com/apache/iceberg-rust/pull/195

[v0.3.0]: https://github.com/apache/iceberg-rust/compare/v0.2.0...v0.3.0
