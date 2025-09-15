Local fork of apache/iceberg-rust
=================================

Branching:
- Upstream: https://github.com/apache/iceberg-rust
- Working branch: feature/df49-upgrade (tracks DF 49 / Arrow 55 integration for iceberg-datafusion)

Crates used by DataFusion server:
- iceberg
- iceberg-catalog-rest
- iceberg-datafusion

Dev workflow:
1) Make changes in this fork (branch feature/df49-upgrade).
2) From docker/dockerfiles/datafusion, cargo will use [patch.crates-io] with local path.
3) Once ready, push to a remote VCIntel fork and switch Cargo.toml to a git patch if desired.

Targets:
- Align with DataFusion 49.0.* / Arrow 55.*.
- Update iceberg-datafusion to new DF49 CatalogProvider/TableProvider APIs.
- Keep object_store versions compatible with DF line.
- Add minimal unit tests without network access.

