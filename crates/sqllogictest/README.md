This crate contains a suite of [sqllogictest](https://crates.io/crates/sqllogictest) tests that are used to validate [iceberg-rust](https://github.com/apache/iceberg-rust).

## Running the tests

Just run the following command:

```bash
cargo test
```

## Sql Engines

The tests are run against the following sql engines:

* [Apache datafusion](https://crates.io/crates/datafusion)
* [Apache spark](https://github.com/apache/spark)