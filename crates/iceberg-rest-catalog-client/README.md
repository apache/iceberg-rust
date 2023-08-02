# Rust API client for openapi

Defines the specification for the first version of the REST Catalog API. Implementations should ideally support both Iceberg table specs v1 and v2, with priority given to v2.

## Installation

Put the package under your project folder in a directory named `openapi` and add the following to `Cargo.toml` under `[dependencies]`:

```
openapi = { path = "./openapi" }
```

## Contributing

To regenerate the client, run the following command:

```shell
export API_VERSION=1.3.1

curl "https://raw.githubusercontent.com/apache/iceberg/apache-iceberg-${API_VERSION}/open-api/rest-catalog-open-api.yaml" -o rest-catalog-open-api.yaml
openapi-generator generate \
    -g rust \
    -i rest-catalog-open-api.yaml \
    -o .
```

NOTES:

- `API_VERSION` should be the latest released version of iceberg.
- `PACKAGE_VERSION` should be changed based on need.
- `openapi-generator` doesn't respect `HTTPS_PROXY`, users behind a proxy should use `curl` to download specs locally instead.

After generation, please use `cargo fmt` to make sure the code is formatted.
