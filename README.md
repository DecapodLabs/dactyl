# dactyl-db

[![Crates.io](https://img.shields.io/crates/v/dactyl-db.svg)](https://crates.io/crates/dactyl-db)
[![Docs.rs](https://docs.rs/dactyl-db/badge.svg)](https://docs.rs/dactyl-db)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

`dactyl-db` is a lightweight, zero-configuration Rust library for interchanging reads and writes between a local SQLite database and a cloud-hosted Neon (PostgreSQL) instance on the fly, behind a single unified facade.

It analyzes SQL queries before execution, rejects unsupported dialect constructs when optimizations are disabled, and applies dialect-specific query rewrites when enabled.

## Key Features

- 🎯 **Unified Facade**: Write queries once. Execute them interchangeably on local SQLite or cloud Neon (Postgres) instances without changing application logic.
- ⚡ **Zero Configuration**: No `init` step. The first database call lazily and thread-safely establishes the connection cache.
- 🛠️ **Dialect rewrites & safety**: Protects database calls by checking dialect compatibility, rejecting incompatible queries, and translating queries when `optimize = true`.
- 🔍 **Compile-time validation**: Catch query syntax errors and dialect mismatches at compile time using the `query!` macro.

## Quick Start

Add `dactyl-db` to your `Cargo.toml`:

```toml
[dependencies]
dactyl-db = { version = "0.1.1", features = ["sqlite", "neon"] }
```

### Usage Example

```rust
use dactyl_db::{self, Rows};

fn main() -> Result<(), dactyl_db::DactylError> {
    // Zero-config: the first call dynamically establishes the datastore connection
    let rows: Rows = dactyl_db::read("select id, title, status from todos", true)?;
    
    for row in rows {
        let id: i64 = row.get("id")?;
        let title: &str = row.get("title")?;
        println!("Todo #{}: {}", id, title);
    }
    
    Ok(())
}
```

## How dactyl-db selects the adapter

`dactyl-db` inspects the environment on the first query invocation to determine where to route read/write calls:

| Environment Variable | Allowed Values / Format | Description |
|---|---|---|
| `DATASTORE` | `"sqlite"` or `"neon"` | Sets the target datastore adapter. |
| `DATASTORE_ROUTE` | SQLite filepath OR Neon endpoint URL | Connection route for the selected adapter. |
| `DATASTORE_TOKEN` | Opaque string | Optional authentication token for the Neon adapter. |

*Note: For backwards compatibility, legacy variables (`DACTYL_NEON_ENDPOINT`, `DACTYL_NEON_BEARER`, `DACTYL_SQLITE_PATH`, `DACTYL_SQLITE_ROOT`) are supported as fallbacks.*

### Datastore Translation & Optimization

- **`optimize = true`**: Allows `dactyl-db` to parse and rewrite target-specific queries to be native to the active datastore.
- **`optimize = false`**: Disables rewrites. If a query contains syntax that is unsupported by the target datastore, the call immediately fails with a `DactylError::Unsupported` error.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
