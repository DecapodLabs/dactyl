# dactyl

Governed datastore boundary for [Decapod](https://github.com/DecapodLabs/decapod).

Dactyl routes the same query string to either a local SQLite adapter or a
remote Neon-backed adapter without changing application logic. It analyzes
SQL before execution, rejects unsupported dialect constructs when rewriting
is disabled, and applies permitted rewrites when enabled. The public API is
intentionally narrow — `read` and `write` are the only callable functions:

```rust
use dactyl::{self, Rows};

fn main() -> Result<(), dactyl::DactylError> {
    let rows: Rows = dactyl::read("select id, title, status from todos", true)?;
    for r in rows { println!("{:?}", r); }
    Ok(())
}
```

There is no `init`. The first call establishes the connection based on env
configuration (see [How dactyl picks the adapter](#how-dactyl-picks-the-adapter)).

## Public surface

- `dactyl::read(query: &str, optimize: bool) -> Result<Rows, DactylError>`
- `dactyl::write(query: &str, optimize: bool) -> Result<Rows, DactylError>`
- `dactyl::query!("...")` — compile-time-analyzed query macro
- `Rows`, `Row`, `DactylError` (result / error types)

Nothing else is exported at the crate root.

## How dactyl picks the adapter

Adapter selection is configured via two environment variables:

- `DATASTORE` — set to `"sqlite"` or `"neon"`.
- `DATASTORE_ROUTE` — when `DATASTORE` is `"sqlite"`, this is the path to the SQLite file. When `DATASTORE` is `"neon"`, this is the Propodus endpoint URL.
- `DATASTORE_TOKEN` — optional auth token for the Neon adapter.

For backwards compatibility, dactyl also supports legacy environment variables (`DACTYL_NEON_ENDPOINT`, `DACTYL_NEON_BEARER`, `DACTYL_SQLITE_PATH`, `DACTYL_SQLITE_ROOT`).

## References

- DecapodLabs/decapod#1110 — datastore boundary spec
- DecapodLabs/decapod#1111 — migration plan
- DecapodLabs/decapod#1112 — adapter ownership
- DecapodLabs/dactyl#1 — bootstrap crate

## License

MIT.
