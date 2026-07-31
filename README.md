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

| Env var | Effect |
|---|---|
| `DACTYL_NEON_ENDPOINT` | Set → Neon adapter. Uses `DACTYL_NEON_BEARER` if present. |
| `DACTYL_SQLITE_PATH` | Set → path to the SQLite file. |
| `DACTYL_SQLITE_ROOT` | Optional default root for per-store SQLite paths (default: `.decapod/data`). |

When neither is set, dactyl defaults to the SQLite adapter and derives a
per-store path from the first `from <name>` clause in the query
(`.decapod/data/<store>.db`).

## References

- DecapodLabs/decapod#1110 — datastore boundary spec
- DecapodLabs/decapod#1111 — migration plan
- DecapodLabs/decapod#1112 — adapter ownership
- DecapodLabs/dactyl#1 — bootstrap crate

## License

MIT.
