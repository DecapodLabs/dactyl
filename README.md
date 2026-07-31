# dactyl

Governed datastore boundary for [Decapod](https://github.com/DecapodLabs/decapod).

Dactyl routes the same query string to either a local SQLite adapter or a remote Neon-backed
adapter without changing application logic. It analyzes SQL before execution, rejects
unsupported dialect constructs when rewriting is disabled, and applies permitted rewrites
when enabled. The public API is intentionally narrow:

```rust
use dactyl::{self, DactylConfig, DactylError, Rows};

fn main() -> Result<(), DactylError> {
    dactyl::init(DactylConfig::sqlite(".decapod/data/todo.db"))?;
    let rows: Rows = dactyl::read("sqlite", "select id, title from todos", true)?;
    for r in rows { println!("{:?}", r); }
    Ok(())
}
```

## Public surface

- `dactyl::init(cfg) -> Result<(), DactylError>`
- `dactyl::active_datastore() -> &'static str`
- `dactyl::read(datastore: &str, query: &str, optimize: bool) -> Result<Rows, DactylError>`
- `dactyl::write(datastore: &str, query: &str, optimize: bool) -> Result<Rows, DactylError>`
- `dactyl::query!("...")` — compile-time-analyzed query macro

Nothing else is exported at the crate root.

## References

- DecapodLabs/decapod#1110 — datastore boundary spec
- DecapodLabs/decapod#1111 — migration plan
- DecapodLabs/decapod#1112 — adapter ownership
- DecapodLabs/dactyl#1 — bootstrap crate

## License

MIT.
