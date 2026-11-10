# Native RPC candidate and release handoff

The additive transport implements the shared Supabase RPC v1 contract while
preserving SQLite, Neon and existing Supabase service routes. Required proof
covers all feature combinations, formatting, clippy, typed transport boundaries,
Rust 1.75 locked tests and a clean consumer without the library lockfile.

At resumption on 2026-11-09 the registry release remained 0.11.1 and no native
RPC release existed. The prior temporary candidate c37dceb lost its Git tree
through temporary-directory cleanup; its validation had stopped before Rust
checks. This reconstruction preserves that uncertainty and claims no prior pass.

The repository requires container isolation. The published controller 0.136.0
currently references an unavailable `v0.136.0-debian` image. The explicit-image
`decapod auto container run` workflow can use `tools/proof/Containerfile`, which
builds the published 0.136.0 CLI from crates.io on pinned Rust 1.96.1 and installs
Rust 1.75.0. This is a local proof image, not an uploaded release image.

No PostgreSQL, hosted Supabase, clean published-consumer or registry handoff is
implied by synthetic transport tests. Those profiles need separately recorded
execution. The existing release-please workflow and protected release environment
retain GitHub App/registry credential ownership. After exact-candidate validation
and review, use that normal process and verify the actual registry artifact before
updating consumer dependencies. Temporary path dependencies are development only.

The Linux proof environment must support the SQLite recovery preconditions.
Podman SELinux labels cause the existing recovery contract to refuse files;
Apple’s headless Linux runtime supplies an unlabeled filesystem without changing
that security policy. The complete profile pins Rust and required tools, exercises
the metadata regression test, and lets Cargo 1.75 resolve a clean active consumer
graph online rather than treating a newer Cargo lockfile as compatibility proof.
Optional HTTP dependency bounds preserve the declared minimum without enabling
QUIC, adding another transport, or changing non-HTTP feature builds.
