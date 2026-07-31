#!/usr/bin/env bash
set -euo pipefail

# This script publishes dactyl_macros and dactyl to crates.io in the correct order.
# Requires CARGO_REGISTRY_TOKEN env var or cargo to be pre-authenticated.

echo "=== Verifying dactyl_macros ==="
cargo publish --manifest-path dactyl_macros/Cargo.toml --dry-run

echo "=== Verifying dactyl ==="
cargo publish --manifest-path Cargo.toml --dry-run

echo "=== Publishing dactyl_macros ==="
if [ -n "${CARGO_REGISTRY_TOKEN:-}" ]; then
  cargo publish --manifest-path dactyl_macros/Cargo.toml --token "${CARGO_REGISTRY_TOKEN}"
else
  cargo publish --manifest-path dactyl_macros/Cargo.toml
fi

echo "=== Waiting for crates.io index propagation (30s) ==="
sleep 30

echo "=== Publishing dactyl ==="
if [ -n "${CARGO_REGISTRY_TOKEN:-}" ]; then
  cargo publish --manifest-path Cargo.toml --token "${CARGO_REGISTRY_TOKEN}"
else
  cargo publish --manifest-path Cargo.toml
fi

echo "=== Success! ==="
