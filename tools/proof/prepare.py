"""Fetch proof dependencies without compiling or running candidate checks.

The image has fixed Cargo 1.96.1 and 1.75 toolchains. Their sparse crates.io
registry namespaces differ. Modern Cargo fetches the complete graph, including
inactive HTTP/3 packages whose edition Cargo 1.75 cannot parse during fetch.
The older Cargo can then resolve/build its active graph from the same public
index and archives offline. No lockfile or candidate source is changed.
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


ROOT = Path.cwd()
STABLE = Path(os.environ["DACTYL_STABLE_TOOLS"])
CARGO_HOME = Path(os.environ.get("CARGO_HOME", "/usr/local/cargo"))
env = dict(os.environ, RUSTC=str(STABLE / "rustc"),
           RUSTDOC=str(STABLE / "rustdoc"), CARGO_NET_OFFLINE="false",
           CARGO_REGISTRIES_CRATES_IO_PROTOCOL="sparse")


def fetch(manifest, locked=False):
    subprocess.run([str(STABLE / "cargo"), "fetch", "--manifest-path", str(manifest)] +
                   (["--locked"] if locked else []), env=env, check=True, timeout=900)


fetch(ROOT / "Cargo.toml", locked=True)
# check.py deliberately resolves a fresh consumer without the library lockfile.
# Warm that resolution too, without retaining a lockfile that could mask it.
with tempfile.TemporaryDirectory(prefix="dactyl-consumer-prepare-") as directory:
    consumer = Path(directory)
    (consumer / "src").mkdir()
    (consumer / "src/main.rs").write_text("fn main() {}\n")
    (consumer / "Cargo.toml").write_text(
        '[package]\nname="clean-consumer"\nversion="0.0.0"\nedition="2021"\nrust-version="1.75"\n'
        '[dependencies]\ndactyl-db={path=' + json.dumps(str(ROOT)) +
        ',features=["sqlite","neon","supabase"]}\n')
    fetch(consumer / "Cargo.toml")

for kind in ("index", "cache"):
    registry = CARGO_HOME / "registry" / kind
    modern = registry / "index.crates.io-1949cf8c6b5b557f"
    legacy = registry / "index.crates.io-6f17d22bba15001f"
    if not modern.is_dir():
        raise SystemExit("expected Cargo 1.96.1 sparse crates.io cache is unavailable")
    shutil.copytree(modern, legacy, dirs_exist_ok=True)
print("Prepared locked and clean-consumer dependencies for offline validation.", flush=True)
