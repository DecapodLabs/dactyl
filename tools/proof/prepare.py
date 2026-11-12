"""Fetch proof dependencies without compiling or running candidate checks.

The image has fixed Cargo 1.96.1 and 1.85 toolchains. Stable Cargo fetches the
locked graph and warms a separate lock-free consumer resolution before the
required offline checks. No candidate lockfile or source is changed.
"""
import json
import os
from pathlib import Path
import subprocess
import tempfile


ROOT = Path.cwd()
STABLE = Path(os.environ["DACTYL_STABLE_TOOLS"])
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
        '[package]\nname="clean-consumer"\nversion="0.0.0"\nedition="2021"\nrust-version="1.85"\n'
        '[dependencies]\ndactyl-db={path=' + json.dumps(str(ROOT)) +
        ',features=["sqlite","neon","supabase"]}\n')
    fetch(consumer / "Cargo.toml")

print("Prepared locked and clean-consumer dependencies for offline validation.", flush=True)
