"""Acquire public Cargo dependencies without compiling or executing proof.

Run in the admitted container before native validation. The proof itself is
offline and does not call this script. This step makes no readiness claim.
"""

import json
import os
from pathlib import Path
import subprocess
import tempfile


ROOT = Path.cwd()
STABLE = Path(os.environ["DACTYL_STABLE_TOOLS"])
MSRV = Path(os.environ["DACTYL_MSRV_TOOLS"])


def fetch(tools, args):
    env = dict(
        os.environ,
        RUSTC=str(tools / "rustc"),
        RUSTDOC=str(tools / "rustdoc"),
        RUSTUP_TOOLCHAIN=tools.parent.name,
        CARGO_NET_OFFLINE="false",
        CARGO_TERM_COLOR="never",
    )
    subprocess.run([str(tools / "cargo"), "fetch", *args], env=env, check=True)


fetch(STABLE, ["--locked"])
fetch(MSRV, ["--locked"])
with tempfile.TemporaryDirectory(prefix="dactyl-consumer-prepare-") as directory:
    consumer = Path(directory)
    (consumer / "src").mkdir()
    (consumer / "src/main.rs").write_text(
        "fn main() { let _ = dactyl_db::Backend::Cloud; }\n", encoding="utf-8"
    )
    (consumer / "Cargo.toml").write_text(
        '[package]\nname="clean-consumer"\nversion="0.0.0"\nedition="2021"\nrust-version="1.75"\n'
        '[dependencies]\ndactyl-db={path=' + json.dumps(str(ROOT))
        + ',features=["sqlite","neon","supabase"]}\n',
        encoding="utf-8",
    )
    fetch(MSRV, ["--manifest-path", str(consumer / "Cargo.toml")])
print("Public dependency preparation completed; no proof was executed.", flush=True)
