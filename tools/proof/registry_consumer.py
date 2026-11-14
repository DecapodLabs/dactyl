"""Prepare a registry-only consumer of the immutable first control-RPC release.

Preparation resolves/downloads dependencies only. check.py owns execution of the
shipped public-boundary tests. Neither the library nor tests come from this Git
checkout. This is synthetic transport compatibility, never hosted authorization.
"""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib

VERSION = "0.17.0"
CHECKSUM = "15face02da6f675d5784497a7ddcb885de14c1d09e2173c14df83c0aadd74abd"
TEST_SHA256 = "35c7fddd2fce9204c0c15fd851f53bdb244a731b41c4e5c935b7fcb25f27b9c7"
REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"
TEST_PATH = "tests/control_rpc.rs"


@contextmanager
def registry_control_consumer(tools, env):
    with tempfile.TemporaryDirectory(prefix="dactyl-registry-control-") as directory:
        root = Path(directory)
        (root / "src").mkdir()
        (root / "src/lib.rs").write_text("// Registry transport qualification only.\n")
        (root / "Cargo.toml").write_text('''[package]
name = "released-control-consumer"
version = "0.0.0"
edition = "2021"
rust-version = "1.85"
[workspace]
resolver = "2"
[features]
default = ["supabase"]
supabase = []
[dependencies]
dactyl-db = { version = "=0.17.0", features = ["supabase"] }
serde_json = "1"
[dev-dependencies]
tokio = { version = ">=1.0, <1.54", features = ["full"] }
''')
        result = subprocess.run(
            [str(tools / "cargo"), "metadata", "--format-version", "1",
             "--manifest-path", str(root / "Cargo.toml")],
            env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=900)
        if result.returncode:
            # Registry diagnostics can contain machine configuration; keep the
            # failure bounded and do not forward arbitrary Cargo output.
            raise RuntimeError("registry control consumer resolution failed")
        metadata = json.loads(result.stdout)
        packages = [p for p in metadata["packages"] if p["name"] == "dactyl-db"]
        locked = [p for p in tomllib.loads((root / "Cargo.lock").read_text())["package"]
                  if p["name"] == "dactyl-db"]
        if len(packages) != 1 or len(locked) != 1:
            raise RuntimeError("registry control consumer package identity is ambiguous")
        package, lock = packages[0], locked[0]
        if (package["version"] != VERSION or package["source"] != REGISTRY
                or lock["version"] != VERSION or lock.get("source") != REGISTRY
                or lock.get("checksum") != CHECKSUM):
            raise RuntimeError("registry control consumer source/version/checksum mismatch")
        source = Path(package["manifest_path"]).parent
        # Verify the actual cached registry archive as well as Cargo's lock.
        archive = (source.parent.parent.parent / "cache" / source.parent.name
                   / f"dactyl-db-{VERSION}.crate")
        if hashlib.sha256(archive.read_bytes()).hexdigest() != CHECKSUM:
            raise RuntimeError("registry control archive checksum mismatch")
        contract = source / TEST_PATH
        if hashlib.sha256(contract.read_bytes()).hexdigest() != TEST_SHA256:
            raise RuntimeError("shipped registry control test contract mismatch")
        (root / "tests").mkdir()
        shutil.copyfile(contract, root / TEST_PATH)
        identity = dict(version=VERSION, source=REGISTRY, checksum=CHECKSUM,
                        test_sha256=TEST_SHA256, profile="synthetic_http_registry_consumer")
        yield root, identity
