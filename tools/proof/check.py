"""Complete reviewed local Rust contract; invoked by decapod validate.

--prepare only resolves/downloads dependencies, never compiles or tests Dactyl.
Real-service/hosted proof and publication remain separate explicit operations.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path.cwd()
STABLE = Path(os.environ.get("DACTYL_STABLE_TOOLS", "/usr/local/rustup/toolchains/1.96.1-aarch64-unknown-linux-gnu/bin"))
MSRV = Path(os.environ.get("DACTYL_MSRV_TOOLS", "/usr/local/rustup/toolchains/1.75.0-aarch64-unknown-linux-gnu/bin"))
checks = []


def environment(tools, offline=True):
    env = dict(os.environ)
    env.update(RUSTC=str(tools / "rustc"), RUSTDOC=str(tools / "rustdoc"),
               PATH=str(tools) + ":/opt/dactyl-proof-tools", CARGO_NET_OFFLINE=str(offline).lower())
    env.pop("DACTYL_SUPABASE_FIXTURE", None)
    env.pop("DACTYL_SUPABASE_HOSTED_PROOF", None)
    return env


def run(name, args, tools=STABLE, expected_missing=False):
    command = [str(tools / "cargo"), *args]
    result = subprocess.run(command, env=environment(tools), text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    passed = result.returncode == 0
    if expected_missing:
        passed = result.returncode != 0 and "unavailable is not a pass" in result.stdout
    checks.append(dict(name=name, passed=passed, exit_code=result.returncode,
                       output_sha256=hashlib.sha256(result.stdout.encode()).hexdigest()))
    print(json.dumps(checks[-1]), flush=True)
    if not passed:
        print(result.stdout[-18000:], flush=True)


def consumer(directory):
    root = Path(directory)
    (root / "src").mkdir()
    (root / "src/main.rs").write_text('fn main() { let _ = dactyl_db::SupabaseRpcConfig::v1("https://project.example", "sb_publishable_fixture"); }\n')
    (root / "Cargo.toml").write_text('[package]\nname="dactyl-clean-consumer"\nversion="0.0.0"\nedition="2021"\nrust-version="1.75"\n[dependencies]\ndactyl-db={path=' + json.dumps(str(ROOT)) + ',features=["sqlite","neon","supabase"]}\n')
    return str(root / "Cargo.toml")


if sys.argv[1:] == ["--prepare"]:
    subprocess.run([str(STABLE / "cargo"), "fetch", "--locked"], env=environment(STABLE, False), check=True)
    with tempfile.TemporaryDirectory(prefix="dactyl-consumer-preparation-") as scratch:
        subprocess.run([str(STABLE / "cargo"), "fetch", "--manifest-path", consumer(scratch)], env=environment(STABLE, False), check=True)
    raise SystemExit(0)
if len(sys.argv) != 1:
    raise SystemExit("Only --prepare is supported in addition to complete proof")
run("format", ["fmt", "--all", "--", "--check"])
run("all-features", ["test", "--locked", "--offline", "--all-features"])
for feature in ["", "sqlite", "neon", "supabase", "sqlite,neon", "sqlite,supabase", "neon,supabase"]:
    run("features-" + (feature or "none"), ["test", "--locked", "--offline", "--no-default-features"] + (["--features", feature] if feature else []))
run("clippy", ["clippy", "--locked", "--offline", "--all-targets", "--all-features", "--", "-D", "warnings"])
run("missing-real-fixture", ["test", "--locked", "--offline", "--features", "supabase", "--test", "supabase_service", "local_postgres_service_contract", "--", "--ignored", "--exact"], expected_missing=True)
run("msrv-locked", ["test", "--locked", "--offline", "--all-features"], tools=MSRV)
with tempfile.TemporaryDirectory(prefix="dactyl-consumer-proof-") as scratch:
    run("msrv-clean-consumer", ["check", "--offline", "--manifest-path", consumer(scratch)], tools=MSRV)
run("package", ["package", "--locked", "--offline", "--allow-dirty"])
print(json.dumps(dict(scope="local_rust_contract", checks=checks, real_postgres="unexecuted", hosted="unexecuted", publication="unexecuted")), flush=True)
raise SystemExit(0 if all(c["passed"] for c in checks) else 1)
