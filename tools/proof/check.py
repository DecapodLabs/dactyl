"""Reviewed local Rust proof, run only by decapod validate.

No hosted service, package publication, or infrastructure deployment is performed.
Dependency preparation is separate; every proof command runs offline.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path.cwd()
STABLE = Path(os.environ["DACTYL_STABLE_TOOLS"])
MSRV = Path(os.environ["DACTYL_MSRV_TOOLS"])
checks = []


def run(name, args, tools=STABLE, expected_missing=False):
    env = dict(os.environ, RUSTC=str(tools / "rustc"), RUSTDOC=str(tools / "rustdoc"),
               RUSTUP_TOOLCHAIN=tools.parent.name, CARGO_NET_OFFLINE="true")
    env["PATH"] = str(tools) + ":" + os.environ["DACTYL_PROOF_TOOLS"]
    for key in ("DACTYL_SUPABASE_FIXTURE", "DACTYL_SUPABASE_HOSTED_PROOF"):
        env.pop(key, None)
    result = subprocess.run([str(tools / "cargo"), *args], env=env, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=900)
    passed = result.returncode == 0
    if expected_missing:
        passed = result.returncode != 0 and "unavailable is not a pass" in result.stdout
    record = dict(name=name, passed=passed, exit_code=result.returncode,
                  output_sha256=hashlib.sha256(result.stdout.encode()).hexdigest())
    checks.append(record)
    print(json.dumps(record), flush=True)
    if not passed:
        print(result.stdout[-18000:], flush=True)


run("format", ["fmt", "--all", "--", "--check"])
run("all-features", ["test", "--locked", "--all-features"])
for feature in ["", "sqlite", "neon", "supabase", "sqlite,neon", "sqlite,supabase", "neon,supabase"]:
    run("features-" + (feature or "none"), ["test", "--locked", "--no-default-features"] +
        (["--features", feature] if feature else []))
run("clippy", ["clippy", "--locked", "--all-targets", "--all-features", "--", "-D", "warnings"])
run("missing-real-fixture", ["test", "--locked", "--features", "supabase", "--test", "supabase_service",
                            "local_postgres_service_contract", "--", "--ignored", "--exact"], expected_missing=True)
run("msrv", ["test", "--locked", "--all-features"], tools=MSRV)
with tempfile.TemporaryDirectory(prefix="dactyl-consumer-") as directory:
    root = Path(directory)
    (root / "src").mkdir()
    (root / "src/main.rs").write_text('fn main() { let _ = dactyl_db::Backend::Cloud; }\n')
    (root / "Cargo.toml").write_text('[package]\nname="clean-consumer"\nversion="0.0.0"\nedition="2021"\nrust-version="1.75"\n'
                                   '[dependencies]\ndactyl-db={path=' + json.dumps(str(ROOT)) +
                                   ',features=["sqlite","neon","supabase"]}\n')
    run("msrv-clean-consumer", ["check", "--manifest-path", str(root / "Cargo.toml")], tools=MSRV)
run("package", ["package", "--locked", "--allow-dirty"])
print(json.dumps(dict(scope="local_rust_contract", checks=checks, hosted="unexecuted", deployment="unexecuted")), flush=True)
raise SystemExit(0 if all(check["passed"] for check in checks) else 1)
