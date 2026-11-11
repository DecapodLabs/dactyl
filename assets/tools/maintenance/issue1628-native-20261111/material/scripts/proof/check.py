"""Reviewed offline Rust contract, invoked only by native Decapod validation.

This local proof does not run hosted CI, a real PostgreSQL service, deployment,
or crate publication. Dependency acquisition is a separate preparation step.
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
CHECKS = []


def environment(tools):
    env = dict(
        os.environ,
        RUSTC=str(tools / "rustc"),
        RUSTDOC=str(tools / "rustdoc"),
        RUSTUP_TOOLCHAIN=tools.parent.name,
        CARGO_NET_OFFLINE="true",
        CARGO_TERM_COLOR="never",
        PATH=str(tools) + ":" + os.environ["DACTYL_PROOF_TOOLS"],
    )
    for key in ("DACTYL_SUPABASE_FIXTURE", "DACTYL_SUPABASE_HOSTED_PROOF"):
        env.pop(key, None)
    return env


def run(name, args, tools=STABLE, expected_missing=False):
    try:
        result = subprocess.run(
            [str(tools / "cargo"), *args],
            env=environment(tools),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=900,
            check=False,
        )
        output, exit_code = result.stdout, result.returncode
        passed = exit_code == 0
        if expected_missing:
            passed = (
                exit_code != 0
                and "unavailable is not a pass" in output
                and "test local_postgres_service_contract ... FAILED" in output
            )
    except subprocess.TimeoutExpired as error:
        output = error.stdout or b""
        if isinstance(output, bytes):
            output = output.decode("utf-8", errors="replace")
        output += "\nRequired check exceeded its 900-second limit.\n"
        exit_code, passed = None, False
    record = dict(
        name=name,
        passed=passed,
        exit_code=exit_code,
        output_sha256=hashlib.sha256(output.encode()).hexdigest(),
    )
    CHECKS.append(record)
    print(json.dumps(record), flush=True)
    if not passed:
        print(output[-18000:], flush=True)


run("format", ["fmt", "--all", "--", "--check"])
run("all-features", ["test", "--locked", "--all-features"])
for feature in (
    "",
    "sqlite",
    "neon",
    "supabase",
    "sqlite,neon",
    "sqlite,supabase",
    "neon,supabase",
):
    run(
        "features-" + (feature or "none"),
        ["test", "--locked", "--no-default-features"]
        + (["--features", feature] if feature else []),
    )
run(
    "clippy",
    ["clippy", "--locked", "--all-targets", "--all-features", "--", "-D", "warnings"],
)
run(
    "missing-real-fixture",
    [
        "test", "--locked", "--features", "supabase", "--test", "supabase_service",
        "local_postgres_service_contract", "--", "--ignored", "--exact",
    ],
    expected_missing=True,
)
run("msrv", ["test", "--locked", "--all-features"], tools=MSRV)
with tempfile.TemporaryDirectory(prefix="dactyl-consumer-") as directory:
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
    # A new consumer resolves against public manifest bounds, without copying
    # this library's lockfile. Only the temporary consumer is writable.
    run(
        "msrv-clean-consumer",
        ["check", "--manifest-path", str(consumer / "Cargo.toml")],
        tools=MSRV,
    )
run("package", ["package", "--locked", "--allow-dirty"])
print(
    json.dumps(
        dict(
            scope="local_rust_contract",
            checks=CHECKS,
            hosted_ci="unexecuted",
            real_postgres="unexecuted",
            deployment="unexecuted",
            publication="unexecuted",
        )
    ),
    flush=True,
)
raise SystemExit(0 if all(check["passed"] for check in CHECKS) else 1)
