"""Build review material only; this script is not a repository proof command."""

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tomllib


ROOT = Path(__file__).resolve().parent
EXECUTION = ROOT.parent / "execution"
INVENTORY = EXECUTION / "dactyl-receiver-43126195742/runtime-inventory.json"
SIZES = EXECUTION / "dactyl-runtime-sizes-43126937218/runtime-sizes.json"
EXPECTED_INVENTORY = "b75516456f1aef558d2615aa32448027f40121f7491e70f446a32b8bd0dcc142"
EXPECTED_SIZES = "fd3fe21e7cd21fdcefdc7daaa5de8babb4e5e3fb532462a690d56abbbecfbf35"
PREFIX = "/usr/local/rustup/toolchains/"
RUST = {
    "stable": PREFIX + "1.96.1-x86_64-unknown-linux-gnu/",
    "msrv": PREFIX + "1.75.0-x86_64-unknown-linux-gnu/",
}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


assert sha(INVENTORY) == EXPECTED_INVENTORY
assert sha(SIZES) == EXPECTED_SIZES
inventory = json.loads(INVENTORY.read_text())["toolchain_files"]
measurements = json.loads(SIZES.read_text())["files"]
assert len(inventory) == len(measurements) == 280
measured = {row["label"]: row for row in measurements}
assert set(measured) == set(inventory)

# Preserve the first existing label in lexical order for each canonical path.
# Deduplicate paths only, never equal content or distinct hard-linked paths.
labels = {}
size_by_path = {}
for label, path in sorted(inventory.items()):
    row = measured[label]
    assert row["path"] == path and Path(path).is_absolute()
    assert not path.startswith(PREFIX) or any(path.startswith(p) for p in RUST.values())
    if path in size_by_path:
        assert size_by_path[path] == row["bytes"]
    size_by_path[path] = row["bytes"]
    labels.setdefault(path, label)
assert len(labels) == 272

base = tomllib.loads((ROOT / "before/config.toml").read_text())
old_commands = base["proof"]["commands"]
assert len(old_commands) == 1 and old_commands[0]["name"] == "rust-contract"
old_execution = base["proof"]["repository_contract"]["executions"]["rust-contract"]
assert old_execution["toolchain_files"] == inventory
assert old_execution["timeout_seconds"] == 3600
assert old_execution["program"] == "/usr/bin/python3.12"
program_bytes = size_by_path[old_execution["program"]]

groups = {}
commands = []
executions = {}
descriptions = {
    "stable": "Stable Rust feature matrix, auth/RPC contract, formatting, clippy, missing-fixture and package verification",
    "msrv": "Rust 1.75 all-feature contract and clean-consumer verification without the library lockfile",
}
for group, timeout in (("stable", 2400), ("msrv", 1200)):
    selected = {
        label: path for path, label in labels.items()
        if not path.startswith(PREFIX) or path.startswith(RUST[group])
    }
    selected = dict(sorted(selected.items()))
    name = "rust-contract-" + group
    command = deepcopy(old_commands[0])
    command.update(
        name=name,
        args=["scripts/proof/check.py", "--group", group],
        description=descriptions[group],
        required=True,
    )
    execution = deepcopy(old_execution)
    execution["timeout_seconds"] = timeout
    execution["toolchain_files"] = selected
    measured_bytes = program_bytes + sum(size_by_path[path] for path in selected.values())
    assert measured_bytes < 1024**3
    commands.append(command)
    executions[name] = execution
    groups[group] = {
        "command": name,
        "toolchain_files": selected,
        "unique_paths": len(selected),
        "measured_bytes_including_direct_python": measured_bytes,
        "headroom_below_one_gib": 1024**3 - measured_bytes,
        "timeout_seconds": timeout,
    }

stable_paths = set(groups["stable"]["toolchain_files"].values())
msrv_paths = set(groups["msrv"]["toolchain_files"].values())
assert stable_paths | msrv_paths == set(inventory.values())
assert len(stable_paths & msrv_paths) == 176
assert sum(execution["timeout_seconds"] for execution in executions.values()) == 3600
coverage = deepcopy(base["proof"]["repository_contract"]["source_coverage"])
mapped = 0
for row in coverage:
    if row["source"] == ".github/workflows/rust-contract.yml":
        assert row["commands"] == ["rust-contract"] and not row.get("ci_only", False)
        row["commands"] = [command["name"] for command in commands]
        mapped += 1
assert mapped == 1

patch = {
    "base_commit": "26e671d809a83c3dcc7eaf09d0b87322c50ff922",
    "replace": {
        "proof.commands": commands,
        "proof.repository_contract.executions": executions,
        "proof.repository_contract.source_coverage": coverage,
    },
    "review_digest_handling": "Recompute only the reviewed source and command digests after independent review; require native reconciliation to reproduce the approved values. Never accept a stale or self-adopted pin.",
    "all_other_config_values": "preserve exactly",
}
projection = {
    "inventory_sha256": EXPECTED_INVENTORY,
    "sizes_sha256": EXPECTED_SIZES,
    "original_labels": len(inventory),
    "original_unique_paths": len(labels),
    "preserved_unique_path_union": len(stable_paths | msrv_paths),
    "common_unique_paths": len(stable_paths & msrv_paths),
    "groups": groups,
}
for name, value in (("runtime-groups.json", projection), ("config-patch.json", patch)):
    (ROOT / name).write_text(json.dumps(value, indent=2) + "\n")
print(json.dumps({"files": ["runtime-groups.json", "config-patch.json"], "unique_path_union": 272}))
