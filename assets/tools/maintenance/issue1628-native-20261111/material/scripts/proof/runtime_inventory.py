"""Print installed native runtime files for independent configuration review.

This read-only aid does not modify configuration or produce approval pins.
Run it only in the designated ubuntu:24.04 x86 container. Its output
must be reviewed before being used as a toolchain_files declaration.
"""

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile  # Match the proof helper's standard-library imports.


FILES = {}


def add(label, path):
    resolved = Path(path).resolve(strict=True)
    if not resolved.is_file():
        raise SystemExit("Expected installed runtime file: " + str(resolved))
    FILES[label] = str(resolved)


for label, path in {
    "python": "/usr/bin/python3.12",
    "rustup": "/usr/local/cargo/bin/rustup",
    "cc": "/usr/bin/x86_64-linux-gnu-gcc-13",
    "cc1": "/usr/lib/gcc/x86_64-linux-gnu/13/cc1",
    "collect2": "/usr/lib/gcc/x86_64-linux-gnu/13/collect2",
    "ar": "/usr/bin/x86_64-linux-gnu-ar",
    "as": "/usr/bin/x86_64-linux-gnu-as",
    "ld": "/usr/bin/x86_64-linux-gnu-ld.bfd",
    "shell": "/bin/dash",
    "sqlite": "/usr/lib/x86_64-linux-gnu/libsqlite3.so.0.8.6",
}.items():
    add(label, path)

for name, variable in (
    ("stable", "DACTYL_STABLE_TOOLS"), ("msrv", "DACTYL_MSRV_TOOLS")
):
    tools = Path(os.environ[variable]).resolve(strict=True)
    for path in sorted(tools.iterdir()):
        if path.is_file():
            add(name + "_" + path.name.replace("-", "_"), path)
    # rustc's driver and LLVM are loaded outside bin/. Host stdlib/static
    # archives also affect compilation and linking, so bind their exact bytes.
    for path in sorted((tools.parent / "lib").rglob("*")):
        if path.is_file() and (
            ".so" in path.name or path.suffix in (".rlib", ".a", ".o")
        ):
            label = name + "_runtime_" + hashlib.sha256(str(path).encode()).hexdigest()[:16]
            add(label, path)

for path in sorted(Path("/usr/lib/gcc/x86_64-linux-gnu/13").glob("*")):
    if path.is_file() and path.suffix in (".a", ".o"):
        add("gcc_" + path.name.replace(".", "_"), path)
for name in ("crt1.o", "crti.o", "crtn.o", "Scrt1.o", "libc.so", "libc_nonshared.a"):
    add("libc_" + name.replace(".", "_"), "/usr/lib/x86_64-linux-gnu/" + name)

# The standard-library source/bytecode and extension modules already imported
# by these helpers are part of their installed Python execution environment.
for module in tuple(sys.modules.values()):
    for attribute in ("__file__", "__cached__"):
        path = getattr(module, attribute, None)
        if path and (
            path.startswith("/usr/lib/python3.12/")
            or path == "/etc/python3.12/sitecustomize.py"
            or path.startswith("/etc/python3.12/__pycache__/sitecustomize.")
        ) and Path(path).is_file():
            label = "python_runtime_" + hashlib.sha256(path.encode()).hexdigest()[:16]
            add(label, path)

# Inspect ELF dependency declarations using the distribution's installed ldd.
# Only fixed installed tool/runtime files above are inspected, never candidate
# binaries. Linker scripts and Rust archives are retained without invoking ldd.
for path in sorted(set(FILES.values())):
    with open(path, "rb") as file:
        if file.read(4) != b"\x7fELF":
            continue
    result = subprocess.run(
        ["/usr/bin/ldd", path], text=True, capture_output=True, check=False
    )
    output = result.stdout + result.stderr
    if "not found" in output:
        raise SystemExit("Missing installed shared dependency for " + path)
    for dependency in re.findall(r"(?:=>\s+|^\s*)(/[^\s()]+)", output, re.MULTILINE):
        resolved = str(Path(dependency).resolve(strict=True))
        label = "shared_runtime_" + hashlib.sha256(resolved.encode()).hexdigest()[:16]
        add(label, resolved)

print(json.dumps({"toolchain_files": dict(sorted(FILES.items()))}, indent=2))
