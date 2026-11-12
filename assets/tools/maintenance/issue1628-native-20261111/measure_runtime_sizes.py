"""Read byte/device/inode sizes for the exact independently reviewed runtime closure."""
import argparse
import hashlib
import json
from pathlib import Path
import stat

EXPECTED = 'b75516456f1aef558d2615aa32448027f40121f7491e70f446a32b8bd0dcc142'
p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--inventory', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
b = a.inventory.read_bytes()
assert hashlib.sha256(b).hexdigest() == EXPECTED, 'Runtime closure differs; no task import is allowed.'
i = json.loads(b)
files = i['toolchain_files']
assert len(files) == 280
rows = []
for label, value in sorted(files.items()):
    path = Path(value)
    assert path.is_absolute() and path.resolve(strict=True) == path
    s = path.stat()
    assert stat.S_ISREG(s.st_mode)
    rows.append(dict(label=label, path=value, bytes=s.st_size, device=s.st_dev, inode=s.st_ino, links=s.st_nlink))
unique = {x['path']: x for x in rows}
inodes = {(x['device'], x['inode']): x for x in rows}
groups = {}
for group, prefix in [('stable', '/usr/local/rustup/toolchains/1.96.1-'), ('msrv', '/usr/local/rustup/toolchains/1.75.0-')]:
    selected = [x for x in rows if x['path'].startswith(prefix)]
    groups[group] = dict(labels=len(selected), labelled_bytes=sum(x['bytes'] for x in selected),
        unique_paths=len({x['path'] for x in selected}), unique_bytes=sum(x['bytes'] for x in {x['path']: x for x in selected}.values()))
shared = [x for x in rows if not x['path'].startswith('/usr/local/rustup/toolchains/')]
groups['common'] = dict(labels=len(shared), labelled_bytes=sum(x['bytes'] for x in shared),
    unique_paths=len({x['path'] for x in shared}), unique_bytes=sum(x['bytes'] for x in {x['path']: x for x in shared}.values()))
program = Path('/usr/bin/python3.12').stat()
summary = dict(inventory_sha256='sha256:' + EXPECTED, labels=len(rows), unique_paths=len(unique),
    labelled_bytes=sum(x['bytes'] for x in rows), unique_canonical_bytes=sum(x['bytes'] for x in unique.values()),
    unique_device_inode_bytes=sum(x['bytes'] for x in inodes.values()), program_bytes=program.st_size,
    labelled_plus_program_bytes=sum(x['bytes'] for x in rows)+program.st_size,
    groups=groups, largest_paths=sorted(unique.values(), key=lambda x:x['bytes'], reverse=True)[:15],
    native_per_invocation_byte_cap=1073741824, observation='read_only_installed_runtime_stat',
    task_import=False, product_execution=False)
a.output.write_text(json.dumps(dict(summary=summary, files=rows), indent=2)+'\n')
print(json.dumps(summary, indent=2))
