#!/usr/bin/env python3
"""Export an explicit manifest from committed source into a new local directory.

No remote, publication, runtime change, original Git metadata or old refs.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path, PurePosixPath

from release_check import ReleaseError, check_file, policy_values


def git(root: Path, *args: str) -> bytes:
    result = subprocess.run(["git", "-C", str(root), *args], capture_output=True)
    if result.returncode:
        raise ReleaseError("Cannot read selected committed source")
    return result.stdout


def export(
    source: Path, destination: Path, *, policy: Path | None = None, init_git: bool = False
) -> dict:
    source = source.resolve()
    destination = destination.absolute()
    if destination.exists() or destination.is_symlink():
        raise ReleaseError("Destination must not exist")
    if source == destination or source in destination.parents:
        raise ReleaseError("Destination must be outside the source checkout")
    commit = git(source, "rev-parse", "HEAD").decode().strip()
    manifest = git(source, "show", f"{commit}:release/public-files.txt").decode().splitlines()
    names = [line.strip() for line in manifest if line.strip() and not line.startswith("#")]
    if len(names) != len(set(names)) or not names or "LICENSE" not in names:
        raise ReleaseError("Manifest must be unique, nonempty and retain LICENSE")
    prohibited = policy_values(policy)
    files: list[tuple[str, bytes, int]] = []
    for name in names:
        rel = PurePosixPath(name)
        if rel.is_absolute() or ".." in rel.parts or "\\" in name or str(rel) != name:
            raise ReleaseError("Invalid manifest path")
        row = git(source, "ls-tree", commit, "--", name).decode().strip()
        if not row or row.split()[1] != "blob":
            raise ReleaseError("Manifest entry is not a committed file")
        mode = row.split()[0]
        data = git(source, "show", f"{commit}:{name}")
        if mode == "120000":
            # Flatten only a manifest-contained, relative link. This avoids
            # exporting a link that would disclose files outside the candidate.
            target = (PurePosixPath(name).parent / data.decode()).as_posix()
            if target not in names or ".." in PurePosixPath(target).parts:
                raise ReleaseError("Manifest symlink target is not selected")
            data = git(source, "show", f"{commit}:{target}")
            mode = "100755"
        if mode not in {"100644", "100755"}:
            raise ReleaseError("Unsupported manifest file mode")
        check_file(name, data, prohibited)
        files.append((name, data, int(mode[-3:], 8)))
    destination.parent.mkdir(parents=True, exist_ok=True)
    scratch = Path(tempfile.mkdtemp(prefix=".public-export-", dir=destination.parent))
    try:
        hashes = {}
        for name, data, mode in files:
            path = scratch / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            path.chmod(mode)
            hashes[name] = hashlib.sha256(data).hexdigest()
        if init_git:
            git(scratch, "init", "--initial-branch=main", "--template=")
            git(scratch, "add", "--", *names)
            git(
                scratch,
                "-c",
                "user.name=Release preparation",
                "-c",
                "user.email=release@example.invalid",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-m",
                "Initial gateway source",
            )
        os.rename(scratch, destination)
    finally:
        if scratch.exists():
            shutil.rmtree(scratch)
    # Evidence goes to caller output, never into the candidate or its history.
    return {"source_commit": commit, "files": len(files), "sha256": hashes}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--source", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--policy", type=Path)
    parser.add_argument("--init-git", action="store_true")
    args = parser.parse_args()
    try:
        result = export(args.source, args.destination, policy=args.policy, init_git=args.init_git)
    except (ReleaseError, OSError, UnicodeError):
        raise SystemExit("Export failed; no publication performed (details redacted)") from None
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
