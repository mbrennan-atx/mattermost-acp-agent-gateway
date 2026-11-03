#!/usr/bin/env python3
"""Scan candidate files and distributions without printing matched values."""

from __future__ import annotations

import argparse
import json
import re
import tarfile
import zipfile
from pathlib import Path, PurePosixPath


class ReleaseError(ValueError):
    pass


# Test fixtures can contain these expressions as patterns, but no real values.
SECRET_PATTERNS = (
    re.compile(rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    re.compile(rb"\bgh[pousr]_[A-Za-z0-9]{30,}\b"),
    re.compile(rb"\bgithub_pat_[A-Za-z0-9_]{50,}\b"),
    re.compile(rb"\bAKIA[A-Z0-9]{16}\b"),
)
DENIED_PARTS = {".env", ".git", ".venv", "node_modules", "__pycache__", "secrets", "logs"}
DENIED_SUFFIXES = (".db", ".sqlite", ".sqlite3", ".db-wal", ".db-shm", ".pyc", ".log")


def policy_values(path: Path | None) -> list[bytes]:
    if path is None:
        return []
    try:
        values = json.loads(path.read_text())
        if not isinstance(values, list) or not all(isinstance(v, str) and v for v in values):
            raise ValueError
        return [v.encode().lower() for v in values]
    except (OSError, ValueError):
        raise ReleaseError("Private release policy is unreadable or invalid") from None


def check_file(name: str, data: bytes, prohibited: list[bytes] = ()) -> None:
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts or "\\" in name:
        raise ReleaseError("Unsafe candidate path")
    if any(p in DENIED_PARTS for p in path.parts) or path.name.endswith(DENIED_SUFFIXES):
        raise ReleaseError("Prohibited runtime file class")
    if path.name.startswith(".env.") and path.name != ".env.example":
        raise ReleaseError("Prohibited environment file")
    if path.suffix == ".env":
        raise ReleaseError("Prohibited environment file")
    if any(pattern.search(data) for pattern in SECRET_PATTERNS):
        raise ReleaseError("Credential-shaped content found; match redacted")
    lower = data.lower()
    if any(value in lower or value in name.lower().encode() for value in prohibited):
        raise ReleaseError("Private identifier policy matched; match redacted")


def check_archive(path: Path, prohibited: list[bytes]) -> int:
    count = 0
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as archive:
            for entry in archive.infolist():
                if entry.is_dir():
                    continue
                if entry.file_size > 20_000_000:
                    raise ReleaseError("Oversized archive entry")
                check_file(entry.filename, archive.read(entry), prohibited)
                count += 1
    else:
        with tarfile.open(path, "r:*") as archive:
            for entry in archive:
                if entry.isdir():
                    continue
                target = entry
                if entry.issym():
                    link = PurePosixPath(entry.linkname)
                    if link.is_absolute() or ".." in link.parts:
                        raise ReleaseError("Unsafe archive link")
                    target_name = str(PurePosixPath(entry.name).parent / link)
                    try:
                        target = archive.getmember(target_name)
                    except KeyError:
                        raise ReleaseError("Missing archive link target") from None
                if not target.isfile() or target.size > 20_000_000:
                    raise ReleaseError("Unsupported archive entry")
                check_file(entry.name, archive.extractfile(target).read(), prohibited)
                count += 1
    if not count:
        raise ReleaseError("Empty archive")
    return count


def check_tree(path: Path, prohibited: list[bytes]) -> int:
    count = 0
    for file in sorted(path.rglob("*")):
        rel = file.relative_to(path)
        if rel.parts[0] == ".git":
            continue
        if file.is_file():
            check_file(rel.as_posix(), file.read_bytes(), prohibited)
            count += 1
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path)
    parser.add_argument("--policy", type=Path)
    args = parser.parse_args()
    prohibited = policy_values(args.policy)
    try:
        count = (
            check_tree(args.path, prohibited)
            if args.path.is_dir()
            else check_archive(args.path, prohibited)
        )
    except (ReleaseError, OSError, ValueError, tarfile.TarError, zipfile.BadZipFile):
        raise SystemExit(
            "Release check failed; inspect candidate privately (values redacted)"
        ) from None
    print(f"Release check passed: {count} files")


if __name__ == "__main__":
    main()
