"""Export isolation, fresh history, redaction and distribution checks."""

import importlib.util
import json
import subprocess
import sys
import tarfile
from io import BytesIO
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))
from release_check import ReleaseError, check_archive, check_file  # noqa: E402

SPEC = importlib.util.spec_from_file_location("export_public", SCRIPTS / "export-public.py")
exporter = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(exporter)


def git(path, *args):
    return subprocess.check_output(["git", "-C", str(path), *args], stderr=subprocess.DEVNULL)


def fixture_repo(tmp_path, extra=""):
    source = tmp_path / "source"
    source.mkdir()
    git(source, "init", "-b", "main")
    git(source, "config", "user.name", "Example")
    git(source, "config", "user.email", "example@example.invalid")
    (source / "private.txt").write_text("historical-private-marker")
    git(source, "add", ".")
    git(source, "commit", "-m", "Private history")
    (source / "release").mkdir()
    (source / "LICENSE").write_text("Retain this license")
    (source / "public.py").write_text("print('example')\n" + extra)
    (source / "release/public-files.txt").write_text(
        "LICENSE\npublic.py\nrelease/public-files.txt\n"
    )
    git(source, "add", ".")
    git(source, "commit", "-m", "Select candidate")
    return source


def test_fresh_export_excludes_history_and_untracked_material(tmp_path):
    source = fixture_repo(tmp_path)
    (source / ".env").write_text("UNTRACKED=private")
    (source / "public.py").write_text("uncommitted-private-marker")
    dest = tmp_path / "candidate"
    result = exporter.export(source, dest, init_git=True)
    assert result["files"] == 3
    assert not (dest / "private.txt").exists()
    assert not (dest / ".env").exists()
    assert (dest / "LICENSE").read_text() == "Retain this license"
    assert "uncommitted" not in (dest / "public.py").read_text()
    assert git(dest, "rev-list", "--count", "--all").strip() == b"1"
    assert git(dest, "remote").strip() == b""
    assert b"Private history" not in git(dest, "log", "--all", "--format=%B")


def test_private_policy_failure_is_redacted_and_leaves_no_candidate(tmp_path):
    marker = "synthetic-prohibited-deployment"
    source = fixture_repo(tmp_path, extra=marker)
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps([marker]))
    dest = tmp_path / "candidate"
    with pytest.raises(ReleaseError) as err:
        exporter.export(source, dest, policy=policy)
    assert marker not in str(err.value)
    assert not dest.exists()


@pytest.mark.parametrize("name", [".env", "state/data.db", "secrets/token", "../escape", "/escape"])
def test_prohibited_file_classes(name):
    with pytest.raises(ReleaseError):
        check_file(name, b"innocent")


def test_secret_shapes_are_rejected_without_echo():
    key = b"gh" + b"p_" + b"x" * 36
    with pytest.raises(ReleaseError) as err:
        check_file("README.md", key)
    assert key.decode() not in str(err.value)


def test_archive_scan_rejects_runtime_and_unsafe_entries(tmp_path):
    for name in ("package/state.db", "../escape"):
        path = tmp_path / "source.tar.gz"
        with tarfile.open(path, "w:gz") as archive:
            entry = tarfile.TarInfo(name)
            entry.size = 3
            archive.addfile(entry, BytesIO(b"abc"))
        with pytest.raises(ReleaseError):
            check_archive(path, [])


def test_existing_destination_is_never_overwritten(tmp_path):
    source = fixture_repo(tmp_path)
    dest = tmp_path / "candidate"
    dest.mkdir()
    with pytest.raises(ReleaseError):
        exporter.export(source, dest)
    assert list(dest.iterdir()) == []


def test_symlink_outside_manifest_is_rejected(tmp_path):
    source = fixture_repo(tmp_path)
    (source / "public.py").unlink()
    (source / "public.py").symlink_to("private.txt")
    git(source, "add", ".")
    git(source, "commit", "-m", "Unsafe fixture")
    with pytest.raises(ReleaseError):
        exporter.export(source, tmp_path / "candidate")
