"""Unit preview is read-only; forwarding requires an explicit destination."""

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_preview_has_no_tunnel_or_sync(tmp_path):
    env = tmp_path / "private env"
    env.touch()
    result = subprocess.run(
        ["bash", "scripts/install-systemd.sh", "--user", "--dry-run", "--env-file", str(env)],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    assert "ExecStart=" in result.stdout
    assert "Restart=always" in result.stdout
    assert 'EnvironmentFile=' + str(env) in result.stdout
    assert "ssh" not in result.stdout
    assert "tunnel.service" not in result.stdout
    assert "User=" not in result.stdout
    assert "WantedBy=default.target" in result.stdout


def test_tunnel_requires_explicit_key(tmp_path):
    env = tmp_path / "env"
    env.touch()
    result = subprocess.run(
        [
            "bash",
            "scripts/install-systemd.sh",
            "--dry-run",
            "--env-file",
            str(env),
            "--tunnel-host",
            "operator@chat.example.com",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    assert result.returncode != 0
    assert "requires an existing" in result.stderr


def test_rendered_paths_with_spaces_pass_real_systemd_parser(tmp_path):
    import shutil

    import pytest

    if not shutil.which("systemd-analyze"):
        pytest.skip("systemd parser unavailable")
    env = tmp_path / "private config with spaces.env"
    env.write_text("SYNTHETIC=value\n")
    preview = subprocess.run(
        ["bash", "scripts/install-systemd.sh", "--dry-run", "--env-file", str(env)],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    unit = tmp_path / "gateway-render-check.service"
    unit.write_text(
        "\n".join(
            "ExecStart=/usr/bin/true" if line.startswith("ExecStart=") else line
            for line in preview.stdout.splitlines()
        )
        + "\n"
    )
    result = subprocess.run(
        ["systemd-analyze", "verify", str(unit)], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    assert "ignoring" not in result.stderr.lower()
    assert "invalid" not in result.stderr.lower()
