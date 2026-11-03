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
    assert 'EnvironmentFile="' + str(env) in result.stdout
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
