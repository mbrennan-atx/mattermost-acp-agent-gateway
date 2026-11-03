import sys

import pytest

from mattermost_acp.main import main


def test_offline_check_has_no_state_or_network(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    for key in list(__import__("os").environ):
        if key.startswith(("MATTERMOST_", "AGENT_", "GROK_", "HARNESS")):
            monkeypatch.delenv(key)
    for key, value in {
        "MATTERMOST_DOTENV": "none",
        "MATTERMOST_URL": "https://chat.example.com",
        "MATTERMOST_TOKEN": "synthetic",
        "HARNESS": "codex",
        "AGENT_COMMAND": sys.executable,
        "MATTERMOST_CONFIG_DIR": str(tmp_path / "absent config"),
        "AGENT_CWD": str(tmp_path),
        "MATTERMOST_SLASH_PORT": "0",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(sys, "argv", ["mattermost-acp-gateway", "--check"])
    main()
    assert "Configuration valid" in capsys.readouterr().out
    assert list(tmp_path.iterdir()) == []
    monkeypatch.setenv("MATTERMOST_ROSTER_FILE", "missing.json")
    with pytest.raises(SystemExit) as err:
        main()
    assert err.value.code == 2
    assert "synthetic" not in capsys.readouterr().err
