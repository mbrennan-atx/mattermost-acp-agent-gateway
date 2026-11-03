from pathlib import Path

import pytest

from mattermost_acp.config import Config


def _clear_mm(monkeypatch) -> None:
    for key in (
        "MATTERMOST_URL",
        "MATTERMOST_TOKEN",
        "MATTERMOST_TOKEN_FILE",
        "MATTERMOST_CHANNEL_ID",
        "MATTERMOST_CHANNEL_IDS",
        "MATTERMOST_ALLOW_USER_IDS",
        "MATTERMOST_TEAM_ID",
        "MATTERMOST_SLASH_TOKEN",
        "MATTERMOST_SLASH_TOKENS",
        "MATTERMOST_SLASH_TOKEN_FILE",
        "HARNESS",
        "AGENT_CWD",
        "AGENT_MODEL",
        "GROK_CWD",
        "GROK_MODEL",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("HARNESS", "grok")
    monkeypatch.setenv("AGENT_COMMAND", "grok")


def test_config_loads_token_file_and_csv_channels(tmp_path: Path, monkeypatch) -> None:
    _clear_mm(monkeypatch)
    token_path = tmp_path / "pat"
    token_path.write_text("mm-user-pat\n", encoding="utf-8")
    monkeypatch.setenv("MATTERMOST_URL", "https://chat.example.com")
    monkeypatch.setenv("MATTERMOST_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MATTERMOST_CHANNEL_ID", "chanA,chanB")
    monkeypatch.setenv("MATTERMOST_ALLOW_USER_IDS", "user1")
    monkeypatch.setenv("AGENT_CWD", "/tmp")
    monkeypatch.setenv("HARNESS", "grok")
    cfg = Config.load("/dev/null")
    assert cfg.mattermost_url == "https://chat.example.com"
    assert cfg.mattermost_token == "mm-user-pat"
    assert cfg.channel_ids == frozenset({"chanA", "chanB"})
    assert cfg.allow_user_ids == frozenset({"user1"})
    assert cfg.harness == "grok"
    assert cfg.auto_rename_threads is True


def test_config_requires_url(tmp_path: Path, monkeypatch) -> None:
    _clear_mm(monkeypatch)
    monkeypatch.setenv("MATTERMOST_TOKEN", "abc")
    monkeypatch.setenv("MATTERMOST_CHANNEL_ID", "x")
    with pytest.raises(ValueError, match="MATTERMOST_URL"):
        Config.load("/dev/null")


def test_empty_channel_ids_means_all(tmp_path: Path, monkeypatch) -> None:
    _clear_mm(monkeypatch)
    monkeypatch.delenv("MATTERMOST_AUTO_JOIN_CHANNELS", raising=False)
    monkeypatch.setenv("MATTERMOST_URL", "https://chat.example.com")
    monkeypatch.setenv("MATTERMOST_TOKEN", "abc")
    cfg = Config.load("/dev/null")
    assert cfg.channel_ids == frozenset()
    assert cfg.all_channels is True
    assert cfg.auto_join_channels is True


def test_channel_ids_all_keyword(tmp_path: Path, monkeypatch) -> None:
    _clear_mm(monkeypatch)
    monkeypatch.delenv("MATTERMOST_AUTO_JOIN_CHANNELS", raising=False)
    monkeypatch.setenv("MATTERMOST_URL", "https://chat.example.com")
    monkeypatch.setenv("MATTERMOST_TOKEN", "abc")
    monkeypatch.setenv("MATTERMOST_CHANNEL_IDS", "ALL")
    cfg = Config.load("/dev/null")
    assert cfg.all_channels is True
    assert cfg.channel_ids == frozenset()
    assert cfg.auto_join_channels is True


def test_explicit_channel_list_is_not_all(tmp_path: Path, monkeypatch) -> None:
    _clear_mm(monkeypatch)
    monkeypatch.delenv("MATTERMOST_AUTO_JOIN_CHANNELS", raising=False)
    monkeypatch.setenv("MATTERMOST_URL", "https://chat.example.com")
    monkeypatch.setenv("MATTERMOST_TOKEN", "abc")
    monkeypatch.setenv("MATTERMOST_CHANNEL_IDS", "chanA,chanB")
    cfg = Config.load("/dev/null")
    assert cfg.all_channels is False
    assert cfg.channel_ids == frozenset({"chanA", "chanB"})
    assert cfg.auto_join_channels is False


def test_legacy_grok_keys_still_load(tmp_path: Path, monkeypatch) -> None:
    _clear_mm(monkeypatch)
    monkeypatch.setenv("MATTERMOST_URL", "https://chat.example.com")
    monkeypatch.setenv("MATTERMOST_TOKEN", "abc")
    monkeypatch.setenv("GROK_MODEL", "legacy")
    monkeypatch.setenv("GROK_CWD", "/tmp")
    assert Config.load("/dev/null").grok_model == "legacy"
    monkeypatch.setenv("AGENT_MODEL", "opus")
    cfg = Config.load("/dev/null")
    assert cfg.grok_model == "opus"
    assert cfg.grok_cwd == Path("/tmp")


def test_config_reads_shared_slash_token_file(tmp_path: Path, monkeypatch) -> None:
    _clear_mm(monkeypatch)
    monkeypatch.setenv("MATTERMOST_DOTENV", "none")
    monkeypatch.setenv("MATTERMOST_URL", "https://mm.example.com")
    monkeypatch.setenv("MATTERMOST_TOKEN", "pat")
    monkeypatch.setenv("MATTERMOST_SLASH_TOKEN", "inline")
    tokens = tmp_path / "slash_tokens"
    tokens.write_text("aaa\nbbb, ccc\n\n", encoding="utf-8")
    monkeypatch.setenv("MATTERMOST_SLASH_TOKEN_FILE", str(tokens))
    cfg = Config.load()
    assert cfg.slash_tokens == frozenset({"inline", "aaa", "bbb", "ccc"})
    monkeypatch.setenv("MATTERMOST_SLASH_TOKEN_FILE", str(tmp_path / "missing"))
    assert Config.load().slash_tokens == frozenset({"inline"})


def test_explicit_harness_and_command_required(monkeypatch):
    _clear_mm(monkeypatch)
    monkeypatch.setenv("MATTERMOST_URL", "https://chat.example.com")
    monkeypatch.setenv("MATTERMOST_TOKEN", "synthetic")
    monkeypatch.delenv("HARNESS")
    with pytest.raises(ValueError, match="HARNESS"):
        Config.load("/dev/null")
    monkeypatch.setenv("HARNESS", "codex")
    monkeypatch.delenv("AGENT_COMMAND")
    monkeypatch.delenv("GROK_COMMAND", raising=False)
    with pytest.raises(ValueError, match="AGENT_COMMAND"):
        Config.load("/dev/null")
