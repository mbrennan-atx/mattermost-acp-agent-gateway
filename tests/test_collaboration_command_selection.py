"""Configured collaboration commands retain their selected executable boundary."""

import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from mattermost_acp.collaboration.consumer import CollaborationConsumer
from mattermost_acp.config import Config


@pytest.fixture
def base_config(monkeypatch):
    monkeypatch.setenv("MATTERMOST_DOTENV", "none")
    monkeypatch.setenv("MATTERMOST_URL", "https://chat.example.invalid")
    monkeypatch.setenv("MATTERMOST_TOKEN", "synthetic-token")
    monkeypatch.setenv("HARNESS", "codex")
    monkeypatch.setenv("AGENT_COMMAND", "synthetic-native")
    monkeypatch.delenv("MATTERMOST_COLLAB_CLI_COMMAND", raising=False)
    monkeypatch.delenv("MATTERMOST_FLEET_FILE", raising=False)
    monkeypatch.delenv("MATTERMOST_ROLE_FILE", raising=False)
    return lambda: Config.load("/dev/null")


def selected_cli(tmp_path):
    selected = tmp_path / "collab CLI ' $(touch injected)"
    selected.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "with open(os.environ['RECEIPTS'], 'a') as stream:\n"
        "    stream.write(json.dumps(sys.argv[1:]) + '\\n')\n"
    )
    selected.chmod(0o700)
    return selected


@pytest.mark.parametrize("value", [None, "", " \t "])
def test_unset_collaboration_command_keeps_optional_default(base_config, monkeypatch, value):
    if value is not None:
        monkeypatch.setenv("MATTERMOST_COLLAB_CLI_COMMAND", value)
    assert base_config().collab_cli_command is None


@pytest.mark.parametrize(
    "problem",
    ["relative", "missing", "directory", "not-executable", "arguments", "newline", "backtick"],
)
def test_config_refuses_invalid_single_executable_selector(
    base_config, tmp_path, monkeypatch, problem
):
    selected = selected_cli(tmp_path)
    if problem == "relative":
        value = selected.name
    elif problem == "missing":
        value = str(tmp_path / "missing")
    elif problem == "directory":
        value = str(tmp_path)
    elif problem == "not-executable":
        selected.chmod(0o600)
        value = str(selected)
    elif problem == "arguments":
        value = str(selected) + " --extra"
    elif problem == "newline":
        value = str(tmp_path / "line\ncommand")
        Path(value).write_text("synthetic")
        Path(value).chmod(0o700)
    else:
        value = str(tmp_path / "code`span")
        Path(value).write_text("synthetic")
        Path(value).chmod(0o700)
    monkeypatch.setenv("MATTERMOST_COLLAB_CLI_COMMAND", value)
    with pytest.raises(ValueError, match="MATTERMOST_COLLAB_CLI_COMMAND"):
        base_config()


def item(kind):
    return {
        "project": "project-id",
        "project_title": "Synthetic project",
        "id": "request-id",
        "claim": "claim-nonce",
        "sender": "assistant-a",
        "kind": kind,
        "objective": "Verify the selected command",
        "text": "Keep the owner's literal mm-work example unchanged.",
    }


def render(config, kind):
    consumer = CollaborationConsumer.__new__(CollaborationConsumer)
    consumer.gateway = SimpleNamespace(config=config)
    return consumer.prompt(item(kind))


@pytest.mark.parametrize("kind", ["ask", "work", "result"])
def test_prompt_commands_execute_exact_selected_cli_with_preserved_request_fields(
    base_config, tmp_path, monkeypatch, kind
):
    selected = selected_cli(tmp_path)
    monkeypatch.setenv("MATTERMOST_COLLAB_CLI_COMMAND", str(selected))
    config = base_config()
    assert config.collab_cli_command == str(selected)
    prompt = render(config, kind)
    assert item(kind)["text"] in prompt
    assert " -m mattermost_acp.collaboration.cli" not in prompt
    commands = re.findall(r"`([^`]+)`", prompt)
    receipts = tmp_path / "receipts.jsonl"
    for command in commands:
        command = command.replace("<recipient>", "assistant-c")
        assert shlex.split(command)[0] == str(selected)
        subprocess.run(
            ["/bin/sh", "-c", command],
            check=True,
            cwd=tmp_path,
            env={"PATH": os.defpath, "RECEIPTS": str(receipts)},
        )
    received = [json.loads(line) for line in receipts.read_text().splitlines()]
    expected = [["show", "project-id"]]
    if kind in {"ask", "work"}:
        expected += [
            ["reply", "request-id", "--claim", "claim-nonce", "--text", "<result and evidence>"],
            [
                "ask",
                "project-id",
                "assistant-c",
                "--parent",
                "request-id",
                "--text",
                "<specific request>",
            ],
        ]
    assert received == expected
    assert not (tmp_path / "injected").exists()


def test_unconfigured_prompt_retains_absolute_python_module_fallback(base_config, monkeypatch):
    selected_python = "/synthetic/virtual environment/python"
    monkeypatch.setattr(sys, "executable", selected_python)
    prompt = render(base_config(), "ask")
    for command in re.findall(r"`([^`]+)`", prompt):
        assert shlex.split(command)[:3] == [
            selected_python,
            "-m",
            "mattermost_acp.collaboration.cli",
        ]
