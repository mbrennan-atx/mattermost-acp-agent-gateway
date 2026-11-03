import json

import pytest

from mattermost_acp.deployment import (
    DeploymentError,
    document_base,
    load_inventory,
    load_profiles_document,
    read_document,
    resolve_path,
    selected_path,
)


def write(path, **fields):
    path.write_text(json.dumps({"schema_version": 1, **fields}))
    return path


def test_config_selection_and_literal_paths(tmp_path, monkeypatch):
    root = tmp_path / "private deployment"
    monkeypatch.setenv("MATTERMOST_CONFIG_DIR", str(root))
    monkeypatch.setenv("MATTERMOST_ROSTER_FILE", "fleet/roster.json")
    assert selected_path("roster") == root / "fleet/roster.json"
    assert resolve_path("$(touch unwanted)", root) == root / "$(touch unwanted)"
    assert selected_path("roster", tmp_path / "explicit") == tmp_path / "explicit"


def test_inventory_relocation_and_shared_role(tmp_path):
    folder = tmp_path / "outside checkout"
    folder.mkdir()
    (folder / "role.md").write_text("Shared instructions")
    (folder / "a.env").write_text("MATTERMOST_SLASH_PORT=8001\n")
    (folder / "b.env").write_text("MATTERMOST_SLASH_PORT=8002\n")
    path = write(
        folder / "roster.json",
        agents=[
            {"username": "alpha", "env_file": "a.env", "role": "role.md"},
            {"username": "beta", "env_file": "b.env", "role": "role.md"},
        ],
    )
    before = load_inventory(path)
    moved = tmp_path / "moved"
    folder.rename(moved)
    assert load_inventory(moved / path.name) == before


@pytest.mark.parametrize("key", ["username", "user_id", "home_channel_id", "unit"])
def test_reject_identity_collisions_without_echoing_values(tmp_path, key):
    agents = [{"username": "alpha"}, {"username": "beta"}]
    for agent in agents:
        agent[key] = "private-value"
    with pytest.raises(DeploymentError, match=f"Duplicate agent {key}") as err:
        load_inventory(write(tmp_path / "roster.json", agents=agents))
    assert "private-value" not in str(err.value)


def test_reject_duplicate_ports(tmp_path):
    for name in ["a", "b"]:
        (tmp_path / f"{name}.env").write_text("MATTERMOST_SLASH_PORT=8001\n")
    path = write(
        tmp_path / "roster.json",
        agents=[{"username": name, "env_file": f"{name}.env"} for name in ["a", "b"]],
    )
    with pytest.raises(DeploymentError, match="Duplicate agent slash port"):
        load_inventory(path)


@pytest.mark.parametrize("version", [None, 2, True, "1"])
def test_versions_require_explicit_legacy_opt_in(tmp_path, version):
    path = tmp_path / "roster.json"
    data = {"agents": []}
    if version is not None:
        data["schema_version"] = version
    path.write_text(json.dumps(data))
    with pytest.raises(DeploymentError, match="schema_version"):
        load_inventory(path)
    if version is None:
        assert load_inventory(path, allow_legacy=True) == data
        assert document_base(path, data) == path.parent.parent


def test_omitted_role_and_selected_missing_role(tmp_path):
    path = write(tmp_path / "roster.json", agents=[{"username": "assistant"}])
    assert load_inventory(path)["agents"]
    write(path, agents=[{"username": "assistant", "role": "absent.md"}])
    with pytest.raises(DeploymentError, match="role is missing"):
        load_inventory(path)


def test_unreadable_and_malformed_documents_do_not_echo_content(tmp_path):
    path = tmp_path / "roster.json"
    with pytest.raises(DeploymentError):
        read_document(path)
    path.write_text('{"secret": "private-value" invalid}')
    with pytest.raises(DeploymentError) as err:
        read_document(path)
    assert "private-value" not in str(err.value)


def test_profiles_validate_types_and_preserve_explicit_settings(tmp_path):
    profiles = {"custom": {"command": "/some path/adapter", "env": {"MODE": "test"}}}
    path = write(tmp_path / "profiles.json", harnesses=profiles)
    assert load_profiles_document(path)["harnesses"] == profiles
    profiles["custom"]["env"]["MODE"] = 1
    write(path, harnesses=profiles)
    with pytest.raises(DeploymentError, match="env must map"):
        load_profiles_document(path)


def test_explicit_null_is_not_legacy(tmp_path):
    path = tmp_path / "roster.json"
    path.write_text('{"schema_version": null, "agents": []}')
    with pytest.raises(DeploymentError, match="schema_version"):
        load_inventory(path, allow_legacy=True)


def test_shared_and_owned_paths_require_lists(tmp_path):
    path = write(tmp_path / "roster.json", shared_paths="not-a-list", agents=[])
    with pytest.raises(DeploymentError, match="shared_paths"):
        load_inventory(path)
    write(path, agents=[{"username": "assistant", "owned_paths": [False]}])
    with pytest.raises(DeploymentError, match="owned_paths"):
        load_inventory(path)
