from unittest.mock import AsyncMock

import pytest

from mattermost_acp.acp.broker import AcpBroker, AcpError
from mattermost_acp.commands.cursor_models import model_effort, with_effort


def test_effort_preserves_other_model_parameters():
    assert (
        with_effort("gpt-5.6-sol[context=272k,reasoning=medium,fast=false]", "high")
        == "gpt-5.6-sol[context=272k,reasoning=high,fast=false]"
    )
    assert model_effort("glm-5p3[reasoning=high]") == "high"
    with pytest.raises(AcpError):
        with_effort("composer-2.5[fast=false]", "high")


@pytest.mark.asyncio
async def test_cursor_uses_config_and_tracks_resolved_model():
    b = AcpBroker(command="cursor-agent", backend="cursor")
    assert b._build_argv() == ["cursor-agent", "acp"]
    b.request = AsyncMock(
        return_value={"configOptions": [{"id": "model", "currentValue": "glm-5p3[reasoning=high]"}]}
    )
    assert (
        await b.set_model("s", "glm-5p3[reasoning=medium]", effort="high")
        == "glm-5p3[reasoning=high]"
    )
    b.request.assert_awaited_once_with(
        "session/set_config_option",
        {"sessionId": "s", "configId": "model", "value": "glm-5p3[reasoning=high]"},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method,outcome", [("cursor/ask_question", "skipped"), ("cursor/create_plan", "rejected")]
)
async def test_dialogs_return_to_chat_without_inventing_approval(method, outcome):
    b = AcpBroker(backend="cursor")
    b._write = AsyncMock()
    await b._dispatch({"jsonrpc": "2.0", "id": 7, "method": method, "params": {}})
    result = b._write.call_args.args[0]
    assert result["id"] == 7
    assert result["result"]["outcome"]["outcome"] == outcome
    assert "user's next message" in result["result"]["outcome"]["reason"]


def test_glm_stamp():
    from mattermost_acp.mm.status import format_model_stamp

    assert (
        format_model_stamp("glm-5p3[reasoning=high]", effort="high")
        == "> *Model: GLM 5.3 · Effort: high*"
    )


@pytest.mark.asyncio
async def test_cursor_load_only_once_per_process():
    b = AcpBroker(backend="cursor")
    b.request = AsyncMock(return_value={"sessionId": "new"})
    await b.session_new("/tmp")
    b.request.reset_mock()
    await b.session_load("new", "/tmp")
    b.request.assert_not_awaited()
    await b.session_load("persisted", "/tmp")
    await b.session_load("persisted", "/tmp")
    b.request.assert_awaited_once_with(
        "session/load", {"sessionId": "persisted", "cwd": "/tmp", "mcpServers": []}
    )
    await b.stop()
    b.request.reset_mock()
    await b.session_load("new", "/tmp")
    b.request.assert_awaited_once()
