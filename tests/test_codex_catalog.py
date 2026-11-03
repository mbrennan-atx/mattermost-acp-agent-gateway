from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from mattermost_acp.acp import AcpBroker
from mattermost_acp.mm.gateway import MattermostGateway


async def test_codex_directive_uses_adapter_catalog():
    broker = SimpleNamespace(session_config_options={
        'sid': [{'id': 'model', 'options': [
            {'value': 'gpt-6-astra'}, {'value': 'gpt-6-sol'},
        ]}],
    })
    sessions = SimpleNamespace(
        broker=broker,
        ensure_session=AsyncMock(return_value='sid'),
        switch_model=AsyncMock(),
    )
    config = SimpleNamespace(harness='codex', grok_model='gpt-6-astra')
    gateway = MattermostGateway(config, SimpleNamespace(), sessions)
    await gateway._apply_model_directive('thread', 'channel', 'sol', 'medium', 'thread')
    sessions.switch_model.assert_awaited_once_with(
        'thread', 'gpt-6-sol', effort='medium', scope='thread', channel_id='channel'
    )


@pytest.mark.parametrize('operation', ['new', 'resume', 'load'])
async def test_broker_remembers_adapter_model_options(operation):
    broker = AcpBroker(backend='codex')
    options = [{'id': 'model', 'options': [{'value': 'gpt-6-astra'}]}]
    broker.request = AsyncMock(return_value={'sessionId': 'sid', 'configOptions': options})
    if operation == 'new':
        await broker.session_new('/tmp')
    else:
        await getattr(broker, 'session_' + operation)('sid', '/tmp')
    assert broker.session_config_options['sid'] == options
