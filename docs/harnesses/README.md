# Harness capabilities

All integrations use the same Mattermost transport and thread store. Their
native clients own authentication, connectors, execution permissions and model
availability. Install and authenticate the selected client separately. These
capabilities describe the implementation; installed versions and account access
still determine what a particular deployment can use.

| Capability | Grok Build | Claude Code | Codex | Cursor |
|---|---|---|---|---|
| Launch | `grok agent stdio` | `claude-agent-acp` | `codex-acp` | `cursor-agent acp` |
| Resume/load | Advertised ACP capability | Advertised ACP capability | Advertised ACP capability | Load plus gateway rehydration handling |
| Model/effort | Model extension and effort fields | Config options `model` / `effort` | Config options `model` / `reasoning_effort` | Native model IDs and effort variants |
| Active steering | Only if advertised | `_session/steering` | Requires `promptRequired` capability | Only if advertised |
| Connector status | `grok mcp doctor` | `claude mcp list` | `codex mcp list/get` | `cursor-agent mcp list/list-tools` |
| Passthroughs | Grok catalog | `/context`, `/compact` forwarded | `/context`, `/compact` forwarded | Unavailable |
| Worker events | Render recognized events | Render recognized events | Async-task events prolong collection | Render recognized events |

Session resume/load is negotiated, not inferred from the provider name. The
gateway preserves thread mappings and re-applies model/effort settings after
loading a session. When the adapter cannot resume, the conversation may need a
new native session. Worker rendering only covers events the adapter emits;
it does not imply identical worker or lifecycle features across providers.

Steering never intentionally starts idle work. Unsupported adapters receive an
explicit unavailable response. Codex is additionally checked for the host-owned
idle behavior so completion races cannot launch a detached turn. See
[Codex](codex.md) for the compatibility checker.

Claude/Codex `/context` and `/compact` are forwarded as native slash prompts.
Their meaning depends on the adapter version; the gateway does not implement
those commands or promise a uniform output. Grok workflows, doctor and session
info are rejected on other harnesses. Cursor's interactive plan/question UI is
not implemented by this transport.

Provider notes: [Grok](grok.md), [Claude](claude.md), [Codex](codex.md),
[Cursor](cursor.md).
