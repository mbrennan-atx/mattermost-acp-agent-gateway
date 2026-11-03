# Mattermost ACP Gateway

Run an agent harness in Mattermost. Each thread has an independent native session,
with streamed progress, cancellation, model selection and persistent thread mappings.
The gateway supports Grok Build, Claude Code, Codex and Cursor through ACP. The
operator supplies accounts, credentials, workspace permissions and optional personas.

## Run one assistant

Use Python 3.12 or newer, `uv`, a reachable Mattermost server, and a dedicated
Mattermost user account with a personal access token. Install and authenticate
one of the [supported harnesses](docs/harnesses/README.md) separately.

From a checkout:

```bash
uv sync --locked --group dev
mkdir -p ~/.config/mattermost-acp-gateway ~/.local/state/mattermost-acp-gateway
cp examples/single.env.example ~/.config/mattermost-acp-gateway/assistant.env
chmod 600 ~/.config/mattermost-acp-gateway/assistant.env
```

Edit the copied file. Set your server, channel and allowed-user IDs, token-file
path, workspace, harness and executable. Store the token in its own private file
with mode `0600`. Invite the assistant account into the selected channel.
Choose `AGENT_ALWAYS_APPROVE` explicitly; see the access behavior below.

```bash
MATTERMOST_DOTENV="$HOME/.config/mattermost-acp-gateway/assistant.env" \
  uv run mattermost-acp-gateway --check
MATTERMOST_DOTENV="$HOME/.config/mattermost-acp-gateway/assistant.env" \
  uv run mattermost-acp-gateway
```

`--check` reads configuration, validates selected files and executable paths,
and exits without connecting, launching a harness, or creating state. A single
assistant needs no roster, shared instructions, role file or harness-profile file.
Post a message in the configured channel, then reply in its thread.

The installed wheel exposes the same `mattermost-acp-gateway` command and needs
no checkout. Administration utilities under `scripts/` are checkout-only.

## Access and approvals

Set `MATTERMOST_CHANNEL_ID` to one channel or a comma-separated list. Empty,
`all`, `*` or `any` means every channel the account can see. In that mode,
auto-join defaults on for public team channels. Set
`MATTERMOST_AUTO_JOIN_CHANNELS=0` to disable it. Private channels require an
invitation. Excluded channels are rejected for ordinary home-channel traffic.

`MATTERMOST_ALLOW_USER_IDS` restricts ordinary conversations. Empty means any
user in scope. Direct messages from explicitly allowlisted users also wake the
assistant, independently of the home-channel list. Optional peer-channel routing
has separate semantics: a root post mentioning exactly that assistant wakes it,
including posts from peers outside the ordinary allowlist. Peer thread replies
only wake it for an allowlisted user under the documented mention/starter rules.
Leave `MATTERMOST_PEER_CHANNEL_ID` unset to disable peer routing.

`AGENT_ALWAYS_APPROVE=1` automatically accepts ACP permission requests.
`0` cancels those requests unless an application-provided permission handler
answers them. The legacy default is `1`; the supplied example explicitly selects
`0`. This setting does not revoke native credentials or permissions already
granted by the harness. Configure its native sandbox/approval mode deliberately.
Cursor interactive questions/plans receive skipped/rejected responses because
this transport does not implement that UI.

## Commands

`/help` describes commands for the selected harness. Gateway commands include
`/new`, `/clear`, `/status`, `/stop`, `/hint`, `/model`, `/effort`, `/mcps`,
`/harness` and `/restart`. Restart and harness switching require a configured
systemd deployment. `/hint` requires an active turn and advertised adapter support.
An acknowledgement means delivered, not that the agent has acted on it.

Mattermost built-in command names may conflict with `/help` and `/status`.
Register team commands such as `/agent-help`, `/agent-status` or `/agent <verb>`
against your gateway's `/slash` endpoint. Supply the returned slash tokens through
`MATTERMOST_SLASH_TOKEN_FILE` or explicit token settings. Port `0` disables the
HTTP listener; ordinary chat uses the WebSocket connection. Do not publish tokens.

Grok-specific passthroughs, native connector status and model/effort behavior
are described in the [capability guide](docs/harnesses/README.md). A provider's
connectors or authentication are never inherited from another provider.

## Optional fleets and customization

Keep roles, profiles, inventory and helper destinations in a separate private
deployment directory or repository. Keep secrets, environment files and runtime
state outside both public source and private configuration Git history.
See [deployment and upgrades](docs/deployment.md),
[harness switching](docs/harness-switching.md),
[synthetic fleet examples](examples/fleet/README.md) and
[optional model routing](docs/model-routing.md).

## Development and release preparation

```bash
uv run pytest -q
uv run ruff check .
uv build
```

The native Codex steering check is opt-in with `CODEX_ACP_TEST_BUNDLE` pointing
to the installed adapter's `dist/index.js`; it executes extracted methods with
stubbed sessions and does not call a model. Tests otherwise use synthetic
transport, ACP and configuration inputs.

[Release preparation](docs/release.md) describes the explicit file manifest,
fresh-history export, package checks and private identifier policy. Exporting a
candidate does not publish it or change any repository's visibility.

See [LICENSE](LICENSE) for attribution and terms.
