# Antigravity (`agy`)

Select `HARNESS=agy` and set `AGENT_COMMAND` to the installed `agy` executable
(or a thin wrapper that forwards all CLI arguments to `agy`). The gateway
launches `agy acp` and negotiates sessions, models and thought levels over ACP.

## Configuration

- `AGENT_COMMAND`: path to `agy` (or a wrapper forwarding arguments to `agy`).
- `AGENT_MODEL`: optional default model ID (for example `gemini-4-neon`).
- `AGENT_EFFORT`: optional default thought level (for example `high`, `medium`,
  `low`, `xhigh`). Applied via the ACP `thought_level` config option.
- `AGENT_ALWAYS_APPROVE`: `1` passes `--dangerously-skip-permissions` to
  `agy acp`; `0` omits that flag and lets ACP permission requests follow the
  gateway approval handler.
- `AGY_CLI_HOME`: optional profile directory passed as `--gemini-dir <path>`.
  When set, `--app-data-dir` defaults to `antigravity-cli` unless overridden.
- `AGY_APP_DATA_DIR`: optional subdirectory name passed as `--app-data-dir`.
- `AGY_DISABLE_TELEMETRY`: set to `1` to pass `--disable-telemetry`.
- `AGY_AUTH_METHOD`: optional ACP `authenticate` method override
  (`oauth-personal`, `oauth-business`, `gemini-api-key`, `agent-platform` or
  `none`). When unset, the gateway selects `gemini-api-key` if `GEMINI_API_KEY`
  or `GOOGLE_API_KEY` is present, `agent-platform` if
  `GOOGLE_GENAI_USE_VERTEXAI=1`, and `oauth-personal` otherwise.
- `AGY_MCP_SERVERS_FILE`: optional path to a JSON list of ACP `mcpServers`
  entries supplied on `session/new`, `session/load` and `session/resume`.
  Native MCP entries configured inside the Antigravity profile continue to load
  through `agy` itself.

## Capabilities and limits

- Model selection (`/model`) and effort (`/effort`) use ACP
  `session/set_config_option` (`model` and `thought_level`).
- `/mcps` lists servers from `AGY_MCP_SERVERS_FILE` when configured, otherwise
  runs `agy mcp list`.
- Grok passthrough commands (`/context`, `/compact`, workflows) are unavailable.
- Authenticate the Antigravity profile separately (for example via `agy acp
  --login` or API-key / proxy environment variables) and preserve its profile
  directory across gateway upgrades.
