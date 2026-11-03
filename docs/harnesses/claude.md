# Claude Code

Select `HARNESS=claude` and set `AGENT_COMMAND` to the installed
`claude-agent-acp` executable. Authenticate the native Claude CLI separately.
Use `CLAUDE_CONFIG_DIR` when selecting a private native profile. The adapter and
native `claude` command must be available to the service's PATH.

The gateway sets `model` and `effort` config options, uses advertised session
resume/load, and supports active steering when advertised. `/mcps` calls
`claude mcp list`. Connector configuration and permissions are native Claude
settings; no other provider's login is needed or copied.
