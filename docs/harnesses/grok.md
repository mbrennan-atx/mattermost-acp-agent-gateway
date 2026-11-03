# Grok Build

Select `HARNESS=grok` and set `AGENT_COMMAND` to the installed `grok` executable.
The gateway launches its `agent stdio` mode. Authenticate Grok separately and
select `GROK_HOME` if using a dedicated native profile.

Grok-specific passthroughs include `/session-info`, `/context`, `/compact`,
`/doctor`, `/workflows` and `/workflow`. `/mcps` calls `grok mcp doctor`.
Model/effort use the Grok extension. Active steering is used only if the adapter
advertises it. `GROK_COMMAND`, `GROK_MODEL` and other legacy `GROK_*` gateway
settings remain fallback aliases for their corresponding `AGENT_*` settings.
New deployment files should use the neutral names.

The optional model-router classifier reads its credentials from a Grok model
configuration. That classifier is Grok-specific even though aliases and static
rules can select models for other explicitly configured harnesses.
