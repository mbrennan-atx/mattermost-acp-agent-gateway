# Switching harnesses

The selected external inventory maps assistants to environment files and service
units. The selected external harness profiles map `grok`, `claude`, `codex` or
`cursor` to executable, model, path prefixes and provider-specific environment.
See [deployment](deployment.md) for selectors, versioning and relative paths.

```bash
.venv/bin/python scripts/set-harness --list
.venv/bin/python scripts/set-harness assistant-a claude --dry-run
.venv/bin/python scripts/set-harness assistant-a codex --no-restart
```

The tool validates the executable, rewrites managed harness keys, stores an env
backup under `.env-backups` beside that env file, and records the harness in the
inventory. It preserves other environment settings. Without `--no-restart` it
waits for active turns, then restarts the mapped unit. If the wait expires,
settings are already written but the running process is retained. Do not use
`--force` when preserving active work.

The `/harness` command uses the same inventory/profiles. It requires the assistant
to be in that inventory and waits for idle before restarting. Account, channels,
persona and title remain associated with the same assistant. Native sessions and
model IDs do not cross harnesses; the next message starts a new native session.
Switching a harness is different from upgrading gateway code while preserving
the current harness and its sessions.

Model and effort selections use the native adapter catalog. A model name from
one provider is not assumed valid on another. Keep credentials and native
profile roots in private configuration; a profile switch does not authenticate
a client or copy its connector settings.
