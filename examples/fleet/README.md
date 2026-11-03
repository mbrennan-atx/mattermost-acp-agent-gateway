# Synthetic mixed fleet

These examples contain no real accounts or credentials. Copy them into your
private configuration directory and replace the IDs, token paths, workspaces
and executable choices. Rename env files and update their roster references.
Each assistant needs its own token, home channel, state database and listener
port. Shared instruction files can be reused across harnesses.

The versioned roster resolves env/role paths relative to itself, so its shape
can be validated offline before substituting live resources. For example, from
the checkout root:

```bash
uv run python -c 'from pathlib import Path; from mattermost_acp.deployment import load_inventory; load_inventory(Path("examples/fleet/roster.json").resolve())'
```

The example selects Codex for assistant-a and Claude for assistant-b. Protection
belongs to the entry, not its name. Omitted `owned_paths` means retirement retains
its files. Configure slash tokens before enabling the example listener ports.
