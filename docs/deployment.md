# Deployment, customization and upgrades

The gateway source, private deployment inputs and runtime state have separate
lifetimes. A code update should not replace a persona, credential or session store.

## Selection and paths

`MATTERMOST_CONFIG_DIR` selects the private configuration root. Otherwise use
`$XDG_CONFIG_HOME/mattermost-acp-gateway`, or
`~/.config/mattermost-acp-gateway` when XDG is unset.

`MATTERMOST_ROSTER_FILE` and `MATTERMOST_HARNESS_PROFILES_FILE` override
`roster.json` and `harnesses.json` under that root. Relative selectors resolve
against the root. A versioned document resolves its referenced files against
its own directory. Absolute paths and `~` are supported; shell expressions
are never evaluated. `MATTERMOST_FLEET_FILE` and `MATTERMOST_ROLE_FILE` select
optional instruction files relative to the configuration root.

Both JSON documents use integer `schema_version: 1`. Existing unversioned
inventories are accepted by compatibility callers with their old references
relative to the inventory's parent's parent. Convert them to version 1 with
explicit paths before moving them. Unknown versions and invalid selected inputs
fail validation. An omitted default fleet file is optional for a single assistant.

Environment loading retains its existing precedence: explicitly passing a file
to `Config.load` overrides environment values. `MATTERMOST_DOTENV=none` disables
implicit dotenv loading. Otherwise a selected dotenv file fills unset values;
without a selector, dotenv discovers `.env`. For services, use an explicit
`EnvironmentFile` plus `MATTERMOST_DOTENV=none` in that file. Runtime database,
log, workspace and attachment paths should be absolute; relative runtime paths
are relative to the process working directory. Set `AGENT_COMMAND` to a single
executable, not a shell command line.

## Personas and fleet inventory

`MATTERMOST_SESSION_RULES_FILE` optionally replaces the common ACP session rules
with an operator-owned file, relative to the config root. Its bytes are preserved
without truncation or whitespace changes. Use it when migrating prior transport
instructions or adding operator-specific session guidance. Keep the gateway's
model/title protocol directives in any replacement that needs those features.
Without this setting, the built-in provider-neutral transport rules apply.

Instruction text is independent of harness selection. Shared instructions are
read first, then the role. Each input is stripped and capped at 4,000 characters,
then joined with a blank line. This preserves the existing prompt composition;
keep the most important instructions within the cap. Selected unreadable inputs
fail rather than silently dropping the role.

An inventory entry uses an arbitrary `username` plus explicit `user_id`,
`home_channel_id`, `unit`, `env_file`, optional `role` and `harness`. Identities,
home channels, service units and active slash ports must be unique. Set
`protected: true` to require the retirement tool's explicit override.
`nudge_enabled` and `config_source_priority` replace named utility exceptions.
Optional `grok_home` applies only to Grok-specific tooling.

Retirement retains data by default. `owned_paths` declares exactly which paths
may be removed for that entry; top-level `shared_paths` protects shared resources.
The tool also protects paths referenced by other assistants and the selected
administrator token. It unlinks symlinks rather than traversing their targets.
Declare shared native homes, workspace roots and credential stores explicitly.
Migration never invokes provisioning or retirement.

`mm-ask` requires `MATTERMOST_SYSADMIN_USERNAME`; `mm-tell` requires
`MATTERMOST_COS_USERNAME`. Both require a peer channel, URL and token file.
They have no implicit destination. The optional peer channel is an intentional
agent-to-agent entry point with different access rules from ordinary chat.

## Checkout-only tools

Run Python administration tools with the checkout virtual environment.
`new-agent.py` requires username, display, workspace and harness, plus explicit
server/team/profile configuration. It previews by default. `--apply` creates or
binds Mattermost resources, `--system-admin` explicitly grants that role, and
`--start` starts the resulting unit. It never copies another native client's
credentials. Existing local env/unit conflicts fail before remote writes;
remote failures may leave partial resources for explicit reconciliation.

`retire-agent.py --username assistant-a` previews. `--yes` stops the unit,
deactivates the user, archives its home channel, removes declared owned files,
and removes its inventory entry. It never requests permanent API deletion and
stops on failure. Both tools require an explicit `MATTERMOST_ADMIN_TOKEN_FILE`
for applied API operations. Set `MATTERMOST_SECRETS_DIR` for provisioning tokens.

`register-hint-command.py` previews registration. Its explicit server, team,
admin-token and slash-token-file arguments select the resources; `--apply`
copies the existing `/stop` endpoint to `/hint` and verifies the result.

## systemd

From the checkout root:

```bash
scripts/install-systemd.sh --user --dry-run --env-file /absolute/private/assistant.env
scripts/install-systemd.sh --user --enable --env-file /absolute/private/assistant.env
```

Preview does not sync dependencies or write files. Rendering without `--enable`
does not start a service. SSH forwarding is optional and only included with
`--tunnel-host` and `--ssh-key`. Configure trusted host keys separately; forwarding
binds the remote loopback interface. Listener/forwarding ports are explicit options.

## Upgrade and rollback

Record current service executable, environment and native profile paths, model
and effort pins, allowlists, approval policy, instruction hashes, thread/session
associations and external consumers before changing a deployment. Preserve
private configuration separately from the new source revision.

Build and validate the new checkout first. Wait until the selected assistant
and its workers are idle. Switch only its executable/configuration references,
start it, and compare the recorded associations plus an existing conversation
before continuing to the next assistant. Keep the executing assistant last
and use a surviving verifier for its restart. A timeout does not authorize
terminating active work.

Rollback switches code and compatible configuration back while retaining the
current state. Never replace a newer database with an old copy. If relocating
SQLite state, stop all writers, check integrity, transfer the database and its
WAL/SHM consistently, verify logical records, then update the absolute path.
Keep native harness stores at their original paths unless their migration is
separately verified. Code updates must not replay historical work.
