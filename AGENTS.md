# Agent instructions

This repository contains the generic Mattermost ACP gateway. Read this file before changing source, tests, commands or deployment behavior. More specific instructions in a nested AGENTS.md take precedence for that directory.

## Architecture

- Keep Mattermost transport, ACP session handling and optional collaboration independent of inference providers. Preserve configured models, effort, connectors and native conversation state during changes.
- Each assistant runs its own gateway process. Collaboration routes explicit project messages between those processes; it does not combine their harnesses or share native sessions.
- Keep deployment identities, real channel IDs, credentials, environment files, transcripts and runtime databases outside this repository. Public examples and tests use synthetic values. Do not merge private deployment history into this repository.
- Optional features must leave single-assistant operation working without a roster, collaboration service or new credentials.
- Persist collaboration requests before delivery. Distinguish acceptance, execution, completion and cancellation. Never treat a cleared busy marker, an HTTP success or a running process as proof that work completed.
- Serialize work for each native thread. Use active-only steering for ongoing sessions; preserve unconfirmed delivery rather than automatically repeating a potentially accepted request.
- Keep project work bounded and preserve unrelated sessions when cancelling. Ordinary acknowledgements and status edits must not start model turns.
- Project elapsed budgets require persisted boot/process evidence from creation admission. Clock rollback cannot extend them; missing recovery evidence closes work admission while preserving native execution debt. Stop retries must preserve unrelated mailbox progress, and new terminal settlement requires reservation-linked evidence.

## Development

- Inspect Git status first and use an isolated branch or worktree. Preserve other writers' changes and stage only owned paths.
- For substantial features, use the installed Spec Kit workflow. Keep requirements, plans, tasks and acceptance evidence under specs/. Existing user authorization governs which stages may proceed.
- Run `uv sync --locked --group dev`, relevant tests, `uv run ruff check .`, and package/release checks appropriate to the change. Before completion, run the full suite and verify CI at the final source commit.
- Code changes require the locally configured BugShark full-branch review before pushing. Read the findings, fix valid in-scope issues and repeat relevant checks and review. Use the configured launcher with `gpt-6.1-sol` for both review and synthesis. A successful exit alone does not establish a clean review.
- Keep documentation plain and precise. State verified behavior and material capability limits; avoid promises of exactly-once execution across native-process crashes.

## Deployment

- Build immutable candidate releases outside running installations. Never run dependency synchronization inside a pinned live release or overwrite its files.
- Inspect effective service overrides and actual runtime settings. Preserve credentials, provider/model pins, connectors, identities and the newest state.
- Activate gateways only at verified idle boundaries. Account for pending native goals, tools, workers and accepted inbound/control requests, not just foreground turns. When a legacy gateway cannot prove these boundaries, add an independent project sidecar and preserve the original process. Never treat inferred legacy idleness as permission to terminate it.
- Sidecars require distinct project databases and maintenance listeners. Preserve actual provider/CLI/connector selectors, verify the hub actor against the Mattermost user, and keep every native project binding in the project discussion. Owner results are verified Mattermost posts and CLI receipts; a sidecar must never resume the original owner's native session.
- A deployment can select one trusted executable with AGENT_WORKSPACE_GUARD_COMMAND. Run its configuration check before native session creation or resume, preserve prior evidence on refusal, and audit each session's actual workspace against its pre-work receipt. Configuration evidence does not establish native goal or queue idleness.
- Warmed native clients without authoritative goal and queue idle evidence report an unknown boundary. Refuse seal, retirement or refresh and leave them running. Foreground completion, cleared tools or a caller assertion cannot prove that boundary. Normal collaboration remains available; cold sidecars can be retired before their first native task.
- Rehearse rollback using the previous executable and current state. Do not restore historical runtime databases or replay completed work.
- Verify real delivery, session continuity and cancellation after activation. Read back any published deployment metadata before claiming success.
