# Project constitution

Version: 1.0.0. Adopted: 2026-11-15.

1. Generic code MUST remain independent of private fleet identities and credentials. See [AGENTS.md](../../AGENTS.md).
2. Optional collaboration MUST preserve existing single-assistant transport, harness pins, connectors and conversation state.
3. Requests MUST be durable before execution. Completion and cancellation MUST have explicit evidence. Uncertain delivery MUST remain visible and MUST NOT trigger blind replay.
4. Work MUST serialize per native thread, while independent threads may execute concurrently within configured limits.
5. Tests, build checks and final-source CI MUST support completion claims. Apply the owner's current review policy: GitHub-only code skips automatic BugShark unless explicitly requested. Live rollout MUST preserve busy work and current state.
