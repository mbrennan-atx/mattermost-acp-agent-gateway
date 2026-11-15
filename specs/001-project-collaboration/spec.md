# Feature specification: Project collaboration

Created: 2026-11-15. Status: implementation authorized.

## User scenarios and testing

### US1: Share work and ask peers (P1)

A project owner creates a project with an objective and participating assistants. Participants assign work, ask questions, claim available tasks and contribute decisions and artifact links. Replies reach the requesting assistant without going through a coordinator.

Acceptance: two existing assistants exchange a question and result; the owner sees task ownership, evidence and the result in shared project state. Ordinary acknowledgements trigger no work.

### US2: Deliver into busy sessions and recover (P1)

A busy assistant receives a project question or result in its ongoing session when supported. Otherwise the request waits durably. Restarting the service preserves queued requests and exposes interrupted or unconfirmed execution for reconciliation.

Acceptance: active injection preserves the native session and model; unsupported steering queues; ambiguous injection is visible without automatic resend; completed replies remain idempotent after a lost response.

### US3: Bound and cancel a project (P1)

The owner sets worker-slot, dispatch-count, delegation-depth and elapsed-time limits. A full worker pool permits one descendant loan per running parent, serializes active sibling loans and enforces the hard total ceiling `max_workers * (max_depth + 1)`. Cancelling the project or stopping its originating thread blocks new work and requests cancellation of its active sessions, preserving unrelated work.

Acceptance: limits are enforced across assistants; queued requests cancel immediately; active requests remain cancelling until terminal evidence arrives; unrelated sessions keep running.

## Edge cases

Multiple messages arrive for one busy native thread; the global turn semaphore is saturated; a gateway or hub restarts during execution; a reply arrives twice; cancellation races a reply; an assistant is offline; an available task is claimed twice; project members and credentials are invalid; a delivery times out after possible acceptance; a native goal continues after foreground return.

## Functional requirements

- FR-001: Project state records the objective, owner, participants, tasks, dependencies, decisions, artifact links and limits.
- FR-002: Participants can assign requests, claim available tasks and return substantive results directly to the requester.
- FR-003: Delivery persists before acknowledgement, with a stable request ID and payload-bound idempotency key.
- FR-004: Explicit project messages may start work; acknowledgements, ordinary floor replies and state-only notes do not.
- FR-005: Active-only steering is preferred; unsupported busy delivery remains queued and ambiguous delivery remains unconfirmed.
- FR-006: Each native thread is serialized across normal and collaboration turns, including preparation before global-slot acquisition.
- FR-007: Queued messages survive restart; in-progress work becomes interrupted and requires reconciliation before retry.
- FR-008: Results remain durable and idempotent; completion is separate from delivery acceptance.
- FR-009: Project concurrency, dispatch-count, depth and elapsed limits apply across gateway processes.
- FR-010: Cancellation blocks new claims, reaches project sessions and remains pending until actual termination evidence.
- FR-011: Existing models, effort, connectors, identities, unrelated sessions and single-assistant behavior remain intact.
- FR-012: Participants can bind isolated workspaces and share evidence without editing the same checkout concurrently.
- FR-013: A root AGENTS.md explains architecture boundaries, development verification and deployment rules.

## Success criteria

- SC-001: A live two-assistant exchange completes with a durable request, result and native session provenance.
- SC-002: Restart, busy-delivery, duplicate-result, task-claim and cancellation scenarios pass without silent request loss or unrelated cancellation.
- SC-003: The deployed pilot preserves all existing persona pins and connectors; final-source CI and local review pass.

## Assumptions and scope

Project membership authorizes collaboration within the existing approved objective, not unrelated deployments or messages to humans. Roles are specialties within projects. Default limits are three active workers, 24 dispatched requests, depth two and one hour; owners can explicitly set other positive limits. Dispatch counts bound model activation; exact dollar spend is unavailable across the heterogeneous providers and is reported as unknown rather than invented. Shared state is private runtime data outside source repositories and the vault. Existing private fleet policy will describe when project collaboration applies.

## Key entities

Project; participant binding; work request or question; result delivery; decision/artifact note; delivery receipt; worker instance lease.
