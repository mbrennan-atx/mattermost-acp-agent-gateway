# Acceptance evidence

## Local implementation checks

- Full suite: 364 passed, one skipped in 39.90 seconds. The skip is the optional installed Codex adapter fixture.
- Collaboration suite: 20 passed. It covers durable restart, actor authentication, verified mention provenance, two independent native-session gateways, unsupported and ambiguous steering, active injection with correlated reply, saturated-thread serialization, dependency and dispatch limits, task ownership, lease/attempt fencing, pre-dispatch cancellation, replaced-session preservation, completion racing cancellation and blocked-result evidence.
- Ruff: passed across the repository.
- Wheel and source distribution: built successfully. Both passed the existing private identifier and runtime-file policy (45 wheel entries, 123 source entries).

## Requirement mapping

FR-001 through FR-004: transactional project state, CLI operations, real-post verification and quiet note/answer routing.
FR-005 through FR-008: consumer steering/queue/uncertainty behavior, native-thread locks, interrupted leases, claim nonces and idempotent result receipts.
FR-009 and FR-010: project limits, dependency dispatch, cancellation controls, final dispatch check and recorded native terminal evidence.
FR-011 and FR-012: optional consumer settings, independent gateways, unchanged provider/connector configuration, explicit workspace binding and mismatch rejection.
FR-013: root AGENTS.md.

## Release acceptance still required

Full-branch local review, committed-source export parity, exact-source hosted CI, immutable installation and live pilot verification remain required before claiming deployment acceptance. Runtime identities, configuration snapshots, native transcripts and rollout receipts belong in private deployment evidence outside this public repository.

SC-001 through SC-003 require that live evidence. Local tests establish implementation behavior, not fleet activation or production capacity.
