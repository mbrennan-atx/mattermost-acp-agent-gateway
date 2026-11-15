# Acceptance evidence

## Local implementation checks

- Full suite: 375 passed, one skipped in 40.20 seconds before the final ownership/identity regressions. Final checks are recorded below. The skip is the optional installed Codex adapter fixture.
- Collaboration suite: 31 passed before the final ownership/identity regressions. It covers durable restart, actor authentication, verified mention provenance, two independent native-session gateways, unsupported and ambiguous steering, active injection with correlated reply, saturated-thread serialization, dependency and dispatch limits, task ownership, lease/attempt fencing, pre-dispatch cancellation, replaced-session preservation, completion racing cancellation and blocked-result evidence.
- Ruff: passed across the repository.
- Wheel and source distribution: built successfully. Both passed the existing private identifier and runtime-file policy (51 wheel entries, 141 source entries; built from the source archive).

## Requirement mapping

FR-001 through FR-004: transactional project state, CLI operations, real-post verification and quiet note/answer routing.
FR-005 through FR-008: consumer steering/queue/uncertainty behavior, native-thread locks, interrupted leases, claim nonces and idempotent result receipts.
FR-009 and FR-010: project limits, dependency dispatch, cancellation controls, final dispatch check and recorded native terminal evidence.
FR-011 and FR-012: optional consumer settings, independent gateways, unchanged provider/connector configuration, explicit workspace binding and mismatch rejection.
FR-013: root AGENTS.md.

## Release acceptance still required

A first full-branch Sol review identified packaging, cancellation, workspace, recovery, identity and idempotency defects. These were fixed with regression checks. Final-source full-branch review, committed-source export parity, exact-source hosted CI, immutable installation and live pilot verification remain required before claiming deployment acceptance. Runtime identities, configuration snapshots, native transcripts and rollout receipts belong in private deployment evidence outside this public repository.

SC-001 through SC-003 require that live evidence. Local tests establish implementation behavior, not fleet activation or production capacity.

## Final local checks before release review

- Full suite: 376 passed, one optional adapter fixture skipped, 40.18 seconds.
- Collaboration contracts: 32 passed, including ordinary-turn cancellation preservation and real recipient identity distinct from actor labels.
- Ruff and build passed. The wheel was built from the source archive and both passed private release policy, with 51 wheel and 141 source entries.
- Convergence checked 13 functional requirements, three success criteria, eight plan components and five constitution principles. No new implementation gap remains. T015-T017 retain final review, hosted CI, immutable installation and live acceptance.
- First full-branch review and synthesis used gpt-6.1-sol/high. All supported findings were addressed, including the synthesis's ordinary-user-turn ownership and recipient-label findings. A fresh full-branch review of this committed source is required before push.

## Lifecycle regression checks

- Python 3.12.15 with locked dependencies: 380 passed, one optional fixture skipped, 43.72 seconds.
- Successful requests now require an explicit correlated reply, including quiet native goals. Foreground output remains evidence rather than completion.
- Additional contracts cover a pending-work check under the native-thread lock, background activity becoming pending again, running-request native cancellation, and peer evidence surviving result requeue.
- All nine supported findings from the second Sol review were addressed. Final-source review and hosted CI remain required.

## Guarded controls and durable creation

- Python 3.12.15 and locked dependencies: full suite 410 passed with one optional fixture skipped in 44.40 seconds. Two additional evidence/cancellation regressions bring the focused new suite to 32 passing tests.
- The third Sol review's 12 supported findings have fixes and regression coverage: installed command paths, retry ownership, guarded native controls, interrupted cancellation, injected failure, early-reply evidence, attempt artifacts, bounded descendant dispatch, durable creation, explicit-root conflicts and conflicting late replies.
- Evidence enrichment updates the existing result delivery without duplicating it. Foreground observation continues after an injected early reply until its actual receipt is available.
- Maintenance reports all seven activity counters and atomically seals drained idle admission. Startup recovery and delayed harness-control tasks are included.
- Final committed-source review, hosted CI, export/install parity and live activation remain pending.
