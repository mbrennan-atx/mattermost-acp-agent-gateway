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

## Sidecar delivery and race convergence

- The next rollout adds independent project sidecars alongside legacy home gateways. It does not replace original processes or load original owner sessions. Native project bindings stay on the discussion; owner results require origin-post read-back.
- Python 3.12.15 with locked dependencies: 429 full-suite tests pass, one optional adapter fixture skipped, 50.73 seconds; Ruff passes. 49 focused regression tests pass. New checks cover cancellation during delayed reservation, guarded fresh dispatch after epoch/cancel changes, cancellation during drain, attempt-safe worker callbacks, durable origin-stop retry after local failure, reply-root rejection, inactive early-result observation, background reopening during preparation, actor/user/mode mismatch and verified sidecar result delivery.
- Shared cancellation for this deployment uses `mm-work cancel`; legacy home `/stop` does not gain project propagation. Original provider/CLI/connector settings are inherited, with distinct project databases and maintenance listeners.
- All 11 supported findings from the guarded-controls review have fixes. Full-branch review of this source, final CI and live acceptance remain required.

## Sidecar isolation convergence

- Python 3.12.15 with locked dependencies: 441 tests pass, one optional adapter fixture skipped, 51.47 seconds. Ruff passes.
- All nine supported sidecar-source review findings are fixed. 61 focused regressions cover native cancellation for owner project work, updates before foreground subscription, uncertain ownership in maintenance, nested owner continuation, request ownership under the thread lock, discussion-only bindings with workspaces, installed CLI module invocation, offline message expiry and bounded terminal previews with durable full-output artifacts.
- Ordinary turns retain no terminal answer cache. Project and pending-goal receipts remain while needed and are released after ownership/background work settles.
- Full-branch review of this committed source, exact-source CI, immutable build/install parity and live acceptance remain required.

## Correlated continuation and native boundary checks

- Python 3.12.15 with locked dependencies: 477 tests pass, one optional adapter fixture skipped, 57.18 seconds. Ruff passes. The two new review suites contain 36 regressions covering queryable project expiry, exclusive discussion bindings, cancellation cutoffs, wait deadlines, validation, parent continuation, ambiguous reply cleanup and maintenance boundaries.
- Correlated child results resume a retained parent through the same native session after its foreground work settles. Parent execution and epoch evidence follow the continuation, while unrelated requests remain queued. Cancellation waits through the ownership handoff and cannot use a pre-dispatch rejection as proof that older native work stopped.
- Warmed native clients without authoritative goal and queue idle evidence report native_boundary_unknown. Maintenance counts this as pending background work and refuses sealing, retirement or refresh. This preserves their running work while ordinary collaboration remains available. Cold sidecars can be retired before their first native task.
- Durable origin-stop retries preserve their original project creation cutoff, including reconstruction and overlapping newer stops. Delayed restart tasks remain counted through exit. CLI wait deadlines bound every request and sleep.
- All nine supported findings from the previous full-branch review have implementation fixes and meaningful regressions. A fresh review of this committed source, final-source CI, immutable installation and live acceptance remain required.

## Dispatch admission and stop convergence

- Python 3.12.15 with locked dependencies: 496 tests pass, one optional adapter fixture skipped, 62.57 seconds. Ruff and diff checks pass. Nineteen new regressions use real broker notifications, guarded writes, local HTTP hubs and fake native processes.
- Native notifications and initial worker snapshots update reports synchronously before guarded dispatch. Queued callbacks do not reapply older pending states. An idle previous reporter remains observed during guarded preparation, preserving new background work when dispatch is rejected.
- Origin stop intent is captured and persisted before awaiting native cancellation, and hub cancellation runs independently. Automatic harness restart checks all native/project counters, excludes only its own deciding control and closes admission before verified idle shutdown. Warmed uncertainty remains after ACP process exit.
- Addressed floor prefixes match the hub with leading whitespace and additional mentions in the question. Task claim retries return the current task for the same actor without changing its native claim. Creation retries validate the configured floor.
- Both findings and synthesis from the last Sol/high review were inspected; all seven supported findings have implementation fixes and meaningful regressions. Fresh full-branch review, exact-source CI, final immutable installation and live acceptance remain required.
