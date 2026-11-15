# Acceptance evidence

## Current local checks

- Python 3.12.15 with locked dependencies: 614 tests pass, one optional installed Codex adapter fixture skipped, 87.63 seconds.
- Ruff passes across the repository. Formatting passes for all changed Python files. Existing formatting differences in untouched files are outside this change.
- Focused continuation, result, creation, delegation and dispatch checks pass. Regression coverage includes actual broker generations, pre-write loaded-session loss, predecessor work after stop, original owner result routing and cancellation during creation/read-back or hub recovery.
- Both reports and synthesis from the preceding full-branch Sol/high reviews were inspected. All supported findings have implementation fixes and meaningful regressions. The latest report and synthesis have three additional fixes: cancellation and quota accounting for retained native execution after an early reply, normalized creation retries and inclusion of admission regressions in source archives. The 192 affected checks and seven byte-verified extracted-source admission tests pass. A fresh review of the committed branch precedes publication.
- The prior immutable candidate passed committed-source export, wheel-from-sdist policy checks, parity for all 46 installed Python files and Config imports for seven actual original profiles. The amended source requires a new immutable build before activation.
- Temporary real-profile staging verified seven generated sidecar configurations, enabled workspace guards and eight units through the actual systemd parser. It installed and started no services.

## Requirement mapping

FR-001 through FR-004: transactional project state, CLI operations, real-post verification and quiet note/answer routing.

FR-005 through FR-008: consumer steering, queue and uncertainty behavior, native-thread locks, interrupted leases, claim nonces and idempotent result receipts.

FR-009 and FR-010: project limits, dependency dispatch, cancellation controls, guarded dispatch and recorded native terminal evidence.

FR-011 and FR-012: optional consumer configuration, independent gateways, preserved provider and connector settings, explicit workspace binding and pre-work configuration guards.

FR-013: root AGENTS.md.

## Verified boundaries

Successful work requests require an explicit correlated reply. Foreground output remains evidence; observed pending tools and workers retain native ownership. Further delegation requires an explicit parent; independent home requests cannot borrow a sidecar's native task. Correlated child results continue their parent through the same native session. A rejected continuation restores the parent's cancellation epoch even when preparation advanced it before ownership transfer. Terminal parents retain ownership until their restored native work settles. Correlated reply completion is separate from reserved native execution. Pending executions still consume worker capacity and remain visible to mailbox admission and cancellation. A matching native terminal receipt clears that execution without rewriting the completed result or its artifacts. Project cancellation waits for that settlement.

Registered project requests reject non-member senders and recipients instead of falling through to ordinary wake rules. Creation intent and project keys use the same normalized value across reservation, post receipts and read-back retries. Result-notification recovery does not consume the exhausted work-request budget. Database startup holds both canonical path and inode locks; hard-link aliases cannot initialize a separate SQLite WAL against live state.

Creation intent records its admission timestamp. Origin cancellation retains the latest boundary in the hub, including across restart, so posting/read-back delays cannot move earlier admitted work beyond the stop cutoff. Later projects remain active.

Steering requires a same-harness session successfully loaded by the current broker process, with a final guarded check at the native write. Restart clears loaded-session evidence. Stop suppresses report publication while preserving every pending predecessor observer.

Native project bindings stay on the project discussion, including the owner's. Owner results require originating-thread post read-back; sidecars never load the original owner's native session. Shared cancellation for legacy home gateways uses `mm-work cancel`; their `/stop` does not propagate to sidecars.

Workspace guards run before native startup and session creation or resume. Reused sessions require pre-work configuration evidence; missing receipts and cached generation drift refuse dispatch. These receipts supply no native goal or queue idle proof.

Warmed native clients without authoritative goal and queue idle evidence report `native_boundary_unknown`. Maintenance counts that uncertainty as pending and refuses sealing, retirement or refresh even after foreground and correlated answers finish or ACP exits. Normal collaboration remains available. Cold sidecars can be retired before their first native task.

## Release acceptance still required

SC-001 through SC-003 require live evidence. Local tests establish implementation behavior, not fleet activation or production capacity.

Before claiming deployment acceptance, complete final committed-source review, exact-source hosted CI, a new immutable export/install verification and real actor identity reads. Activate independent project sidecars while preserving original PID/start identities, current databases and actual native/provider/MCP selectors.

The live pilot covers cold Cursor retirement and reapply, durable queued work across an owned hub restart, actual peer answers, busy follow-up delivery, addressed floor requests, owner origin-post read-back, durable CLI receipts, isolated native cancellation and actual native Codex/Gemini exchanges. Verify the original home-shell wrapper independently and leave warmed clients running.

Private runtime identities, credentials, configuration snapshots, native transcripts and detailed rollout receipts remain outside this public repository.
