# Tasks: Project collaboration

## Phase 1: Setup

- [x] T001 Add repository instructions in AGENTS.md and the constitution in .specify/memory/constitution.md.
- [x] T002 Record requirements and delivery research under specs/001-project-collaboration/.

## Phase 2: Foundation

- [x] T003 Add meaningful transactional store and HTTP contract tests in tests/test_collaboration.py.
- [x] T004 Implement durable projects, bindings, messages, notes, leases and idempotency in src/mattermost_acp/collaboration/store.py.
- [x] T005 Implement authenticated loopback coordination and bounded mailbox waits in src/mattermost_acp/collaboration/hub.py.

## Phase 3: US1 shared work

- [x] T006 [US1] Add create/ask/task/claim/reply/note/show/bind/retry/cancel commands in src/mattermost_acp/collaboration/cli.py.
- [x] T007 [US1] Integrate explicit registered-project floor mentions and result delivery in src/mattermost_acp/mm/gateway.py.
- [x] T008 [US1] Verify two gateway consumers exchange a question and result in tests/test_collaboration.py.

## Phase 4: US2 delivery and recovery

- [x] T009 [US2] Serialize native-thread preparation and execution in src/mattermost_acp/session_service.py and src/mattermost_acp/mm/gateway.py.
- [x] T010 [US2] Implement active-only steering, queue fallback, uncertainty, restart recovery and receipt fencing in src/mattermost_acp/collaboration/consumer.py.
- [x] T011 [US2] Test saturated-thread races, injection, unsupported/ambiguous delivery, restart and duplicate results in tests/test_collaboration.py.

## Phase 5: US3 limits and cancellation

- [x] T012 [US3] Enforce worker/dispatch/depth/deadline limits and evidence-based cancellation in src/mattermost_acp/collaboration/store.py and consumer.py.
- [x] T013 [US3] Propagate origin-thread stop in src/mattermost_acp/mm/gateway.py and test unrelated-session preservation in tests/test_collaboration.py.

## Phase 6: Verification and deployment

- [x] T014 Document capability limits and operation in docs/collaboration.md; include packaged commands and export inputs.
- [ ] T015 Run full suite, Ruff, build/export checks and full-branch local review; record evidence in specs/001-project-collaboration/validation.md.
- [ ] T016 Verify final-source CI and immutable release installation; wire private fleet configuration outside public source.
- [ ] T017 Verify live peer exchange, busy delivery, restart/cancellation and persona preservation; promote private operation notes and session log.

## Dependencies and implementation strategy

Foundation precedes US1. US2 and US3 complete the delivery contract before live activation. CLI and store tests can be developed independently once the contract is fixed; gateway consumer and native-thread locking share a boundary and must be coordinated. No shared-checkout parallel writers are planned. Finish all three stories and run convergence before publication and activation.
