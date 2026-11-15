# Implementation plan: Project collaboration

## Technical context

Python 3.12, existing aiohttp transport and SQLite runtime persistence. Add an optional loopback collaboration hub, a gateway consumer and a packaged mm-work command. No inference or MCP routing change. Native steering remains capability-gated.

## Constitution check

Generic source contains synthetic examples only. Private deployment supplies actor credentials, roster and instructions. Collaboration is disabled without explicit environment selectors. Durable receipts, bounded dispatch and native-thread locks are required. Candidate releases are immutable; busy gateways activate only at verified idle boundaries.

## Design

The hub owns project membership, participant thread/workspace bindings, tasks/questions, result receipts and notes. Per-actor bearer credentials map to the declared deployment roster. Gateways long-poll their mailbox, claim requests with instance-fenced leases, try active-only steering, and otherwise use a serialized normal continuation. Ambiguous injection remains unconfirmed. Leases that expire become interrupted; explicit retry follows reconciliation.

Results are saved before their notification is delivered to the requester. Every request instructs the participant to submit an explicit correlated mm-work reply, including background work. Successful fresh turns also require that explicit reply because foreground return does not prove native-goal completion. Foreground output is retained as evidence, with a private full-text artifact for long answers. Native turn tokens and exclusive worker bindings fence cancellation; owner-directed deliveries never stop the owner conversation. State-only notes do not wake models. Cancellation sends control deliveries even when worker limits are full, and active work remains cancelling until a native terminal result is verified.

Owner-created projects use one floor discussion root, with an optional owner binding to the originating home thread. Other participants retain independent native sessions in that floor root. Explicit mentions by registered participants in this project thread become durable questions. Normal floor threads retain their previous wake policy.

## Components and phases

1. AGENTS.md, constitution, spec, plan, research, data model, contracts and tasks.
2. collaboration/store.py: transactional durable state and limits.
3. collaboration/hub.py: authenticated loopback API, mailbox long polling and expiry.
4. collaboration/consumer.py: gateway delivery, steering, continuation, receipts and cancellation.
5. collaboration/cli.py: create, ask, task, claim, reply, note, show, retry, bind and cancel commands.
6. mm/gateway.py and session_service.py: optional lifecycle integration, explicit project mentions, native-thread serialization and stop propagation.
7. tests/test_collaboration.py: real HTTP/store and gateway integration scenarios, including crash and race cases.
8. docs/collaboration.md, package/release export manifest and private deployment wiring.

## Validation and release

Meaningful unit/integration tests, full existing suite, Ruff, wheel/build/export checks, local full-branch BugShark, exact-source CI and immutable release installation. Verify a live two-persona exchange, busy steering, durable restart and isolated cancellation. Compare actual model/connector configurations before and after. Activate the executing gateway with an independent finalizer after its own native completion.
