# Data model

Project: id, title, objective, owner, members, floor channel/root, originating channel/root, status, limits, creation/deadline, dispatched count. Membership is fixed at creation; identities must exist in deployment configuration.

Binding: project, actor, native thread root, channel, optional absolute workspace. One binding per actor/project; changes are forbidden while that actor has active requests.

Message: stable id, project, sender, recipient or unassigned, kind (ask/work/result/cancel), text, parent, dependencies, depth, idempotency key, state, instance/lease, result, timestamps. Idempotency keys are scoped to sender and bind the complete semantic payload. Dependencies must be completed before claim.

State: open -> queued -> running -> done/failed/blocked; running -> unconfirmed/interrupted; explicit reconciliation may retry interrupted/unconfirmed; running -> cancelling -> cancelled only on terminal confirmation. Queued/open cancel immediately. Delivery results and cancel controls bypass project worker limits.

Notes: actor, project, text, artifact links and timestamp. Notes are durable, append-only and do not dispatch work.
