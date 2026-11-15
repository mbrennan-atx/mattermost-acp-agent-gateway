# Collaboration API and commands

Loopback HTTP, bearer token mapped to an actor. Runtime configuration supplies token files and actor identities; never put those credentials into source.

POST /projects creates a project. GET /projects lists actor memberships. GET /identity returns the authenticated actor. GET /projects/{id} returns shared state. POST /projects/{id}/bindings binds the current actor. POST /projects/{id}/messages enqueues an ask/work request. POST /projects/{id}/notes adds state without waking models. POST /projects/{id}/cancel requests project cancellation by its owner.

GET /mailbox?instance=...&wait=20 claims one actor delivery with a fenced lease. POST /messages/{id}/update renews, requeues, records uncertainty or reports a terminal result. POST /messages/{id}/reply saves an idempotent participant result. POST /messages/{id}/claim claims an open task. POST /messages/{id}/retry reconciles interrupted/unconfirmed work. POST /cancel-thread cancels projects owned by the actor and linked to the supplied origin thread. POST /mentions converts an authenticated participant's exact addressed floor reply into a durable project question using its post ID.

Packaged mm-work commands: create, list, show, ask, task, claim, reply, note, bind, retry and cancel. Project, request and note creation use payload-bound keys for retries. Other mutations use their target ID and delivery claim as the stable identity. CLI waits are bounded to at most 45 seconds; project state remains queryable when a wait expires.
