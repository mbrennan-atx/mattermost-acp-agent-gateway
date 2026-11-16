# Delivery research

Decision: Use an optional local durable coordination hub and explicit mailbox events.
Rationale: Existing gateways already maintain separate provider/harness identities and native sessions; a coordination service adds shared work without changing those routes.
Alternatives: Wake every peer reply, which introduces acknowledgement loops and lacks durable receipts; shared-file polling alone, which offers weaker delivery control.

Read-only research worker delivery_design, inherited Sol 6.1/high, verified session_service.py, acp/broker.py and mm/reporting.py. No tests or source changes were made by that worker. Its completion report established that foreground busy state excludes continuing native goals, steering timeout is ambiguous, cancellation notification is not terminal evidence, and same-thread preparation can race before global semaphore acquisition. Parent implementation must cover each finding with acceptance tests and final verification.
