# Deadline and admission followup

Known review findings at 97fe11c require four bounded corrections. Existing project requirements and execution uncertainty remain authoritative.

- Local receipt deadlines and native write admission use Linux BOOTTIME, matching the hub's suspension-inclusive elapsed budget. Cancellation timers recheck that budget within one second of event-loop progress after resume. A hub outage cannot extend observed native work admission.
- Creation preparation, final creation and rebinding apply expiry in the same admission transaction. An expired idle binding can be reused. Native execution awaiting cancellation or reconciliation still conflicts.
- Deadline caches are evicted only after hub evidence closes admission and proves no remaining execution debt, and after local worker, ownership, inherited-claim and recovery debt clears. Active projects retain budget evidence between requests. Unavailable or legacy settlement evidence retains the budget. Expired idle budgets reconcile through a targeted budget read; retries are limited to four projects per mailbox iteration.
- Persisted binding rows and owner-scoped creation keys support indexed conflict and idempotency reads. Legacy JSON metadata is backfilled without replacing runtime state. Creation-key evidence exposes only the requesting creator's project/intent. CLI retries preserve legacy direct project creation and reuse the existing discussion without reading project history.

Acceptance coverage: `tests/test_collaboration_deadline_admission_followup.py` covers suspension with an unavailable hub, bounded cancellation timing, expired idle/debt bindings, migration and query plans, unrelated settled-history decoding, creator-key identity, encoded keys, legacy CLI retries, active/uncertain cache retention and bounded cleanup. Parent validation owns final full-suite, packaging, review, CI and deployment evidence.

Concurrent expiry observers share one bounded native cancellation attempt per project/session/execution/epoch. Replaced timers cannot duplicate the same stop. This marker retires with verified settled budget state; uncertain execution remains visible. A dedicated overlap regression and every existing continuation cancellation variant pass. The combined parent suite passes 1,218 tests with one optional adapter fixture skipped.
