# Validation quickstart

Run uv sync --locked --group dev, uv run pytest -q tests/test_collaboration.py, uv run pytest -q, uv run ruff check ., uv build and the documented release checks.

Use docs/collaboration.md to configure a local hub with synthetic actors and token files outside Git. Create a project, bind the owner to an existing thread, ask a peer and read the returned request ID. Verify show records the peer's result and native provenance. Repeat with active steering and unsupported busy delivery. Restart the hub with queued and running work: queued remains available, running is visibly interrupted. Repeat replies and cancellation to prove idempotency and isolation.

Production pilot evidence is retained privately and summarized in validation.md. Do not copy runtime transcripts, credentials or actual fleet IDs into this public repository.
