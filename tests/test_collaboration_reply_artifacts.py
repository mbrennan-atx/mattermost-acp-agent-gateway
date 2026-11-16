"""Repeated correlated replies keep artifact evidence stable through native updates."""

import pytest
from test_collaboration import api_fixture, ask, close_api, project

from mattermost_acp.collaboration.client import HubError


@pytest.mark.parametrize("state", ["done", "failed", "blocked"])
@pytest.mark.parametrize("cancelling", [False, True])
@pytest.mark.parametrize("changed", [[], ["new-evidence"], ["original-evidence", "new-evidence"]])
async def test_repeated_reply_rejects_changed_artifacts_without_losing_native_evidence(
    tmp_path, state, cancelling, changed
):
    store, server, clients = await api_fixture(tmp_path)
    try:
        p = project(store)
        work = ask(store, p["id"])
        claim = store.claim_next("beta", "worker")
        store.update(
            work["id"],
            "beta",
            "worker",
            "dispatch",
            evidence={"session_id": "native-session", "execution_id": "native-execution"},
            claim=claim["claim"],
        )
        if cancelling:
            store.cancel(p["id"], "alpha")
        body = {
            "claim": claim["claim"],
            "result": "Verified result",
            "state": state,
            "evidence": {"artifacts": ["original-evidence"]},
        }
        path = f"/messages/{work['id']}/reply"
        first = await clients["beta"].request("POST", path, body)
        evidence_key = "late_evidence" if cancelling else "evidence"
        assert first[evidence_key]["artifacts"] == body["evidence"]["artifacts"]
        # Native observation may advance after a worker submits its reply.
        latest = store.get_message(work["id"], "beta")
        latest["evidence"]["native_observation"] = "new native progress"
        store.save_message(latest)
        before = store.show(p["id"], "alpha")
        repeated = await clients["beta"].request("POST", path, body)
        assert repeated["evidence"]["native_observation"] == "new native progress"
        assert repeated[evidence_key]["artifacts"] == body["evidence"]["artifacts"]
        assert store.show(p["id"], "alpha") == before
        with pytest.raises(HubError) as conflict:
            await clients["beta"].request(
                "POST", path, {**body, "evidence": {"artifacts": changed}}
            )
        assert conflict.value.status == 409
        assert store.show(p["id"], "alpha") == before
    finally:
        await close_api(store, server, clients)
