"""Exercise the patched adapter's real methods without starting a model."""

from __future__ import annotations

import importlib.util
import os
import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/patch-codex-steering.py"
spec = importlib.util.spec_from_file_location("steering_patch", SCRIPT)
patch = importlib.util.module_from_spec(spec)
spec.loader.exec_module(patch)
BUNDLE = Path(os.environ.get("CODEX_ACP_TEST_BUNDLE", "/nonexistent/adapter.js"))


def test_patch_rejects_unknown_source():
    with pytest.raises(ValueError, match="no files changed"):
        patch.patched_source("unknown adapter version")


@pytest.mark.skipif(
    not BUNDLE.exists() or not shutil.which("node"), reason="local adapter required"
)
def test_native_codex_active_idle_and_completion_race(tmp_path):
    source = patch.patched_source(BUNDLE.read_text())
    assert patch.patched_source(source) == source
    methods = []
    for start, end in [
        ("  async performSteeringRequest(params) {", "\n  /**"),
        ("  parseSessionSteerParams(params) {", "\n  createSessionConfigOptions"),
    ]:
        begin = source.index(start)
        methods.append(source[begin : source.index(end, begin)])
    js = """
import assert from 'node:assert/strict';
const logger = {log() {}};
const assertPromptContentSupported = () => {};
const randomUUID5 = () => 'synthetic-message-id';
const isRecord2 = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const RequestError = {invalidParams() {return new Error('invalid params')}};
class Adapter {
METHODS
}
const adapter = new Adapter();
adapter.getSessionState = () => ({});
adapter.assertSteerInputSupported = () => {};
let started = 0;
adapter.startNewTurnFromSteering = async () => {started++; return {outcome: 'startedNewTurn'}};
const params = adapter.parseSessionSteerParams({
  sessionId: 'same-session', prompt: [{type: 'text', text: 'hint'}],
  _meta: {steering: {idleBehavior: 'promptRequired'}}
});
assert.equal(params._meta.steering.idleBehavior, 'promptRequired');
adapter.getSteerableTurnId = async () => null;
assert.equal((await adapter.performSteeringRequest(params)).outcome, 'promptRequired');
assert.equal(started, 0);
adapter.getSteerableTurnId = async () => 'active-turn';
adapter.injectSteerIntoActiveTurn = async () => true;
assert.equal((await adapter.performSteeringRequest(params)).outcome, 'injected');
assert.equal(started, 0);
// The turn completed between looking up its id and submitting the steer.
adapter.injectSteerIntoActiveTurn = async () => false;
assert.equal((await adapter.performSteeringRequest(params)).outcome, 'promptRequired');
assert.equal(started, 0);
// Existing clients retain their established behavior when they do not opt in.
assert.equal((await adapter.performSteeringRequest({sessionId: 's', prompt: []})).outcome,
             'startedNewTurn');
assert.equal(started, 1);
console.log('active, idle, completion race and legacy behavior passed');
""".replace("METHODS", "\n".join(methods))
    test = tmp_path / "steering.mjs"
    test.write_text(js)
    subprocess.run([shutil.which("node"), str(test)], check=True, capture_output=True, text=True)


def test_legacy_patch_is_atomic_and_idempotent():
    source = "\n".join(before for before, after in patch.REPLACEMENTS)
    updated = patch.patched_source(source)
    assert patch.patched_source(updated) == updated
    for _before, after in patch.REPLACEMENTS:
        assert after in updated
    broken = source.replace(patch.REPLACEMENTS[-1][0], "unsupported")
    with pytest.raises(ValueError):
        patch.patched_source(broken)
