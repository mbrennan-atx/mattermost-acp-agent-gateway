# Codex

Select `HARNESS=codex` and set `AGENT_COMMAND` to the installed `codex-acp`
executable. That adapter runs Codex App Server and uses Codex's native config,
authentication, sessions and connectors. Set `CODEX_PATH` if the native CLI is
not on the service PATH. Keep `CODEX_HOME` outside the source checkout.

Model and reasoning effort use ACP config options. The gateway reapplies pins
after load/resume; validation should compare the actual turn model and effort,
not only a model-list response. `/mcps` invokes the native Codex CLI.

Newer adapters advertise native `promptRequired` steering. For compatibility
inspection, pass the explicit installed bundle to:

```bash
python3 scripts/patch-codex-steering.py /absolute/path/to/codex-acp/dist/index.js
```

The checker leaves recognized native behavior untouched. For its supported older
source shape it applies the narrow compatibility patch with an adjacent original
copy; it rejects unknown source without writing. Restart only idle gateways after
an actual adapter change. To test the installed methods without calling a model:

```bash
CODEX_ACP_TEST_BUNDLE=/absolute/path/to/codex-acp/dist/index.js \
  uv run pytest -q tests/test_codex_steering_patch.py
```

Native approval/sandbox mode and connector configuration remain operator choices.
The gateway does not import Grok or Claude credentials.
