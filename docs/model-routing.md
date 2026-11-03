# Optional model routing

Set `MODEL_ROUTER_FILE` to an external TOML file to select a model when a new
thread receives its first session. Explicit thread/channel/harness model settings
win. Resumed sessions retain their selections. See
[the synthetic example](../examples/model-router.toml).

Routing tries an explicit alias request, then the first matching rule, then an
optional classifier, then `AGENT_MODEL`. Rules can match channel IDs, attachments
and keywords. `[router].harnesses` explicitly restricts where the model IDs apply;
the legacy default is Grok. Model IDs are never translated across providers.

The optional classifier uses a chat-completions endpoint/key from a model entry
in a private Grok configuration. It is not a common authentication mechanism for
all harnesses. Classifier timeout/errors fall back to the configured default.
Keep endpoints, keys and private routing policy outside published source.
