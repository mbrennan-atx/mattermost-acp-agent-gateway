from mattermost_acp.commands.models_catalog import (
    ModelCatalog,
    ModelInfo,
    filter_for_autocomplete,
    format_catalog,
    parse_grok_models_output,
    resolve_model_id,
)

SAMPLE = """
You are logged in with grok.com.

Default model: grok-4.5

Available models:
  * grok-4.5 (default)
  - hub-claude-4.6-sonnet
  - hub-kimi-k2.6
  - hub-gpt-5.4
  - hub-gemini-2.5-pro
"""


def test_parse_grok_models_output() -> None:
    cat = parse_grok_models_output(SAMPLE)
    assert cat.default_id == "grok-4.5"
    ids = cat.ids
    assert "grok-4.5" in ids
    assert "hub-kimi-k2.6" in ids
    assert len(ids) == 5
    defaults = [m for m in cat.models if m.is_default]
    assert len(defaults) == 1
    assert defaults[0].id == "grok-4.5"


def test_format_catalog_includes_default_marker() -> None:
    cat = parse_grok_models_output(SAMPLE)
    text = format_catalog(cat)
    assert "default `grok-4.5`" in text
    assert "`hub-kimi-k2.6`" in text
    assert "(default)" in text


def test_format_catalog_filter() -> None:
    cat = parse_grok_models_output(SAMPLE)
    text = format_catalog(cat, query="kimi")
    assert "`hub-kimi-k2.6`" in text
    assert "`hub-gpt-5.4`" not in text


def test_autocomplete_prefix_and_limit() -> None:
    cat = parse_grok_models_output(SAMPLE)
    hits = filter_for_autocomplete(cat, "hub-k")
    assert hits[0].startswith("hub-k") or all("hub-k" in h for h in hits)
    assert "hub-kimi-k2.6" in hits

    empty = filter_for_autocomplete(cat, "")
    assert empty[0] == "grok-4.5"  # default first
    assert len(empty) <= 25


def _resolve_catalog() -> ModelCatalog:
    return ModelCatalog(
        default_id="grok-4.5",
        models=[
            ModelInfo(id="grok-4.5", is_default=True),
            ModelInfo(id="grok-4.6"),
            ModelInfo(id="hub-claude-sonnet-5"),
            ModelInfo(id="hub-claude-opus-5"),
            ModelInfo(id="hub-gpt-5.6-sol"),
            ModelInfo(id="hub-kimi-k2.6"),
        ],
        raw="",
        fetched_at=0.0,
    )


def test_resolve_exact_and_hub_prefix() -> None:
    cat = _resolve_catalog()
    assert resolve_model_id(cat, "grok-4.5") == ("grok-4.5", ())
    assert resolve_model_id(cat, "hub-claude-sonnet-5") == (
        "hub-claude-sonnet-5",
        (),
    )
    assert resolve_model_id(cat, "claude-sonnet-5") == (
        "hub-claude-sonnet-5",
        (),
    )


def test_resolve_unique_alias() -> None:
    cat = _resolve_catalog()
    assert resolve_model_id(cat, "sonnet") == ("hub-claude-sonnet-5", ())
    assert resolve_model_id(cat, "opus") == ("hub-claude-opus-5", ())
    assert resolve_model_id(cat, "kimi") == ("hub-kimi-k2.6", ())
    assert resolve_model_id(cat, "claude sonnet") == ("hub-claude-sonnet-5", ())


def test_resolve_grok_picks_default_among_family() -> None:
    cat = _resolve_catalog()
    assert resolve_model_id(cat, "grok") == ("grok-4.5", ())


def test_resolve_ambiguous_family_stays_unresolved() -> None:
    cat = _resolve_catalog()
    model, candidates = resolve_model_id(cat, "claude")
    assert model is None
    assert "hub-claude-sonnet-5" in candidates
    assert "hub-claude-opus-5" in candidates


def test_resolve_unknown() -> None:
    cat = _resolve_catalog()
    assert resolve_model_id(cat, "not-a-model") == (None, ())


def test_autocomplete_caps_at_25() -> None:
    lines = ["Default model: m0", "Available models:", "  * m0 (default)"]
    for i in range(1, 40):
        lines.append(f"  - m{i}")
    cat = parse_grok_models_output("\n".join(lines))
    assert len(cat.ids) == 40
    assert len(filter_for_autocomplete(cat, "")) == 25
