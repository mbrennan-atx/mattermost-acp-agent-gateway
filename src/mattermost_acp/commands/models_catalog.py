"""Grok model catalog for Mattermost ``/model`` — mirrors CLI ``grok models``.

Listing is gateway-local (shell out to ``grok models``). Switching is a
gateway-owned ACP RPC (``session/set_model`` via ``SessionService.switch_model``)
validated against this catalog — not a chat passthrough.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass

logger = logging.getLogger(__name__)

_DEFAULT_LINE = re.compile(r"^Default model:\s*(.+?)\s*$", re.I)
_MODEL_LINE = re.compile(
    r"^\s*(?:\*|[-•])\s+([^\s(]+)"  # * id  or  - id
    r"(?:\s+\(default\))?\s*$",
    re.I,
)
_CACHE_TTL_S = 120.0
_AUTOCOMPLETE_LIMIT = 25  # UI result limit


@dataclass(frozen=True)
class ModelInfo:
    id: str
    is_default: bool = False


@dataclass
class ModelCatalog:
    default_id: str | None
    models: list[ModelInfo]
    raw: str
    fetched_at: float

    @property
    def ids(self) -> list[str]:
        return [m.id for m in self.models]


_cache: ModelCatalog | None = None
_cache_lock = asyncio.Lock()


def parse_grok_models_output(text: str) -> ModelCatalog:
    """Parse plain-text ``grok models`` output into a structured catalog."""
    default_id: str | None = None
    models: list[ModelInfo] = []
    seen: set[str] = set()

    for line in text.splitlines():
        m_def = _DEFAULT_LINE.match(line.strip())
        if m_def:
            default_id = m_def.group(1).strip()
            continue
        m = _MODEL_LINE.match(line)
        if not m:
            continue
        mid = m.group(1).strip()
        if not mid or mid in seen:
            continue
        seen.add(mid)
        is_default = "(default)" in line.lower() or (
            default_id is not None and mid == default_id
        )
        if is_default and default_id is None:
            default_id = mid
        models.append(ModelInfo(id=mid, is_default=is_default))

    # Ensure default is marked even if only the header named it
    if default_id and default_id not in seen:
        models.insert(0, ModelInfo(id=default_id, is_default=True))
    elif default_id:
        models = [
            ModelInfo(id=m.id, is_default=(m.id == default_id)) for m in models
        ]

    return ModelCatalog(
        default_id=default_id,
        models=models,
        raw=text,
        fetched_at=time.monotonic(),
    )


def format_catalog(catalog: ModelCatalog, *, query: str | None = None) -> str:
    """Mattermost markdown list of models (optionally filtered)."""
    models = catalog.models
    q = (query or "").strip().lower()
    if q:
        models = [m for m in models if q in m.id.lower()]

    header = f"**Models** ({len(models)}"
    if q:
        header += f" matching `{query}`"
    header += f" · default `{catalog.default_id or '—'}`)"
    if not models:
        return header + "\n_No matches. Try `/model` with no filter._"

    lines = [header, ""]
    for m in models:
        mark = "\\*" if m.is_default else "·"
        tag = " **(default)**" if m.is_default else ""
        lines.append(f"{mark} `{m.id}`{tag}")
    lines.append("")
    lines.append(
        "Set: `/model <id> [effort] [thread|channel|harness]` "
        "(scope defaults to `thread`). Also `/effort high` on current model. "
        "In chat: ask to switch, or emit `MATTERMOST_MODEL_SET: <id> [effort] [scope]`."
    )
    return "\n".join(lines)


def filter_for_autocomplete(
    catalog: ModelCatalog, current: str, *, limit: int = _AUTOCOMPLETE_LIMIT
) -> list[str]:
    """Return up to 25 model ids for Mattermost autocomplete."""
    cur = (current or "").strip().lower()
    ids = catalog.ids
    if not cur:
        # Prefer default first, then alpha — still cap at 25
        default = catalog.default_id
        rest = sorted(i for i in ids if i != default)
        ordered = ([default] if default and default in ids else []) + rest
        return ordered[:limit]

    # Prefix matches first, then substring
    prefix = [i for i in ids if i.lower().startswith(cur)]
    substr = [i for i in ids if cur in i.lower() and i not in prefix]
    # stable-ish: sort each bucket
    prefix.sort(key=str.lower)
    substr.sort(key=str.lower)
    return (prefix + substr)[:limit]


def resolve_model_id(
    catalog: ModelCatalog, query: str
) -> tuple[str | None, tuple[str, ...]]:
    """Map a free-form name onto a catalog id.

    Returns ``(id, ())`` on a unique hit, ``(None, candidates)`` when more
    than one catalog id matches, or ``(None, ())`` when nothing matches.
    Exact ids win. ``hub-`` is optional. Multiple hits collapse to the
    catalog default only when the query is a prefix of that default id
    (so ``grok`` picks the default Grok model; ``claude`` stays ambiguous
    when both sonnet and opus are installed).
    """
    raw = (query or "").strip()
    if not raw or not catalog.models:
        return None, ()

    ids = catalog.ids
    lower_map = {i.lower(): i for i in ids}
    key = re.sub(r"\s+", "-", raw.lower())

    if key in lower_map:
        return lower_map[key], ()
    hub_key = key if key.startswith("hub-") else f"hub-{key}"
    bare_key = key[4:] if key.startswith("hub-") else key
    if hub_key in lower_map:
        return lower_map[hub_key], ()
    if bare_key in lower_map:
        return lower_map[bare_key], ()

    tokens = [t for t in re.split(r"[\s/_]+", raw.lower()) if t]
    prefixes = [i for i in ids if i.lower().startswith(key)]
    if len(prefixes) == 1:
        return prefixes[0], ()

    def _token_hit(mid: str) -> bool:
        ml = mid.lower()
        if key in ml:
            return True
        return bool(tokens) and all(t in ml for t in tokens)

    hits = [i for i in ids if _token_hit(i)]
    # Keep prefix matches even when they failed the substring/token pass.
    seen = set(hits)
    for item in prefixes:
        if item not in seen:
            hits.append(item)
            seen.add(item)
    if len(hits) == 1:
        return hits[0], ()
    if len(hits) > 1:
        default = catalog.default_id
        if (
            default
            and default in hits
            and default.lower().startswith(key)
        ):
            return default, ()
        return None, tuple(hits)
    return None, ()


async def run_grok_models(
    grok_command: str, *, timeout_s: float = 45.0
) -> tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        grok_command,
        "models",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        out_b, err_b = await asyncio.wait_for(proc.communicate(), timeout=timeout_s)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        return 124, "", f"timeout after {timeout_s:.0f}s"
    return (
        int(proc.returncode or 0),
        (out_b or b"").decode("utf-8", errors="replace"),
        (err_b or b"").decode("utf-8", errors="replace"),
    )


async def fetch_catalog(
    grok_command: str,
    *,
    force: bool = False,
    timeout_s: float = 45.0,
) -> ModelCatalog:
    """Cached ``grok models`` parse (TTL ~2 min)."""
    global _cache
    async with _cache_lock:
        if (
            not force
            and _cache is not None
            and (time.monotonic() - _cache.fetched_at) < _CACHE_TTL_S
            and _cache.models
        ):
            return _cache

        rc, out, err = await run_grok_models(grok_command, timeout_s=timeout_s)
        text = out.strip() or err.strip()
        if rc != 0 and not out.strip():
            logger.warning("grok models failed rc=%s err=%s", rc, err[:300])
            # Return empty catalog with error in raw
            catalog = ModelCatalog(
                default_id=None,
                models=[],
                raw=text or f"rc={rc}",
                fetched_at=time.monotonic(),
            )
            return catalog

        catalog = parse_grok_models_output(text)
        if not catalog.models:
            logger.warning("grok models produced no parseable entries")
        _cache = catalog
        return catalog


def clear_catalog_cache() -> None:
    global _cache
    _cache = None
