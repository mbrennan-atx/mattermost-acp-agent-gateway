"""Pick a model for a brand-new thread from its first message.

Order: an explicit "use <alias>" in the message, then the first matching rule,
then an optional classifier call, then the agent's configured default. The
choice applies once, when the thread's session is created. Explicit
``/model`` settings (thread, channel, harness) always win and skip routing.
"""

from __future__ import annotations

import json
import logging
import os
import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

import aiohttp

logger = logging.getLogger(__name__)

# Verbs that make "<verb> <alias>" a request for that model. "compare grok
# and claude" does not match; "use grok to ...", "with opus", "have luna" do.
_EXPLICIT_VERBS = r"(?:use|using|with|on|via|have|ask|switch\s+to|run\s+(?:this\s+)?(?:on|with))"

_CLASSIFIER_SYSTEM = (
    "You route chat requests to a model. Read the request and pick exactly one "
    "model id from the list. Prefer the cheapest model that can do the job "
    "well. Reply with only the model id."
)


@dataclass(frozen=True)
class RouteRequest:
    text: str
    channel_id: str | None = None
    has_attachments: bool = False


@dataclass(frozen=True)
class RouteDecision:
    model: str
    effort: str | None
    reason: str


@dataclass(frozen=True)
class Target:
    model: str
    effort: str | None = None


@dataclass(frozen=True)
class Rule:
    target: Target
    name: str
    keywords: tuple[str, ...] = ()
    channels: frozenset[str] = frozenset()
    attachments: bool | None = None

    def matches(self, req: RouteRequest) -> bool:
        if not (self.keywords or self.channels or self.attachments is not None):
            return False
        if self.channels and req.channel_id not in self.channels:
            return False
        if self.attachments is not None and req.has_attachments != self.attachments:
            return False
        if self.keywords:
            lowered = req.text.lower()
            return any(_word_in(k, lowered) for k in self.keywords)
        return True


@dataclass(frozen=True)
class Classifier:
    model: str
    choices: dict[str, str]
    grok_config: Path
    choice_effort: dict[str, str] = field(default_factory=dict)
    effort: str | None = "low"
    timeout_s: float = 10.0
    max_chars: int = 4000


@dataclass(frozen=True)
class ModelRouter:
    harnesses: frozenset[str]
    aliases: dict[str, Target] = field(default_factory=dict)
    rules: tuple[Rule, ...] = ()
    classifier: Classifier | None = None

    def applies_to(self, harness: str) -> bool:
        return harness in self.harnesses

    def explicit(self, text: str) -> RouteDecision | None:
        for alias in sorted(self.aliases, key=len, reverse=True):
            pattern = rf"\b{_EXPLICIT_VERBS}\s+(?:the\s+)?{re.escape(alias)}(?![\w.-])"
            if re.search(pattern, text, re.IGNORECASE):
                target = self.aliases[alias]
                return RouteDecision(target.model, target.effort, f"explicit:{alias}")
        return None

    def by_rule(self, req: RouteRequest) -> RouteDecision | None:
        for rule in self.rules:
            if rule.matches(req):
                return RouteDecision(rule.target.model, rule.target.effort, f"rule:{rule.name}")
        return None

    async def route(self, req: RouteRequest) -> RouteDecision | None:
        text = (req.text or "").strip()
        if not text and not req.has_attachments:
            return None
        decision = self.explicit(text) or self.by_rule(req)
        if decision is not None:
            return decision
        if self.classifier is None or not text:
            return None
        return await classify(self.classifier, text)


def _word_in(needle: str, haystack: str) -> bool:
    return re.search(rf"(?<![\w-]){re.escape(needle.lower())}(?![\w-])", haystack) is not None


def _target(raw: object, where: str) -> Target:
    if isinstance(raw, str):
        parts = raw.split()
        if not parts or len(parts) > 2:
            raise ValueError(f"{where}: expected '<model> [effort]', got {raw!r}")
        return Target(parts[0], parts[1] if len(parts) == 2 else None)
    if isinstance(raw, dict) and isinstance(raw.get("model"), str):
        effort = raw.get("effort")
        return Target(raw["model"], effort if isinstance(effort, str) else None)
    raise ValueError(f"{where}: expected a model string or {{model, effort}} table")


def load_router(path: Path, *, grok_home: Path | None = None) -> ModelRouter:
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    router = data.get("router", {})
    harnesses = frozenset(router.get("harnesses", ["grok"]))

    aliases = {
        str(name).lower(): _target(value, f"aliases.{name}")
        for name, value in data.get("aliases", {}).items()
    }

    rules: list[Rule] = []
    for idx, raw in enumerate(data.get("rules", [])):
        name = str(raw.get("name") or f"#{idx + 1}")
        target = _target(raw.get("model") if "effort" not in raw else raw, f"rules.{name}")
        attachments = raw.get("attachments")
        rules.append(
            Rule(
                target=target,
                name=name,
                keywords=tuple(str(k) for k in raw.get("keywords", [])),
                channels=frozenset(str(c) for c in raw.get("channels", [])),
                attachments=attachments if isinstance(attachments, bool) else None,
            )
        )

    classifier = None
    raw_cls = data.get("classifier")
    if raw_cls:
        choices: dict[str, str] = {}
        choice_effort: dict[str, str] = {}
        for mid, value in raw_cls.get("choices", {}).items():
            if isinstance(value, dict):
                choices[str(mid)] = str(value.get("description", ""))
                if isinstance(value.get("effort"), str):
                    choice_effort[str(mid)] = value["effort"]
            else:
                choices[str(mid)] = str(value)
        if not choices or not raw_cls.get("model"):
            raise ValueError("classifier needs model and at least one choice")
        home = grok_home or Path(os.environ.get("GROK_HOME", "~/.grok")).expanduser()
        classifier = Classifier(
            model=str(raw_cls["model"]),
            choices=choices,
            choice_effort=choice_effort,
            grok_config=Path(raw_cls.get("grok_config", home / "config.toml")).expanduser(),
            effort=raw_cls.get("effort", "low"),
            timeout_s=float(raw_cls.get("timeout_s", 10.0)),
        )

    return ModelRouter(
        harnesses=harnesses, aliases=aliases, rules=tuple(rules), classifier=classifier
    )


def load_router_from_env(grok_home: Path | None = None) -> ModelRouter | None:
    raw = os.environ.get("MODEL_ROUTER_FILE", "").strip()
    if not raw:
        return None
    path = Path(raw).expanduser()
    try:
        router = load_router(path, grok_home=grok_home)
    except (OSError, ValueError, tomllib.TOMLDecodeError):
        logger.exception("model router disabled: could not load %s", path)
        return None
    logger.info(
        "model router loaded file=%s aliases=%s rules=%s classifier=%s",
        path,
        len(router.aliases),
        len(router.rules),
        router.classifier.model if router.classifier else None,
    )
    return router


def _endpoint(cls: Classifier) -> tuple[str, str, str]:
    """``(url, api_key, upstream_model)`` for the classifier's Grok model entry."""
    config = tomllib.loads(cls.grok_config.read_text(encoding="utf-8"))
    entry = config.get("model", {}).get(cls.model)
    if not isinstance(entry, dict) or not entry.get("base_url") or not entry.get("api_key"):
        raise ValueError(f"no base_url/api_key for [model.{cls.model}] in {cls.grok_config}")
    url = str(entry["base_url"]).rstrip("/") + "/chat/completions"
    return url, str(entry["api_key"]), str(entry.get("model") or cls.model)


def _pick(answer: str, choices: dict[str, str]) -> str | None:
    answer = answer.strip().strip("`\"' .").lower()
    for choice in choices:
        if answer == choice.lower():
            return choice
    for choice in sorted(choices, key=len, reverse=True):
        if choice.lower() in answer:
            return choice
    return None


async def classify(cls: Classifier, text: str) -> RouteDecision | None:
    try:
        url, key, upstream = _endpoint(cls)
    except (OSError, ValueError, tomllib.TOMLDecodeError) as exc:
        logger.warning("model router classifier unavailable: %s", exc)
        return None

    options = "\n".join(f"- {mid}: {desc}" for mid, desc in cls.choices.items())
    body: dict[str, object] = {
        "model": upstream,
        "messages": [
            {"role": "system", "content": f"{_CLASSIFIER_SYSTEM}\n\nModels:\n{options}"},
            {"role": "user", "content": text[: cls.max_chars]},
        ],
        "max_completion_tokens": 2000,
    }
    if cls.effort:
        body["reasoning_effort"] = cls.effort

    try:
        timeout = aiohttp.ClientTimeout(total=cls.timeout_s)
        async with aiohttp.ClientSession(timeout=timeout) as http:
            async with http.post(
                url, json=body, headers={"Authorization": f"Bearer {key}"}
            ) as resp:
                payload = await resp.json(content_type=None)
                if resp.status >= 400:
                    logger.warning(
                        "model router classifier HTTP %s: %s",
                        resp.status,
                        json.dumps(payload)[:300],
                    )
                    return None
    except (TimeoutError, aiohttp.ClientError, ValueError) as exc:
        logger.warning("model router classifier failed: %s", exc)
        return None

    try:
        answer = payload["choices"][0]["message"]["content"] or ""
    except (KeyError, IndexError, TypeError):
        logger.warning("model router classifier: unexpected reply %s", str(payload)[:300])
        return None
    choice = _pick(str(answer), cls.choices)
    if choice is None:
        logger.warning("model router classifier picked unknown model %r", answer[:80])
        return None
    return RouteDecision(choice, cls.choice_effort.get(choice), "classifier")
