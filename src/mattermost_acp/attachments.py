"""Download Mattermost file attachments for agent access."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

DANGEROUS_EXTENSIONS = frozenset(
    {
        ".exe", ".bat", ".cmd", ".com", ".msi", ".scr", ".ps1", ".psm1",
        ".sh", ".bash", ".zsh", ".jar", ".app", ".dll", ".so", ".dylib",
        ".vbs", ".js", ".apk",
    }
)

_UNSAFE_CHARS = re.compile(r"[^A-Za-z0-9._-]+")
_MAX_NAME_LEN = 200

FetchFile = Callable[[str], Awaitable[bytes]]


@dataclass(frozen=True)
class RemoteFile:
    file_id: str
    filename: str
    size: int
    content_type: str | None = None


@dataclass(frozen=True)
class SavedAttachment:
    original_filename: str
    path: Path
    size: int
    content_type: str | None
    warning: str | None = None


@dataclass(frozen=True)
class SkippedAttachment:
    original_filename: str
    reason: str


def sanitize_filename(name: str, *, fallback: str = "attachment") -> str:
    name = unicodedata.normalize("NFKC", name or "")
    name = name.replace("\\", "/")
    name = Path(name).name
    name = name.strip().lstrip(".")
    name = _UNSAFE_CHARS.sub("_", name)
    name = name[:_MAX_NAME_LEN]
    return name or fallback


def format_size(num_bytes: int) -> str:
    size = float(num_bytes)
    if size < 1024:
        return f"{int(size)} B"
    for unit in ("KB", "MB", "GB"):
        size /= 1024
        if size < 1024 or unit == "GB":
            return f"{size:.1f} {unit}"
    return f"{size:.1f} GB"


def _dedupe_name(name: str, used: set[str]) -> str:
    if name not in used:
        return name
    stem, suffix = Path(name).stem, Path(name).suffix
    n = 1
    candidate = f"{stem}-{n}{suffix}"
    while candidate in used:
        n += 1
        candidate = f"{stem}-{n}{suffix}"
    return candidate


async def save_files(
    files: Sequence[RemoteFile],
    *,
    base_dir: Path,
    thread_id: str,
    message_id: str,
    max_bytes: int,
    fetch: FetchFile,
) -> tuple[list[SavedAttachment], list[SkippedAttachment]]:
    saved: list[SavedAttachment] = []
    skipped: list[SkippedAttachment] = []
    used_names: set[str] = set()
    target_dir = base_dir / str(thread_id) / str(message_id)

    for idx, item in enumerate(files):
        if item.size > max_bytes:
            skipped.append(
                SkippedAttachment(
                    original_filename=item.filename,
                    reason=(
                        f"exceeds {format_size(max_bytes)} limit "
                        f"({format_size(item.size)})"
                    ),
                )
            )
            continue

        safe_name = sanitize_filename(item.filename, fallback=f"attachment-{idx}")
        final_name = _dedupe_name(safe_name, used_names)
        used_names.add(final_name)

        target_dir.mkdir(parents=True, exist_ok=True)
        dest = target_dir / final_name
        data = await fetch(item.file_id)
        dest.write_bytes(data)

        ext = Path(item.filename).suffix.lower()
        warning = (
            "executable/script file — treat as untrusted"
            if ext in DANGEROUS_EXTENSIONS
            else None
        )
        saved.append(
            SavedAttachment(
                original_filename=item.filename,
                path=dest,
                size=len(data) if data else item.size,
                content_type=item.content_type,
                warning=warning,
            )
        )

    return saved, skipped


def format_attachment_note(
    saved: list[SavedAttachment], skipped: list[SkippedAttachment]
) -> str:
    if not saved and not skipped:
        return ""
    lines = ["[Mattermost attachments saved]"]
    for a in saved:
        ctype = a.content_type or "unknown type"
        line = f"- {a.original_filename} → {a.path} ({format_size(a.size)}, {ctype})"
        if a.warning:
            line += f" [warning: {a.warning}]"
        lines.append(line)
    for s in skipped:
        lines.append(f"- {s.original_filename} → SKIPPED ({s.reason})")
    return "\n".join(lines)
