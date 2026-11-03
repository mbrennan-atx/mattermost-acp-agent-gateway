from pathlib import Path

import pytest

from mattermost_acp.attachments import (
    RemoteFile,
    format_attachment_note,
    sanitize_filename,
    save_files,
)


def test_sanitize_filename_strips_paths() -> None:
    assert sanitize_filename("../../etc/passwd") == "passwd"
    assert sanitize_filename("weird name.sh") == "weird_name.sh"


@pytest.mark.asyncio
async def test_save_files_and_skip(tmp_path: Path) -> None:
    async def fetch(file_id: str) -> bytes:
        return b"hello"

    saved, skipped = await save_files(
        [
            RemoteFile("f1", "notes.txt", 5, "text/plain"),
            RemoteFile("f2", "huge.bin", 99999, "application/octet-stream"),
        ],
        base_dir=tmp_path,
        thread_id="t1",
        message_id="m1",
        max_bytes=100,
        fetch=fetch,
    )
    assert len(saved) == 1
    assert saved[0].path.read_bytes() == b"hello"
    assert len(skipped) == 1
    note = format_attachment_note(saved, skipped)
    assert "Mattermost attachments saved" in note
    assert "SKIPPED" in note
