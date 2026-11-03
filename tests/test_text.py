import pytest

from mattermost_acp.text import join_text_chunks


def test_join_preserves_spaces() -> None:
    assert join_text_chunks(["Hel", "lo"]) == "Hello"
    assert join_text_chunks(["Hello ", "world"]) == "Hello world"


@pytest.mark.parametrize(
    "text",
    [
        "**I haven’t verified that.** I traced the stock local setup.",
        "**Result!** Next paragraph.\n\nMore text.",
        "*Really?* Yes.",
        "[Docs](https://Example.Com/Path#Section)",
        "`value:Type;Next` and ```\nobject.Method()\n```",
        "Done.Next\n\n## Heading\n- item\n> quote",
    ],
)
def test_join_preserves_text_at_every_stream_boundary(text: str) -> None:
    for split in range(len(text) + 1):
        assert join_text_chunks([text[:split], text[split:]]) == text
    assert join_text_chunks(list(text)) == text
