from mattermost_acp.mm.chunk import chunk_message, estimate_lines, normalize_answer


def test_normalize_collapses_blank_lines() -> None:
    assert normalize_answer("a\n\n\n\nb\n") == "a\n\nb"


def test_chunk_short() -> None:
    assert chunk_message("hello") == ["hello"]


def test_chunk_preserves_fence_across_split() -> None:
    body = "intro\n\n```python\n" + ("x = 1\n" * 400) + "```\n\noutro"
    chunks = chunk_message(body, limit=500)
    assert len(chunks) >= 2
    # Every chunk that opened a fence should be closed
    for c in chunks:
        assert c.count("```") % 2 == 0


def test_mattermost_chunk_marker_splits_and_is_stripped() -> None:
    text = "first piece\n\nMATTERMOST_CHUNK\n\nsecond piece"
    assert chunk_message(text) == ["first piece", "second piece"]


def test_mattermost_chunk_marker_accepts_colon() -> None:
    text = "alpha\nMATTERMOST_CHUNK:\nbeta"
    assert chunk_message(text) == ["alpha", "beta"]


def test_mattermost_chunk_then_safety_split() -> None:
    short_a = "A paragraph that fits."
    long_b = "B paragraph one.\n\n" + ("B more. " * 80)
    text = f"{short_a}\nMATTERMOST_CHUNK\n{long_b}"
    chunks = chunk_message(text, limit=300)
    assert chunks[0] == short_a
    assert len(chunks) >= 3
    assert all("MATTERMOST_CHUNK" not in c for c in chunks)
    assert all(len(c) <= 300 + 10 for c in chunks)  # fence close may add a few


def test_split_before_heading() -> None:
    first = "Intro paragraph. " * 20
    rest = "More body under the heading. " * 30
    text = first + "\n\n## Next section\n\n" + rest
    chunks = chunk_message(text, limit=400)
    assert len(chunks) >= 2
    assert chunks[0].startswith("Intro")
    assert any(c.lstrip().startswith("## Next section") for c in chunks[1:])


def test_split_before_opening_fence_when_intro_fits() -> None:
    # Intro must be >= max_chars/3 so the opener starts the next message.
    intro = ("Here is the script we are about to run. " * 3) + "\n\n"
    body = "x = 1\n" * 80
    text = intro + "```python\n" + body + "```"
    chunks = chunk_message(text, limit=200)
    assert "```" not in chunks[0]
    assert chunks[1].startswith("```python")
    for c in chunks:
        assert c.count("```") % 2 == 0


def test_does_not_split_inside_url() -> None:
    prefix = "See docs at "
    url = "https://issues.example.com/wiki/spaces/DEVX/pages/" + ("x" * 80)
    suffix = " for the rest of the writeup " + ("more words " * 40)
    text = prefix + url + suffix
    chunks = chunk_message(text, limit=200)
    joined_urls = " ".join(chunks)
    assert url in joined_urls.replace(" ", "") or any(url in c for c in chunks)
    for c in chunks:
        # A chunk must not start with the tail of the URL (no scheme).
        stripped = c.lstrip()
        if stripped.startswith("x") and "https://" not in stripped[:20]:
            raise AssertionError(f"URL was split mid-token: {c[:60]!r}")


def test_does_not_split_inside_markdown_link() -> None:
    label = "R" * 40
    dest = "https://issues.example.com/browse/EXAMPLE-42"
    after = " and then a lot more explanation " + ("words " * 50)
    text = f"See [{label}]({dest})" + after
    chunks = chunk_message(text, limit=180)
    full = "".join(chunks)
    assert f"[{label}]({dest})" in full
    for c in chunks:
        assert "EXAMPLE-42)" not in c or "[" in c


def test_list_items_split_between_items() -> None:
    items = [f"- item {i} with some extra padding text" for i in range(20)]
    text = "\n".join(items)
    chunks = chunk_message(text, limit=200)
    assert len(chunks) >= 2
    for c in chunks:
        # Each chunk should start on an item boundary, not mid-item.
        first = c.lstrip().splitlines()[0]
        assert first.startswith("- item")


def test_fitting_fence_is_atomic_when_intro_overflows() -> None:
    """A fence that fits in one message must not be close/reopened."""
    intro = "Here is the snippet.\n\n"
    fence = "```python\nprint('hello')\nprint('world')\n```"
    assert len(intro) + len(fence) > 50
    assert len(fence) <= 50
    chunks = chunk_message(intro + fence, limit=50)
    assert len(chunks) == 2
    assert "```" not in chunks[0]
    assert chunks[1] == fence
    assert chunks[1].count("```") == 2


def test_fitting_fence_stays_with_intro_when_both_fit() -> None:
    text = "Intro.\n\n```js\nconst x = 1;\n```"
    assert chunk_message(text, limit=200) == [text]


def test_mattermost_chunk_inside_fence_is_ignored() -> None:
    text = "```python\nprint(1)\nMATTERMOST_CHUNK\nprint(2)\n```"
    chunks = chunk_message(text)
    assert chunks == [text]


def test_huge_fence_pieces_are_complete_never_empty() -> None:
    body = "x = 1\n" * 80
    text = "```python\n" + body + "```"
    chunks = chunk_message(text, limit=200)
    assert len(chunks) >= 2
    for c in chunks:
        assert c.startswith("```python")
        assert c.rstrip().endswith("```")
        assert c.count("```") == 2
        inner = c[len("```python") :].strip().removesuffix("```").strip()
        assert inner, f"empty fence piece: {c!r}"


def test_huge_fence_does_not_leave_stray_closer() -> None:
    # Closer lands just past a 200-char window — old closer/reopen emitted
    # ```python\n``` then a stray ``` that swallowed the outro.
    line = "x = 1\n"
    n = 25  # 6*25=150 body + opener/closer ≈ 165, plus outro
    text = "```python\n" + (line * n) + "```\n\noutro after"
    chunks = chunk_message(text, limit=180)
    joined = "\n".join(chunks)
    assert joined.count("```") % 2 == 0
    assert any("outro after" in c and c.count("```") == 0 for c in chunks) or any(
        "outro after" in c for c in chunks
    )
    for c in chunks:
        if c.strip() in {"```", "```python", "```python\n```"}:
            raise AssertionError(f"empty/stray fence: {c!r}")


def test_quad_fence_does_not_close_on_inner_triple() -> None:
    text = "````md\nexample:\n```\ncode\n```\nmore\n````"
    chunks = chunk_message(text, limit=2000)
    assert chunks == [text]


def test_long_prose_splits_below_the_show_more_height() -> None:
    para = "Some prose about the gateway that runs a bit long.\n\n"
    chunks = chunk_message(para * 40)
    assert len(chunks) > 1
    for c in chunks:
        assert estimate_lines(c) <= 20


def test_short_answer_stays_one_post() -> None:
    body = "- one\n- two\n- three\n\nDone."
    assert chunk_message(body) == [body]


def test_fenced_code_counts_one_row_per_line() -> None:
    body = "```\n" + ("x" * 300 + "\n") * 3 + "```"
    assert estimate_lines(body) == 5


def test_line_budget_splits_a_tall_but_short_answer() -> None:
    body = "\n\n".join(f"line {i}" for i in range(60))
    chunks = chunk_message(body)
    assert len(chunks) > 1
    assert sum(len(c) for c in chunks) < 2000
    for c in chunks:
        assert estimate_lines(c) <= 20
