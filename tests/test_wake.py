from mattermost_acp.mm.wake import classify_wake, sole_mention


def _wake(**overrides: object) -> tuple[str | None, str]:
    base = dict(
        channel_id="home",
        author_id="owner",
        self_id="bot",
        message="hello",
        root_id="",
        username="sysadmin",
        home_channel_ids=frozenset({"home"}),
        all_channels=False,
        exclude_channel_ids=frozenset(),
        allow_user_ids=frozenset({"owner"}),
        peer_channel_id="floor",
    )
    base.update(overrides)
    return classify_wake(**base)  # type: ignore[arg-type]


def test_sole_mention_is_exactly_one() -> None:
    assert sole_mention("please @support-helper look") == "support-helper"
    assert sole_mention("no mention") is None
    assert sole_mention("@a and @b") is None
    assert sole_mention("@all hands") is None
    assert sole_mention("@here") is None
    assert sole_mention("@channel") is None


def test_home_wakes_only_for_allowlisted_human() -> None:
    assert _wake() == ("home", "home")
    assert _wake(author_id="other")[0] is None
    assert _wake(author_id="bot") == (None, "self")
    assert _wake(channel_id="elsewhere") == (None, "not-home")


def test_peer_wakes_only_on_a_root_mention_of_self() -> None:
    assert _wake(channel_id="floor", author_id="assistant-b", message=" @sysadmin disk full") == (
        "peer",
        "peer",
    )
    assert _wake(channel_id="floor", message="hello")[1] == "peer-mention"
    assert _wake(channel_id="floor", message="@sysadmin @assistant-c")[1] == "peer-mention"
    reply = _wake(
        channel_id="floor",
        author_id="sysadmin-other",
        message="@sysadmin done",
        root_id="rootpost",
    )
    assert reply == (None, "peer-reply")


def test_peer_does_not_use_the_home_allowlist() -> None:
    kind, _reason = _wake(
        channel_id="floor",
        author_id="unlisted-user",
        message="@sysadmin help",
    )
    assert kind == "peer"


def test_owner_dm_wakes_any_agent() -> None:
    assert _wake(channel_id="dm1", channel_type="D") == ("home", "owner-dm")
    assert _wake(channel_id="dm1", channel_type="D", author_id="other")[0] is None
    assert _wake(channel_id="dm1", channel_type="O") == (None, "not-home")


def test_owner_floor_reply_wakes_mentioned_agent_or_starter() -> None:
    reply = dict(channel_id="floor", root_id="rootpost")
    assert _wake(**reply, message="@sysadmin go") == ("peer", "owner-reply-mention")
    assert _wake(**reply, message="@assistant-c go") == (None, "owner-reply-other")
    assert _wake(**reply, message="go", root_author_id="bot") == (
        "peer",
        "owner-reply-starter",
    )
    assert _wake(**reply, message="go", root_author_id="assistant-c-id") == (
        None,
        "owner-reply-other",
    )
    # Two mentions are ambiguous, so the thread starter owns it.
    assert _wake(**reply, message="@sysadmin @assistant-c go", root_author_id="bot") == (
        "peer",
        "owner-reply-starter",
    )
    assert _wake(**reply, author_id="assistant-b", message="@sysadmin done") == (
        None,
        "peer-reply",
    )
