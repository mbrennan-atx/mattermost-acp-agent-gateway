"""Gateway allowlist / self-filter / home-channel without a live MM server."""

from mattermost_acp.mm.gateway import MattermostGateway


class _Cfg:
    allow_user_ids = frozenset({"matt"})
    channel_ids = frozenset({"home"})


def test_allowed_and_home() -> None:
    gw = MattermostGateway.__new__(MattermostGateway)
    gw.config = _Cfg()
    assert gw.allowed("matt") is True
    assert gw.allowed("other") is False
    assert gw.in_home("home") is True
    assert gw.in_home("else") is False


def test_empty_home_accepts_all_channels() -> None:
    gw = MattermostGateway.__new__(MattermostGateway)
    gw.config = type("Cfg", (), {"allow_user_ids": frozenset(), "channel_ids": frozenset()})()
    assert gw.in_home("anything") is True


def test_all_channels_flag_accepts_everything() -> None:
    gw = MattermostGateway.__new__(MattermostGateway)
    gw.config = type(
        "Cfg",
        (),
        {
            "allow_user_ids": frozenset(),
            "channel_ids": frozenset({"home"}),
            "all_channels": True,
        },
    )()
    assert gw.in_home("home") is True
    assert gw.in_home("brand-new-channel") is True


def test_exclude_beats_all_channels() -> None:
    gw = MattermostGateway.__new__(MattermostGateway)
    gw.config = type(
        "Cfg",
        (),
        {
            "allow_user_ids": frozenset(),
            "channel_ids": frozenset(),
            "all_channels": True,
            "exclude_channel_ids": frozenset({"ea"}),
        },
    )()
    assert gw.in_home("ea") is False
    assert gw.in_home("anything-else") is True
