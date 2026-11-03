from mattermost_acp.mm.gateway import MattermostGateway
from mattermost_acp.store import ThreadStore


class _Cfg:
    auto_rename_threads = True
    inject_channel_context = True
    allow_user_ids = frozenset()
    channel_ids = frozenset()


class _Client:
    def __init__(self) -> None:
        self.posts: dict[str, str] = {}
        self.patched: list[tuple[str, str]] = []

    async def get_post(self, post_id: str) -> dict[str, str]:
        return {"id": post_id, "message": self.posts[post_id]}

    async def patch_post(self, post_id: str, message: str) -> dict[str, str]:
        self.posts[post_id] = message
        self.patched.append((post_id, message))
        return {"id": post_id, "message": message}


async def test_apply_root_rename_prepends_heading(tmp_path) -> None:
    store = ThreadStore(tmp_path / "t.db")
    await store.open()
    await store.save("root1", "sess", "/tmp")
    client = _Client()
    client.posts["root1"] = "can you set up the gateway to update the top level text"
    gw = MattermostGateway.__new__(MattermostGateway)
    gw.config = _Cfg()
    gw.client = client
    gw.sessions = type("S", (), {"store": store})()
    gw._rename_attempts = {}

    applied = await gw._apply_root_rename(
        "root1", client.posts["root1"], "Gateway thread titles"
    )
    assert applied == "Gateway thread titles"
    assert client.posts["root1"].startswith("**Gateway thread titles**\n\n")
    rec = await store.get("root1")
    assert rec is not None
    assert rec.title == "Gateway thread titles"
    await store.close()


async def test_finish_rename_fallback_after_three_misses(tmp_path) -> None:
    store = ThreadStore(tmp_path / "t.db")
    await store.open()
    await store.save("root1", "sess", "/tmp")
    client = _Client()
    original = "can you set up the gateway to update the top level text of a thread"
    client.posts["root1"] = original
    gw = MattermostGateway.__new__(MattermostGateway)
    gw.config = _Cfg()
    gw.client = client
    gw.sessions = type("S", (), {"store": store})()
    gw._rename_attempts = {"root1": 2}

    applied = await gw._finish_rename("root1", original, original, None, True)
    assert applied
    assert client.patched
    await store.close()
