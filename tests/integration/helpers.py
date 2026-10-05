import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any

import httpx


class WebhookReceiver:
    def __init__(self, base_url: str, client: httpx.AsyncClient) -> None:
        self.base_url = base_url
        self._client = client

    def url(self, name: str, *, fail: int = 0) -> str:
        return f"{self.base_url}/hooks/{name}" + (f"?fail={fail}" if fail else "")

    async def deliveries(self, name: str) -> list[dict[str, Any]]:
        response = await self._client.get(f"/hooks/{name}")
        response.raise_for_status()
        result: list[dict[str, Any]] = response.json()
        return result


async def eventually[T](
    probe: Callable[[], Awaitable[T]],
    condition: Callable[[T], bool],
    *,
    within: float = 15.0,
    interval: float = 0.1,
) -> T:
    """Опрашивает probe, пока condition не станет истинным."""
    deadline = time.monotonic() + within
    while True:
        value = await probe()
        if condition(value):
            return value
        if time.monotonic() > deadline:
            raise AssertionError(f"Condition not met within {within}s, last value: {value!r}")
        await asyncio.sleep(interval)
