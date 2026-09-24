"""Public REST info requests, via the SDK's plain `API.post` in a worker thread.

Used for predictedFundings (not on the websocket at all) and as the fallback source for
asset contexts and books when the websocket value is stale. Weights (limit 1200/min per IP):
l2Book 2, metaAndAssetCtxs 20, predictedFundings 20.
"""

import asyncio
from typing import Any, Protocol

from hyperliquid.api import API  # pyright: ignore[reportMissingTypeStubs]


class InfoClient(Protocol):
    async def info(self, payload: dict[str, Any]) -> Any: ...


class RestInfoClient:
    def __init__(self, base_url: str, timeout_s: float) -> None:
        self._api = API(base_url=base_url, timeout=timeout_s)
        # requests.Session isn't documented as thread-safe: one request at a time.
        self._lock = asyncio.Lock()

    async def info(self, payload: dict[str, Any]) -> Any:
        async with self._lock:
            return await asyncio.to_thread(self._post, payload)

    def _post(self, payload: dict[str, Any]) -> Any:
        return self._api.post("/info", payload)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
