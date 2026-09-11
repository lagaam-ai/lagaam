"""httpx wiring for Pinot: the only module in this adapter with an HTTP verb.

Two services with separate principal lists — the controller for metadata and
the broker for queries — so basic auth is threaded into both. Every failure
that is not a well-formed JSON body leaves as PinotTransportError, which the
engine turns into an EngineError; a controller 404 is not a failure but a
"no such thing", returned as the NotFound sentinel so the caller can decide.
"""

from typing import Any, Final
from urllib.parse import quote

import httpx


class PinotTransportError(Exception):
    """Pinot could not be reached, or answered with something unreadable.

    Adapter-private: never raised past engine.py, which translates it into
    core's EngineError so no transport detail reaches an agent.
    """


class _NotFound:
    """Sentinel: the controller answered 404 for this path."""


# Anything that could reshape a URL path, plus whitespace a name may not carry.
_ILLEGAL_PART_CHARS: Final = frozenset("/.\\?#%")


class PinotClient:
    NotFound: Final = _NotFound

    def __init__(
        self,
        controller_url: str,
        broker_url: str,
        user: str | None = None,
        password: str | None = None,
        timeout_seconds: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._controller_url = controller_url.rstrip("/")
        self._broker_url = broker_url.rstrip("/")
        auth = (user, password) if user is not None and password is not None else None
        self._http = httpx.AsyncClient(auth=auth, timeout=timeout_seconds, transport=transport)

    @staticmethod
    def path_part(part: str) -> str:
        """One agent-supplied name, safe to place in a URL path.

        Table names arrive from agents and land in controller paths, so this
        is a security boundary: a part carrying a separator is refused rather
        than encoded, because a name Pinot cannot hold is a name we never ask for.
        """
        if not part or part.strip() != part:
            raise ValueError(f"not a usable Pinot name: {part!r}")
        # Non-ASCII would raise UnicodeEncodeError inside httpx's header encoding.
        if not part.isascii():
            raise ValueError(f"not a usable Pinot name: {part!r}")
        if any(char in _ILLEGAL_PART_CHARS for char in part):
            raise ValueError(f"not a usable Pinot name: {part!r}")
        if any(char.isspace() for char in part):
            raise ValueError(f"not a usable Pinot name: {part!r}")
        return quote(part, safe="")

    async def controller_get(
        self,
        path: str,
        params: dict[str, str] | None = None,
        database: str | None = None,
    ) -> Any:
        """GET one controller path. Returns parsed JSON, or NotFound on 404."""
        headers = {"database": database} if database else None
        try:
            response = await self._http.get(
                f"{self._controller_url}{path}",
                params=params,
                headers=headers,
            )
        except httpx.HTTPError as exc:
            raise PinotTransportError(f"controller GET {path} failed") from exc
        if response.status_code == 404:
            return self.NotFound
        if response.status_code >= 400:
            raise PinotTransportError(
                f"controller GET {path} returned {response.status_code}"
            )
        return self._json(response, f"controller GET {path}")

    async def broker_query(
        self, sql: str, options: str, timeout_seconds: float | None = None
    ) -> Any:
        """POST one query to the broker. Returns parsed JSON.

        Never checks the status code for a query error: every Pinot query
        error is an HTTP 200 carrying exceptions[].
        """
        try:
            response = await self._http.post(
                f"{self._broker_url}/query/sql",
                json={"sql": sql, "queryOptions": options},
                timeout=timeout_seconds if timeout_seconds is not None else httpx.USE_CLIENT_DEFAULT,
            )
        except httpx.HTTPError as exc:
            raise PinotTransportError("broker query failed") from exc
        if response.status_code >= 400:
            raise PinotTransportError(
                f"broker query returned {response.status_code}"
            )
        return self._json(response, "broker query")

    async def aclose(self) -> None:
        await self._http.aclose()

    @staticmethod
    def _json(response: httpx.Response, what: str) -> Any:
        try:
            return response.json()
        except ValueError as exc:
            raise PinotTransportError(f"{what} returned a non-JSON body") from exc
