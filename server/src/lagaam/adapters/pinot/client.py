"""httpx wiring for Pinot: the only module in this adapter with an HTTP verb.

Two services with separate principal lists — the controller for metadata and
the broker for queries — so basic auth is threaded into both. Every failure
that is not a well-formed JSON body leaves as PinotTransportError, which the
engine turns into an EngineError; a controller 404 is not a failure but a
"no such thing", returned as the NotFound sentinel so the caller can decide.
"""

import json
from typing import Any, Final
from urllib.parse import quote

import httpx


class PinotTransportError(Exception):
    """Pinot could not be reached, or answered with something unreadable.

    Adapter-private: never raised past engine.py, which translates it into
    core's EngineError so no transport detail reaches an agent.
    """


class PinotForbidden(Exception):
    """Pinot refused the credentials or the table: a 401 or a 403.

    Adapter-private like PinotTransportError, and deliberately body-free: the
    refusal text names tables the caller may not be allowed to learn about.
    A refusal is not an outage — retrying it unchanged never succeeds.
    """


class PinotResponseTooLarge(Exception):
    """The broker's answer outgrew the byte ceiling this client will read.

    Adapter-private like PinotTransportError: engine.py turns it into the
    agent-fixable RESPONSE_TOO_LARGE hint. Measured on 1.5.1, the multi-stage
    engine accepts maxQueryResponseSizeBytes and ignores it, so this is the
    only place the ceiling is actually enforced.
    """


class _NotFound:
    """Sentinel: the controller answered 404 for this path."""


# Anything that could reshape a URL path, plus whitespace a name may not carry.
_ILLEGAL_PART_CHARS: Final = frozenset("/.\\?#%")

# 401 from the auth filter, 403 from either request handler's table refusal.
_REFUSED_STATUSES: Final = frozenset({401, 403})


class PinotClient:
    NotFound: Final = _NotFound

    def __init__(
        self,
        controller_url: str,
        broker_url: str,
        user: str | None = None,
        password: str | None = None,
        timeout_seconds: float = 30.0,
        *,
        max_response_bytes: int = 64 * 1024 * 1024,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._controller_url = controller_url.rstrip("/")
        self._broker_url = broker_url.rstrip("/")
        self._max_response_bytes = max_response_bytes
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
        if response.status_code in _REFUSED_STATUSES:
            raise PinotForbidden(f"controller GET {path} was refused")
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
        error is an HTTP 200 carrying exceptions[]. The body is streamed and
        abandoned the moment it passes max_response_bytes, because the broker
        will not stop on its own — measured, the multi-stage engine returns
        the whole result however low maxQueryResponseSizeBytes is set.
        """
        try:
            async with self._http.stream(
                "POST",
                f"{self._broker_url}/query/sql",
                json={"sql": sql, "queryOptions": options},
                timeout=timeout_seconds if timeout_seconds is not None else httpx.USE_CLIENT_DEFAULT,
            ) as response:
                if response.status_code in _REFUSED_STATUSES:
                    raise PinotForbidden("broker query was refused")
                if response.status_code >= 400:
                    raise PinotTransportError(
                        f"broker query returned {response.status_code}"
                    )
                self._check_declared_size(response)
                body = await self._read_capped(response)
        except (PinotResponseTooLarge, PinotForbidden):
            # Must not be caught by the httpx.HTTPError handler below.
            raise
        except httpx.HTTPError as exc:
            raise PinotTransportError("broker query failed") from exc
        try:
            return json.loads(body)
        except ValueError as exc:
            raise PinotTransportError("broker query returned a non-JSON body") from exc

    def _check_declared_size(self, response: httpx.Response) -> None:
        """Refuse an oversized body before a byte of it is read."""
        declared = response.headers.get("content-length")
        if declared is None:
            return
        try:
            length = int(declared)
        except ValueError:
            return
        if length > self._max_response_bytes:
            raise PinotResponseTooLarge(
                f"broker declared {length} bytes, over the {self._max_response_bytes} ceiling"
            )

    async def _read_capped(self, response: httpx.Response) -> bytes:
        chunks: list[bytes] = []
        total = 0
        async for chunk in response.aiter_bytes():
            total += len(chunk)
            if total > self._max_response_bytes:
                raise PinotResponseTooLarge(
                    f"broker response passed the {self._max_response_bytes} byte ceiling"
                )
            chunks.append(chunk)
        return b"".join(chunks)

    async def aclose(self) -> None:
        await self._http.aclose()

    @staticmethod
    def _json(response: httpx.Response, what: str) -> Any:
        try:
            return response.json()
        except ValueError as exc:
            raise PinotTransportError(f"{what} returned a non-JSON body") from exc
