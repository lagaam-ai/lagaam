"""TTL cache over the QueryEngine port.

Grounding calls repeat constantly (every agent turn re-grounds); the cache
keeps them off the engine. Time is injected so tests never sleep.
"""

import pytest

from lagaam.core.caching import CachingQueryEngine
from lagaam.core.errors import TableNotFoundError
from lagaam.core.ports import QueryEngine
from tests.fakes import FakeQueryEngine


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class CountingEngine(FakeQueryEngine):
    def __init__(self) -> None:
        self.list_calls = 0
        self.describe_calls = 0

    async def list_catalogs(self):  # type: ignore[no-untyped-def]
        self.list_calls += 1
        return await super().list_catalogs()

    async def describe_table(self, catalog, schema, table):  # type: ignore[no-untyped-def]
        self.describe_calls += 1
        return await super().describe_table(catalog, schema, table)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def backend() -> CountingEngine:
    return CountingEngine()


@pytest.fixture
def cached(backend: CountingEngine, clock: FakeClock) -> CachingQueryEngine:
    return CachingQueryEngine(backend, ttl_seconds=300.0, clock=clock)


def test_caching_engine_satisfies_the_port(cached: CachingQueryEngine) -> None:
    assert isinstance(cached, QueryEngine)


async def test_repeat_calls_within_ttl_hit_the_engine_once(
    cached: CachingQueryEngine, backend: CountingEngine, clock: FakeClock
) -> None:
    first = await cached.list_catalogs()
    clock.now += 299.0
    second = await cached.list_catalogs()
    assert backend.list_calls == 1
    assert first == second

    await cached.describe_table("tpch", "tiny", "orders")
    await cached.describe_table("tpch", "tiny", "orders")
    assert backend.describe_calls == 1


async def test_expired_entries_are_refetched(
    cached: CachingQueryEngine, backend: CountingEngine, clock: FakeClock
) -> None:
    await cached.list_catalogs()
    clock.now += 301.0
    await cached.list_catalogs()
    assert backend.list_calls == 2


async def test_distinct_tables_cache_separately(
    cached: CachingQueryEngine, backend: CountingEngine
) -> None:
    await cached.describe_table("tpch", "tiny", "orders")
    await cached.describe_table("tpch", "tiny", "lineitem")
    assert backend.describe_calls == 2


async def test_cache_key_ignores_identifier_case(
    cached: CachingQueryEngine, backend: CountingEngine
) -> None:
    # Engines resolve ORDERS and orders to the same table; so must the cache.
    await cached.describe_table("tpch", "tiny", "orders")
    await cached.describe_table("TPCH", "Tiny", "ORDERS")
    assert backend.describe_calls == 1


async def test_errors_are_not_cached(
    cached: CachingQueryEngine, backend: CountingEngine
) -> None:
    # A table can appear right after a miss; never remember failures.
    for _ in range(2):
        with pytest.raises(TableNotFoundError):
            await cached.describe_table("tpch", "tiny", "nope")
    assert backend.describe_calls == 2
