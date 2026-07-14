"""QueryEngine port: the hexagonal boundary adapters must satisfy."""

from lagaam.core.ports import QueryEngine
from tests.fakes import FakeQueryEngine


def test_fake_engine_satisfies_query_engine_protocol() -> None:
    engine: QueryEngine = FakeQueryEngine()
    assert isinstance(engine, QueryEngine)


async def test_port_contract_list_then_describe() -> None:
    engine: QueryEngine = FakeQueryEngine()
    meta = await engine.list_catalogs()
    catalog = meta.catalogs[0]
    schema = catalog.schemas[0]
    table = schema.tables[0]
    described = await engine.describe_table(catalog.name, schema.name, table)
    assert described.fqn == f"{catalog.name}.{schema.name}.{table}"
    assert described.columns, "describe_table must ground the agent with columns"
