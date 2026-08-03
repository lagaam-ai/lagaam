"""Table allowlists: keep an agent in its lane.

A hijacked agent must not read tables outside its grant. The check runs on the
validated SQL and fails closed — an unqualified or unparseable reference it
cannot resolve is denied, never waved through.
"""

import pytest
from pydantic import ValidationError

from lagaam.core.allowlist import check_tables_allowed, filter_catalog_metadata
from lagaam.core.errors import TableAccessDeniedError
from lagaam.core.identity import AgentIdentity
from lagaam.core.models import CatalogInfo, CatalogMetadata, SchemaInfo


def guard(sql: str, allowed: set[str] | None) -> None:
    identity = AgentIdentity(name="agent-1", allowed_tables=allowed)
    check_tables_allowed(sql, dialect="trino", identity=identity)


# --- what passes ---------------------------------------------------------


def test_none_allowlist_permits_everything() -> None:
    # No allowlist configured = unrestricted (single-tenant default).
    guard("SELECT x FROM tpch.tiny.orders", allowed=None)


def test_allowed_table_passes() -> None:
    guard("SELECT x FROM tpch.tiny.orders", allowed={"tpch.tiny.orders"})


def test_join_of_two_allowed_tables_passes() -> None:
    guard(
        "SELECT o.x FROM tpch.tiny.orders o JOIN tpch.tiny.lineitem l ON o.k=l.k",
        allowed={"tpch.tiny.orders", "tpch.tiny.lineitem"},
    )


def test_allowlist_match_is_case_insensitive() -> None:
    guard("SELECT x FROM TPCH.TINY.ORDERS", allowed={"tpch.tiny.orders"})


def test_cte_name_is_not_treated_as_a_table() -> None:
    # A CTE is a local alias, not a base table — it must not need a grant.
    guard(
        "WITH picked AS (SELECT x FROM tpch.tiny.orders) SELECT x FROM picked",
        allowed={"tpch.tiny.orders"},
    )


def test_cte_shapes_that_stay_in_scope_pass() -> None:
    # Narrowing CTE resolution to enclosing scopes must not cost the ordinary
    # spellings: chained, nested in a derived table, recursive, and a CTE
    # deliberately named after a real table it reads.
    allowed = {"tpch.tiny.orders"}
    guard(
        "WITH a AS (SELECT k FROM tpch.tiny.orders), b AS (SELECT k FROM a) "
        "SELECT k FROM b",
        allowed=allowed,
    )
    guard(
        "SELECT x FROM (WITH inner_c AS (SELECT k AS x FROM tpch.tiny.orders) "
        "SELECT x FROM inner_c) t",
        allowed=allowed,
    )
    guard(
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r "
        "WHERE n < 5) SELECT n FROM r",
        allowed=allowed,
    )
    guard(
        "WITH orders AS (SELECT k FROM tpch.tiny.orders) SELECT k FROM orders",
        allowed=allowed,
    )


# --- what is denied ------------------------------------------------------


def test_a_cte_does_not_vouch_for_the_same_name_in_an_outer_scope() -> None:
    # The CTE is declared inside the IN-subquery, so the outer bare customer
    # is a base table the engine resolves — a grant bypass if it were skipped.
    with pytest.raises(TableAccessDeniedError):
        guard(
            "SELECT c.name FROM customer c WHERE c.custkey IN "
            "(WITH customer AS (SELECT 1 AS custkey) SELECT custkey FROM customer)",
            allowed={"tpch.tiny.orders"},
        )


def test_a_cte_in_a_sibling_scope_does_not_vouch_for_a_bare_name() -> None:
    with pytest.raises(TableAccessDeniedError):
        guard(
            "WITH a AS (WITH customer AS (SELECT 1 AS c) SELECT c FROM customer) "
            "SELECT x FROM customer",
            allowed={"tpch.tiny.orders"},
        )


def test_table_outside_allowlist_is_denied() -> None:
    with pytest.raises(TableAccessDeniedError, match="tpch.tiny.customer"):
        guard("SELECT x FROM tpch.tiny.customer", allowed={"tpch.tiny.orders"})


def test_one_disallowed_table_in_a_join_denies_the_whole_query() -> None:
    with pytest.raises(TableAccessDeniedError, match="secret"):
        guard(
            "SELECT o.x FROM tpch.tiny.orders o "
            "JOIN tpch.secret.pii p ON o.k=p.k",
            allowed={"tpch.tiny.orders"},
        )


def test_disallowed_table_in_subquery_is_denied() -> None:
    with pytest.raises(TableAccessDeniedError):
        guard(
            "SELECT x FROM tpch.tiny.orders WHERE k IN "
            "(SELECT k FROM tpch.secret.pii)",
            allowed={"tpch.tiny.orders"},
        )


def test_empty_allowlist_denies_all_tables() -> None:
    # An explicit empty set means "no tables" — distinct from None.
    with pytest.raises(TableAccessDeniedError):
        guard("SELECT x FROM tpch.tiny.orders", allowed=set())


def test_unqualified_table_is_denied_when_an_allowlist_exists() -> None:
    # Can't prove which table `orders` resolves to, so fail closed.
    with pytest.raises(TableAccessDeniedError):
        guard("SELECT x FROM orders", allowed={"tpch.tiny.orders"})


def test_unparseable_sql_is_denied() -> None:
    with pytest.raises(TableAccessDeniedError):
        guard("not valid sql !!!", allowed={"tpch.tiny.orders"})


def test_check_runs_on_the_canonicalized_sql_that_executes() -> None:
    # Defense in depth: the allowlist must see the SAME string the engine runs,
    # so validation's canonical output — not the raw input — is what we check.
    from lagaam.core.safety import validate_query

    safe = validate_query(
        'select ssn from tpch.secret."pii"', dialect="trino", default_limit=10
    )
    with pytest.raises(TableAccessDeniedError, match="pii"):
        guard(safe, allowed={"tpch.tiny.orders"})


def test_multi_statement_smuggle_is_denied() -> None:
    # A trailing statement's table must still be caught, not silently dropped.
    with pytest.raises(TableAccessDeniedError, match="pii"):
        guard(
            "SELECT x FROM tpch.tiny.orders; SELECT ssn FROM tpch.secret.pii",
            allowed={"tpch.tiny.orders"},
        )


def test_table_function_with_empty_name_is_denied() -> None:
    # A data-reading table function surfaces as an unqualified table -> deny.
    with pytest.raises(TableAccessDeniedError):
        guard(
            "SELECT * FROM TABLE(system.something(x => 1))",
            allowed={"tpch.tiny.orders"},
        )


# --- name-shape bypasses --------------------------------------------------
# Each of these once matched a grant while the engine resolved a different
# table, because the check compared a lossy string projection of the name.


def test_four_part_name_is_denied() -> None:
    # sqlglot maps 4 parts onto catalog/db/name by dropping the middle one, so
    # this once checked 'tpch.tiny.orders' and ran against a nested namespace.
    with pytest.raises(TableAccessDeniedError, match="name parts"):
        guard(
            "SELECT ssn FROM tpch.tiny.secret.orders",
            allowed={"tpch.tiny.orders"},
        )


def test_four_part_name_with_allowed_prefix_is_denied() -> None:
    with pytest.raises(TableAccessDeniedError, match="name parts"):
        guard(
            "SELECT ssn FROM tpch.tiny.orders.extra",
            allowed={"tpch.tiny.orders"},
        )


def test_quoted_name_folds_the_way_trino_folds_it() -> None:
    # Trino lowercases every identifier in a table position, quoted or not:
    # "Orders" IS orders, and denying it would refuse a granted table.
    guard('SELECT x FROM tpch.tiny."Orders"', allowed={"tpch.tiny.orders"})


def test_fully_quoted_uppercase_name_matches_a_lowercase_grant() -> None:
    guard('SELECT x FROM "TPCH"."TINY"."ORDERS"', allowed={"tpch.tiny.orders"})


def test_quoted_lowercase_name_still_matches() -> None:
    # Quoting alone must not deny: "orders" and orders are the same object.
    guard('SELECT x FROM tpch.tiny."orders"', allowed={"tpch.tiny.orders"})


def test_quoted_name_outside_the_grant_is_still_denied() -> None:
    # Folding must not become a way in — only the granted name matches.
    with pytest.raises(TableAccessDeniedError, match="PII"):
        guard('SELECT x FROM tpch.secret."PII"', allowed={"tpch.tiny.orders"})


def test_non_ascii_identifier_is_denied() -> None:
    # U+212A KELVIN SIGN lowercases to ASCII 'k' in Python but not in Trino,
    # so a folded match would name a different table than the SQL renders.
    with pytest.raises(TableAccessDeniedError, match="non-ASCII"):
        guard("SELECT x FROM tpch.tiny.orderK", allowed={"tpch.tiny.orderk"})


def test_non_ascii_grant_is_rejected_at_construction() -> None:
    # A grant that can never match is a configuration bug, not a silent denial.
    with pytest.raises(ValidationError, match="non-ASCII"):
        AgentIdentity(name="agent-1", allowed_tables={"tpch.tiny.orderK"})


def test_malformed_grant_is_rejected_at_construction() -> None:
    with pytest.raises(ValidationError, match="catalog.schema.table"):
        AgentIdentity(name="agent-1", allowed_tables={"tiny.orders"})


# --- metadata filtering ---------------------------------------------------


def _metadata() -> CatalogMetadata:
    return CatalogMetadata(
        catalogs=[
            CatalogInfo(
                name="tpch",
                schemas=[
                    SchemaInfo(name="tiny", tables=["orders", "lineitem"]),
                    SchemaInfo(name="secret", tables=["pii"]),
                ],
                truncated=True,
            ),
            CatalogInfo(name="mysql", schemas=[SchemaInfo(name="app", tables=["users"])]),
        ]
    )


def test_no_allowlist_returns_metadata_unfiltered() -> None:
    identity = AgentIdentity(name="agent-1")
    assert filter_catalog_metadata(_metadata(), identity) == _metadata()


def test_filter_drops_tables_schemas_and_catalogs_outside_the_grant() -> None:
    identity = AgentIdentity(name="agent-1", allowed_tables={"TPCH.tiny.Orders"})
    filtered = filter_catalog_metadata(_metadata(), identity)
    assert len(filtered.catalogs) == 1
    catalog = filtered.catalogs[0]
    assert catalog.name == "tpch"
    assert catalog.truncated is True
    assert [s.name for s in catalog.schemas] == ["tiny"]
    assert catalog.schemas[0].tables == ["orders"]


def test_empty_allowlist_hides_everything() -> None:
    identity = AgentIdentity(name="agent-1", allowed_tables=set())
    assert filter_catalog_metadata(_metadata(), identity).catalogs == []


def test_filter_folds_engine_names_the_way_the_engine_does() -> None:
    # A connector that reports uppercase names (Oracle, Snowflake) must still
    # ground an agent whose grant is written lowercase — otherwise
    # list_catalogs shows nothing while query_data allows the same table.
    metadata = CatalogMetadata(
        catalogs=[
            CatalogInfo(
                name="ORCL",
                schemas=[SchemaInfo(name="HR", tables=["EMPLOYEES", "SALARIES"])],
            )
        ]
    )
    identity = AgentIdentity(name="agent-1", allowed_tables={"orcl.hr.employees"})
    filtered = filter_catalog_metadata(metadata, identity)
    assert filtered.catalogs[0].schemas[0].tables == ["EMPLOYEES"]


def test_filter_hides_a_non_ascii_table_no_grant_could_name() -> None:
    metadata = CatalogMetadata(
        catalogs=[
            CatalogInfo(
                name="tpch",
                schemas=[SchemaInfo(name="tiny", tables=["orders", "ord\u212Ars"])],
            )
        ]
    )
    identity = AgentIdentity(name="agent-1", allowed_tables={"tpch.tiny.orders"})
    filtered = filter_catalog_metadata(metadata, identity)
    assert filtered.catalogs[0].schemas[0].tables == ["orders"]
