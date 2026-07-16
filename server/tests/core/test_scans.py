"""Detecting queries whose IO estimate would undercount physical scans.

Trino's IO plan reports one entry per distinct table, so a table scanned by
several operators (self-join, UNION of a table with itself, a CTE referenced
twice) is counted once — an underestimate. The cost gate degrades such quotes
to low confidence; this module is how it spots them from the SQL.
"""

from lagaam.core.scans import has_repeated_scan


def repeated(sql: str) -> bool:
    return has_repeated_scan(sql, dialect="trino")


def test_single_scan_is_not_repeated() -> None:
    assert not repeated("SELECT orderkey FROM tpch.tiny.orders")


def test_distinct_table_join_is_not_repeated() -> None:
    assert not repeated(
        "SELECT o.orderkey FROM tpch.tiny.orders o "
        "JOIN tpch.tiny.lineitem l ON o.orderkey = l.orderkey"
    )


def test_cte_referenced_once_is_not_repeated() -> None:
    assert not repeated(
        "WITH x AS (SELECT orderkey FROM tpch.tiny.orders) SELECT orderkey FROM x"
    )


def test_self_join_is_repeated() -> None:
    assert repeated(
        "SELECT a.orderkey FROM tpch.tiny.lineitem a "
        "JOIN tpch.tiny.lineitem b ON a.orderkey = b.orderkey"
    )


def test_union_of_same_table_is_repeated() -> None:
    assert repeated(
        "SELECT orderkey FROM tpch.tiny.orders "
        "UNION ALL SELECT orderkey FROM tpch.tiny.orders"
    )


def test_cte_referenced_twice_is_repeated() -> None:
    # The CTE re-scans its source table on each reference.
    assert repeated(
        "WITH x AS (SELECT orderkey FROM tpch.tiny.orders) "
        "SELECT a.orderkey FROM x a JOIN x b ON a.orderkey = b.orderkey"
    )


def test_scalar_subquery_over_same_table_is_repeated() -> None:
    assert repeated(
        "SELECT o.orderkey, (SELECT max(totalprice) FROM tpch.tiny.orders) "
        "FROM tpch.tiny.orders o"
    )


def test_unparseable_sql_is_treated_as_repeated() -> None:
    # Can't prove it's single-scan, so assume the risky answer (fail safe).
    assert repeated("this is not sql !!!")
