"""Detecting queries whose IO estimate would misprice the real scan.

Trino's IO plan reports one entry per distinct table and the quote is their
sum, so a repeated scan undercounts, a cross join reports a sum where the work
is a product, and a row generator does not appear at all. The cost gate
degrades such quotes to low confidence; this module spots them from the SQL.
"""

from lagaam.core.scans import has_unpriceable_shape


def unpriceable(sql: str) -> bool:
    return has_unpriceable_shape(sql, dialect="trino")


def test_single_scan_is_priceable() -> None:
    assert not unpriceable("SELECT orderkey FROM tpch.tiny.orders")


def test_distinct_table_join_is_priceable() -> None:
    assert not unpriceable(
        "SELECT o.orderkey FROM tpch.tiny.orders o "
        "JOIN tpch.tiny.lineitem l ON o.orderkey = l.orderkey"
    )


def test_cte_referenced_once_is_priceable() -> None:
    assert not unpriceable(
        "WITH x AS (SELECT orderkey FROM tpch.tiny.orders) SELECT orderkey FROM x"
    )


def test_self_join_is_unpriceable() -> None:
    assert unpriceable(
        "SELECT a.orderkey FROM tpch.tiny.lineitem a "
        "JOIN tpch.tiny.lineitem b ON a.orderkey = b.orderkey"
    )


def test_union_of_same_table_is_unpriceable() -> None:
    assert unpriceable(
        "SELECT orderkey FROM tpch.tiny.orders "
        "UNION ALL SELECT orderkey FROM tpch.tiny.orders"
    )


def test_cte_referenced_twice_is_unpriceable() -> None:
    # The CTE re-scans its source table on each reference.
    assert unpriceable(
        "WITH x AS (SELECT orderkey FROM tpch.tiny.orders) "
        "SELECT a.orderkey FROM x a JOIN x b ON a.orderkey = b.orderkey"
    )


def test_scalar_subquery_over_same_table_is_unpriceable() -> None:
    assert unpriceable(
        "SELECT o.orderkey, (SELECT max(totalprice) FROM tpch.tiny.orders) "
        "FROM tpch.tiny.orders o"
    )


def test_unparseable_sql_is_treated_as_unpriceable() -> None:
    # Can't prove the shape is safe, so assume the risky answer (fail safe).
    assert unpriceable("this is not sql !!!")


# --- same name, different table -------------------------------------------


def test_same_table_name_in_different_catalogs_is_priceable() -> None:
    # Comparing prod against staging is the most common lakehouse shape there
    # is. Two distinct tables, scanned once each — the IO sum is correct, and
    # denying it leaves the agent no rewrite that would help.
    assert not unpriceable(
        "SELECT p.id, s.id FROM prod.sales.orders p "
        "JOIN staging.sales.orders s ON p.id = s.id"
    )


def test_same_table_name_in_different_schemas_is_priceable() -> None:
    assert not unpriceable(
        "SELECT a.id, b.id FROM hive.s1.events a JOIN hive.s2.events b ON a.id = b.id"
    )


# --- products and generators ----------------------------------------------


def test_cross_join_is_unpriceable() -> None:
    # The plan reports each table once and the gate sums them, but the work is
    # their product: three 100 MB tables quote as 300 MB and do 10^18 rows.
    assert unpriceable(
        "SELECT a.x FROM hive.s.t1 a CROSS JOIN hive.s.t2 b CROSS JOIN hive.s.t3 c"
    )


def test_comma_join_without_a_predicate_is_unpriceable() -> None:
    assert unpriceable("SELECT x FROM hive.s.a, hive.s.b, hive.s.c")


def test_join_with_a_using_clause_is_priceable() -> None:
    assert not unpriceable(
        "SELECT a.x FROM hive.s.t1 a JOIN hive.s.t2 b USING (id)"
    )


def test_unnest_generator_is_unpriceable() -> None:
    # UNNEST contributes no inputTableColumnInfos entry, so it is invisible to
    # the byte sum: a billion generated rows quote as free.
    assert unpriceable(
        "SELECT n FROM UNNEST(sequence(1, 1000000000)) AS t(n)"
    )


def test_unnest_against_a_real_table_is_unpriceable() -> None:
    assert unpriceable(
        "SELECT b.n FROM hive.s.big b CROSS JOIN UNNEST(sequence(1, 1000000)) AS t(n)"
    )
