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


# --- a product is what the predicate does, not how the join is spelled ----


def test_join_on_a_constant_is_unpriceable() -> None:
    # ON 1=1 is a cartesian product wearing a JOIN..ON costume: both inputs
    # are scanned once, so the byte sum is correct and says nothing.
    assert unpriceable("SELECT a.x FROM hive.s.t1 a JOIN hive.s.t2 b ON 1 = 1")
    assert unpriceable("SELECT a.x FROM hive.s.t1 a JOIN hive.s.t2 b ON true")


def test_join_on_an_inequality_is_unpriceable() -> None:
    # A nested loop: every row of one side compared against every row of the
    # other, with no key to hash or merge on.
    for predicate in ("a.k <> b.k", "a.k > b.k", "a.k BETWEEN b.lo AND b.hi"):
        assert unpriceable(
            f"SELECT a.x FROM hive.s.t1 a JOIN hive.s.t2 b ON {predicate}"
        )


def test_outer_join_on_a_constant_is_unpriceable() -> None:
    assert unpriceable("SELECT a.x FROM hive.s.t1 a LEFT JOIN hive.s.t2 b ON 1 = 1")


def test_equi_join_with_extra_predicates_is_priceable() -> None:
    assert not unpriceable(
        "SELECT a.x FROM hive.s.t1 a JOIN hive.s.t2 b "
        "ON a.k = b.k AND a.d > b.d"
    )


def test_comma_join_with_an_equality_in_where_is_priceable() -> None:
    # Semantically the JOIN..ON form the planner rewrites it to; denying one
    # while allowing the other would just teach the agent a syntax ritual.
    assert not unpriceable(
        "SELECT x FROM hive.s.a a, hive.s.b b WHERE a.k = b.k"
    )


def test_joining_a_constant_relation_is_priceable() -> None:
    # `CROSS JOIN (SELECT 0.2 AS rate)` is how an agent parameterizes a query.
    assert not unpriceable(
        "SELECT a.x * r.rate FROM hive.s.t1 a CROSS JOIN (SELECT 0.2 AS rate) r"
    )
    assert not unpriceable(
        "SELECT a.x FROM hive.s.t1 a CROSS JOIN (VALUES (1), (2)) AS v(n)"
    )


def test_unnest_of_a_generated_series_is_unpriceable() -> None:
    # UNNEST contributes no inputTableColumnInfos entry, so it is invisible to
    # the byte sum: a billion generated rows quote as free.
    assert unpriceable("SELECT n FROM UNNEST(sequence(1, 1000000000)) AS t(n)")


def test_unnest_of_a_column_is_priceable() -> None:
    # Reading a nested column expands rows of a table the plan already priced;
    # it is the standard way to query an array column, not a generator.
    assert not unpriceable(
        "SELECT o.x FROM hive.s.orders o CROSS JOIN UNNEST(o.items) AS t(i)"
    )
