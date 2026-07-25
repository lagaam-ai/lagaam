"""Detecting queries whose IO estimate would misprice the real scan.

The quote is the sum of the IO plan's per-scan entries. A join without an
equality reports a sum where the work is a product, and a row generator does
not appear at all. A table read twice is fine — Trino emits one entry per scan
operator, so the sum already counts it. The cost gate degrades a misprice-able
quote to low confidence; this module spots the shapes from the SQL.
"""

from lagaam.core.scans import has_unpriceable_shape, table_scan_counts


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


def test_self_join_is_priceable() -> None:
    # The obvious worry, and measurably wrong: Trino's IO plan emits one entry
    # per scan operator, so a self-join reports two and the sum is already
    # right. Blocking it denied year-over-year comparison for nothing.
    assert not unpriceable(
        "SELECT a.orderkey FROM tpch.tiny.lineitem a "
        "JOIN tpch.tiny.lineitem b ON a.orderkey = b.orderkey"
    )


def test_union_of_same_table_is_priceable() -> None:
    assert not unpriceable(
        "SELECT orderkey FROM tpch.tiny.orders WHERE totalprice > 100 "
        "UNION ALL SELECT orderkey FROM tpch.tiny.orders WHERE totalprice < 50"
    )


def test_cte_referenced_twice_is_priceable() -> None:
    assert not unpriceable(
        "WITH x AS (SELECT orderkey, custkey FROM tpch.tiny.orders) "
        "SELECT a.orderkey FROM x a JOIN x b ON a.custkey = b.custkey"
    )


def test_scalar_subquery_over_same_table_is_priceable() -> None:
    assert not unpriceable(
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


# --- an equality only counts if it constrains the join --------------------


def test_a_disjoined_equality_does_not_constrain() -> None:
    # `OR 1=1` satisfies every row pair, so the equality never binds. A bare
    # subtree search for exp.EQ launders a cartesian product through it.
    assert unpriceable(
        "SELECT a.x FROM hive.s.t1 a JOIN hive.s.t2 b ON a.k = b.k OR 1 = 1"
    )


def test_a_negated_equality_does_not_constrain() -> None:
    assert unpriceable(
        "SELECT a.x FROM hive.s.t1 a JOIN hive.s.t2 b ON NOT (a.k = b.k)"
    )


def test_an_equality_inside_a_case_does_not_constrain() -> None:
    assert unpriceable(
        "SELECT a.x FROM hive.s.t1 a JOIN hive.s.t2 b "
        "ON CASE WHEN a.k = b.k THEN TRUE ELSE TRUE END"
    )


def test_an_equality_between_expressions_does_not_constrain() -> None:
    # The engine cannot hash on a computed key; it compares every pair.
    assert unpriceable(
        "SELECT a.x FROM hive.s.t1 a JOIN hive.s.t2 b ON a.k + b.k = 5"
    )
    assert unpriceable(
        "SELECT a.x FROM hive.s.t1 a JOIN hive.s.t2 b ON greatest(a.k, b.k) = 1"
    )


def test_an_equality_against_a_subquery_does_not_constrain() -> None:
    assert unpriceable(
        "SELECT a.x FROM hive.s.t1 a JOIN hive.s.t2 b "
        "ON a.k = (SELECT max(z.k) FROM hive.s.t3 z)"
    )


def test_an_equality_within_one_source_does_not_constrain() -> None:
    assert unpriceable(
        "SELECT a.x FROM hive.s.t1 a JOIN hive.s.t2 b ON a.k <> b.k AND a.k = a.j"
    )


def test_one_predicate_does_not_clear_every_comma_join() -> None:
    # Three tables, one equality: the third is joined to nothing.
    assert unpriceable(
        "SELECT a.x FROM hive.s.t1 a, hive.s.t2 b, hive.s.t3 c WHERE a.k = b.k"
    )


def test_every_comma_joined_source_needs_its_own_equality() -> None:
    assert not unpriceable(
        "SELECT a.x FROM hive.s.t1 a, hive.s.t2 b, hive.s.t3 c "
        "WHERE a.k = b.k AND b.j = c.j"
    )


def test_an_equality_in_a_nested_select_does_not_clear_the_outer_join() -> None:
    assert unpriceable(
        "SELECT a.x FROM hive.s.t1 a, hive.s.t2 b WHERE a.k IN "
        "(SELECT z.k FROM hive.s.t3 z, hive.s.t4 p WHERE z.k = p.k)"
    )


def test_repeat_manufactures_rows_despite_a_column_feed() -> None:
    # repeat(col, 10000) reads a column and invents 10000 rows per input row,
    # so "fed by a column" does not mean "bounded by the table".
    assert unpriceable(
        "SELECT t.n FROM hive.s.orders a "
        "CROSS JOIN UNNEST(repeat(a.orderkey, 10000)) AS t(n)"
    )


def test_unnest_of_a_literal_array_is_priceable() -> None:
    # A two-element lookup table written inline invents nothing.
    assert not unpriceable(
        "SELECT s.v FROM hive.s.orders o CROSS JOIN UNNEST(ARRAY['O', 'F']) AS s(v)"
    )


# --- scaling a quote the plan may have collapsed --------------------------


def multiplier(sql: str) -> int:
    counts = table_scan_counts(sql, dialect="trino")
    return max(counts.values(), default=1)


def test_a_single_scan_needs_no_scaling() -> None:
    assert multiplier("SELECT orderkey FROM tpch.tiny.orders") == 1


def test_two_scans_of_one_table_double_the_quote() -> None:
    # Trino collapses two scans of the same table reading the same columns
    # into one IO entry, so the plan bills half the work.
    assert multiplier(
        "SELECT a.orderkey FROM tpch.tiny.orders a "
        "JOIN tpch.tiny.orders b ON a.orderkey = b.orderkey"
    ) == 2


def test_a_scalar_subquery_over_the_same_table_doubles_it() -> None:
    assert multiplier(
        "SELECT o.orderkey, (SELECT max(totalprice) FROM tpch.tiny.orders) "
        "FROM tpch.tiny.orders o"
    ) == 2


def test_distinct_tables_are_not_scaled() -> None:
    assert multiplier(
        "SELECT o.orderkey FROM tpch.tiny.orders o "
        "JOIN tpch.tiny.lineitem l ON o.orderkey = l.orderkey"
    ) == 1


def test_same_name_in_different_catalogs_is_not_scaled() -> None:
    assert multiplier(
        "SELECT p.id FROM prod.s.orders p JOIN stg.s.orders s ON p.id = s.id"
    ) == 1


def test_unparseable_sql_is_not_scaled() -> None:
    # The shape check has already refused it; scaling nothing is correct.
    assert multiplier("not valid sql !!!") == 1


# --- what an inline relation and an alias actually bound ------------------


def test_a_huge_inline_array_is_a_multiplier_not_a_lookup() -> None:
    # ARRAY['O','F'] is a lookup table; 20,000 elements crossed into a 1.5M
    # row scan is 30 billion rows the plan still prices as one scan.
    elements = ",".join(str(i) for i in range(20_000))
    assert unpriceable(
        f"SELECT count(x) FROM hive.s.orders a "
        f"CROSS JOIN UNNEST(ARRAY[{elements}]) AS t(x)"
    )


def test_a_huge_inline_values_relation_is_unpriceable() -> None:
    rows = ",".join(f"({i})" for i in range(20_000))
    assert unpriceable(
        f"SELECT count(v.x) FROM hive.s.orders a "
        f"CROSS JOIN (VALUES {rows}) AS v(x)"
    )


def test_two_sources_sharing_an_alias_are_unpriceable() -> None:
    # An alias is how a predicate names a source. Two sources answering to
    # one name means an equality may constrain a different source entirely.
    assert unpriceable(
        "SELECT count(a.k) FROM hive.s.orders a, hive.s.lineitem b, "
        "hive.s.customer b WHERE a.k = b.k"
    )


def test_a_correlated_subquery_without_an_equality_is_unpriceable() -> None:
    # A nested-loop product with no exp.Join node anywhere to notice it.
    assert unpriceable(
        "SELECT count(o.k) FROM hive.s.orders o WHERE "
        "(SELECT count(*) FROM hive.s.lineitem l WHERE l.q > o.p) > 0"
    )


def test_a_correlated_subquery_on_an_equality_is_priceable() -> None:
    # The planner decorrelates this into a hash join.
    assert not unpriceable(
        "SELECT c.n, (SELECT count(*) FROM hive.s.orders o WHERE o.ck = c.ck) "
        "FROM hive.s.customer c"
    )


# --- generators the parser does not model natively ------------------------


def test_an_unmodelled_generator_function_is_unpriceable() -> None:
    # sqlglot parses an unknown function as Anonymous, whose sql_name() is the
    # literal "ANONYMOUS" — a denylist of names cannot see it at all.
    for call in ("ngrams(a.words, 2)", "array_repeat(a.k, 10000)", "mystery(a.k)"):
        assert unpriceable(
            f"SELECT t.n FROM hive.s.orders a CROSS JOIN UNNEST({call}) AS t(n)"
        ), call


def test_a_reshaping_function_over_a_column_is_priceable() -> None:
    # shuffle/array_sort cannot change how many rows the array yields.
    assert not unpriceable(
        "SELECT t.i FROM hive.s.orders o CROSS JOIN UNNEST(shuffle(o.items)) AS t(i)"
    )


def test_a_generator_hidden_inside_a_literal_array_is_unpriceable() -> None:
    assert unpriceable(
        "SELECT t.n FROM hive.s.orders a "
        "CROSS JOIN UNNEST(ARRAY[sequence(1, 1000000)]) AS t(n)"
    )


# --- expression equi-joins are joins ---------------------------------------


def test_an_equality_on_computed_keys_is_priceable() -> None:
    # Trino compiles each of these to a hash InnerJoin with real criteria;
    # demanding bare columns denied ordinary year-over-year and normalized
    # joins for nothing.
    for predicate in (
        "year(a.d) = year(b.d) + 1",
        "lower(a.k) = lower(b.k)",
        "date_trunc('month', a.d) = date_trunc('month', b.d)",
        "CAST(a.k AS VARCHAR) = CAST(b.k AS VARCHAR)",
        "coalesce(a.k, 0) = b.k",
    ):
        assert not unpriceable(
            f"SELECT a.x FROM hive.s.t1 a JOIN hive.s.t2 b ON {predicate}"
        ), predicate


def test_an_equality_mixing_both_sources_on_one_side_is_unpriceable() -> None:
    # a.k + b.k = 5 reads both sources on the left, so there is no key to
    # hash: the engine still compares every pair.
    assert unpriceable(
        "SELECT a.x FROM hive.s.t1 a JOIN hive.s.t2 b ON a.k + b.k = 5"
    )
