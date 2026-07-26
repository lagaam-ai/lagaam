"""Detecting queries whose IO estimate would misprice the real scan.

The quote is the sum of the IO plan's per-scan entries. A join without an
equality reports a sum where the work is a product, and a row generator does
not appear at all. A table read twice reports one entry however often it is
read, so the sum undercounts by the reference count — table_scan_counts
recovers that. The cost gate degrades a misprice-able quote to low
confidence; this module spots the shapes from the SQL.
"""

import time

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
    # Not a product: both sides hash on orderkey. The plan does undercount it
    # (one entry for two reads), but that is table_scan_counts' job to scale
    # — blocking it here denied year-over-year comparison for nothing.
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


def test_a_column_equal_to_itself_does_not_constrain() -> None:
    # Confirmed on Trino 476: ON b.k = b.k plans as CrossJoin[]. A predicate
    # naming one source on both sides is constant-true, not join criteria.
    assert unpriceable(
        "SELECT a.x FROM hive.s.t1 a JOIN hive.s.t2 b ON b.k = b.k"
    )
    assert unpriceable(
        "SELECT a.x FROM hive.s.t1 a, hive.s.t2 b WHERE b.k = b.k"
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


def counts(sql: str) -> dict[str, int]:
    return table_scan_counts(sql, dialect="trino")


def multiplier(sql: str) -> int:
    return max(counts(sql).values(), default=1)


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


def test_a_cte_body_is_counted_once_per_reference() -> None:
    # Measured on Trino 476: a 4-times-referenced CTE reports one IO entry
    # and processes 4x the rows. Skipping CTE references as "a result, not a
    # scan" left the shortfall invisible to the very factor meant to fix it.
    #
    # Asserted on the whole dict, not on max(): with the CTE walk disabled the
    # alias is counted as if it were a table, so the maximum still reads 2 and
    # a max()-only assertion passes while the key is "c". plan_entry_counts
    # emits fully-qualified keys, so a wrong key means no match and a factor
    # that silently collapses to 1.
    cte = "WITH c AS (SELECT orderkey FROM tpch.tiny.orders) "
    assert counts(cte + "SELECT orderkey FROM c a") == {"tpch.tiny.orders": 1}
    assert counts(
        cte + "SELECT a.orderkey FROM c a JOIN c b ON a.orderkey = b.orderkey"
    ) == {"tpch.tiny.orders": 2}
    assert counts(
        cte + "SELECT count(*) FROM (SELECT * FROM c UNION ALL "
        "SELECT * FROM c UNION ALL SELECT * FROM c) z"
    ) == {"tpch.tiny.orders": 3}


def test_an_unreferenced_cte_body_is_not_counted() -> None:
    # A CTE nobody selects from is never scanned; counting it at its
    # definition would charge for work the engine skips.
    assert counts(
        "WITH unused AS (SELECT orderkey FROM tpch.tiny.orders) "
        "SELECT custkey FROM tpch.tiny.customer"
    ) == {"tpch.tiny.customer": 1}


def test_a_self_referencing_cte_terminates() -> None:
    # Recursion depth is the engine's business; the quote must not hang.
    assert multiplier(
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r "
        "WHERE n < 5) SELECT n FROM r"
    ) == 1


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


def test_inline_relations_multiplying_past_the_cap_are_unpriceable() -> None:
    # Each is bounded alone, so a per-relation cap waves both through while
    # the plan prices one scan: measured on Trino 476, a 1.5M-row table
    # against two 1000-row VALUES is 1.5e12 rows quoted at 1.5e6.
    rows = ",".join(f"({i})" for i in range(1000))
    literal = "ARRAY[" + ",".join(str(i) for i in range(1000)) + "]"
    assert unpriceable(
        f"SELECT count(a.k) FROM hive.s.orders a "
        f"CROSS JOIN (VALUES {rows}) AS v(x) CROSS JOIN (VALUES {rows}) AS w(y)"
    )
    assert unpriceable(
        f"SELECT count(a.k) FROM hive.s.orders a "
        f"CROSS JOIN (VALUES {rows}) AS v(x) CROSS JOIN UNNEST({literal}) AS u(z)"
    )
    # A subquery reading no table is exempt as a whole, so the product has to
    # be counted inside it too.
    assert unpriceable(
        f"SELECT count(a.k) FROM hive.s.orders a CROSS JOIN "
        f"(SELECT v.x FROM (VALUES {rows}) AS v(x) "
        f"CROSS JOIN (VALUES {rows}) AS w(y)) AS s"
    )


def test_a_wrapper_subquery_carries_its_inner_product_outward() -> None:
    # The outer SELECT sees one subquery source. What that source yields is
    # the product of what it reads, or a nested wrapper launders the
    # multiplier one level down and each level looks bounded alone.
    small = ",".join(f"({i})" for i in range(10))
    assert unpriceable(
        f"SELECT count(a.k) FROM hive.s.orders a CROSS JOIN "
        f"(SELECT p.x FROM (SELECT v.x FROM (VALUES {small}) AS v(x) "
        f"CROSS JOIN (VALUES {small}) AS w(y)) AS p "
        f"CROSS JOIN (VALUES {small}) AS q(r)) AS s "
        f"CROSS JOIN (VALUES {small}) AS u(m)"
    )


def test_deeply_nested_subqueries_do_not_burn_the_gate() -> None:
    # A subquery is reachable from every one of its ancestors, so an
    # unmemoized walk re-descends it and goes exponential in nesting depth:
    # 49 KB of SQL cost 7.3s before this was memoized. A gate that can be
    # made to burn CPU is the unbounded work it exists to refuse.
    inner = "(VALUES (1))"
    for _ in range(12):
        inner = f"(SELECT 1 AS x FROM {inner} AS a CROSS JOIN {inner} AS b)"
    sql = f"SELECT z.x FROM {inner} AS z"
    started = time.monotonic()
    # One row squared any number of times is still one row: not a product.
    assert not unpriceable(sql)
    assert time.monotonic() - started < 5.0


def test_a_set_operation_adds_its_branches_rather_than_crossing_them() -> None:
    # UNION ALL stacks rows; only sources within one branch cross. Counting
    # the whole thing as a product would block an ordinary stacked lookup.
    small = ",".join(f"({i})" for i in range(400))
    assert not unpriceable(
        f"SELECT count(a.k) FROM hive.s.orders a CROSS JOIN "
        f"(SELECT v.x FROM (VALUES {small}) AS v(x) UNION ALL "
        f"SELECT w.y FROM (VALUES {small}) AS w(y)) AS s"
    )
    # ...but crossing inside one branch still multiplies.
    assert unpriceable(
        f"SELECT count(a.k) FROM hive.s.orders a CROSS JOIN "
        f"(SELECT v.x FROM (VALUES {small}) AS v(x) "
        f"CROSS JOIN (VALUES {small}) AS w(y) UNION ALL SELECT 1) AS s"
    )


def test_inline_relations_under_the_cap_stay_priceable() -> None:
    # The cap is on the product, not the count: two small lookup tables are
    # how an agent parameterizes a query.
    small = ",".join(f"({i})" for i in range(10))
    assert not unpriceable(
        f"SELECT count(a.k) FROM hive.s.orders a "
        f"CROSS JOIN (VALUES {small}) AS v(x) CROSS JOIN (VALUES {small}) AS w(y)"
    )
    assert not unpriceable(
        "SELECT t.n FROM hive.s.orders a CROSS JOIN UNNEST(a.items) AS t(n) "
        "CROSS JOIN UNNEST(ARRAY['O','F']) AS s(f)"
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


def test_a_subquery_reading_only_its_own_sources_is_priceable() -> None:
    # No outer column appears, so there is nothing to correlate on. Reading
    # the subquery's own FROM is what tells these apart from the shape above.
    assert not unpriceable(
        "SELECT count(o.k) FROM hive.s.orders o WHERE o.ck IN "
        "(SELECT c.ck FROM hive.s.customer c WHERE c.nk = 7)"
    )
    assert not unpriceable(
        "SELECT count(n.k) FROM hive.s.nation n WHERE n.rk NOT IN "
        "(SELECT r.rk FROM hive.s.region r WHERE r.name = 'ASIA')"
    )


def test_an_outer_column_outside_the_subquery_where_is_unpriceable() -> None:
    # An outer reference binds the subquery wherever it sits. In a JOIN's ON
    # it decorrelates to a join carrying only an inequality — the same nested
    # loop, one planner rewrite further away.
    assert unpriceable(
        "SELECT count(o.k) FROM hive.s.orders o WHERE o.p > "
        "(SELECT count(*) FROM hive.s.lineitem l JOIN hive.s.partsupp p "
        "ON l.pk = p.pk AND p.aq < o.ck)"
    )
    assert unpriceable(
        "SELECT count(o.k) FROM hive.s.orders o WHERE o.p > "
        "(SELECT count(*) FROM hive.s.lineitem l GROUP BY l.pk "
        "HAVING count(*) < o.ck)"
    )


def test_two_sources_sharing_an_alias_across_a_join_are_unpriceable() -> None:
    # The shadowed pair carries a real binding equality, so only the alias
    # collision itself makes this a product — the FROM source has to be read
    # for the collision to be visible at all.
    assert unpriceable(
        "SELECT count(x.k) FROM hive.s.orders x "
        "JOIN hive.s.lineitem y ON x.ok = y.ok "
        "JOIN hive.s.customer y ON y.ck = x.ck"
    )


# --- generators the parser does not model natively ------------------------


def test_every_row_preserving_function_reshapes_a_column_freely() -> None:
    # Each name is asserted on its own: a member whose parsed node reports a
    # different name is silently inert, and the set as a whole still passes.
    for call in (
        "array_sort(o.items)",
        "array_distinct(o.items)",
        "reverse(o.items)",
        "shuffle(o.items)",
        "slice(o.items, 1, 10)",
        "trim_array(o.items, 1)",
        "filter(o.items, x -> x > 1)",
        "transform(o.items, x -> x + 1)",
        "CAST(o.items AS ARRAY(INTEGER))",
        "TRY_CAST(o.items AS ARRAY(INTEGER))",
        "coalesce(o.items, ARRAY[1])",
        "if(o.k > 1, o.items, o.other)",
        "nullif(o.items, ARRAY[1])",
    ):
        assert not unpriceable(
            f"SELECT t.n FROM hive.s.orders o CROSS JOIN UNNEST({call}) AS t(n)"
        ), call


def test_a_generator_hidden_in_any_argument_is_unpriceable() -> None:
    # IF keeps its branches under "true"/"false", not "expressions": walking
    # .this and .expressions alone would wave the generator through.
    for call in (
        "if(true, sequence(1, 100000), o.items)",
        "if(false, o.items, sequence(1, 99999))",
        "if(o.k > 1, sequence(1, 99999), o.items)",
        "coalesce(sequence(1, 99999), o.items)",
        "nullif(sequence(1, 99999), ARRAY[1])",
        "CAST(sequence(1, 100000) AS ARRAY(INTEGER))",
        "slice(sequence(1, 100000), 1, 50000)",
        "filter(sequence(1, 99999), x -> x > 1)",
    ):
        assert unpriceable(
            f"SELECT t.n FROM hive.s.orders o CROSS JOIN UNNEST({call}) AS t(n)"
        ), call


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
