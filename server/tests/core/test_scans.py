"""Tests for lagaam.core.scans: row generators and table_scan_counts.

has_unpriceable_shape now flags only row generators, which the plan's own
estimates cannot see. table_scan_counts recovers the IO plan's per-table
undercount when a table is read more than once.
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


def test_union_of_same_table_is_priceable() -> None:
    assert not unpriceable(
        "SELECT orderkey FROM tpch.tiny.orders WHERE totalprice > 100 "
        "UNION ALL SELECT orderkey FROM tpch.tiny.orders WHERE totalprice < 50"
    )


def test_scalar_subquery_over_same_table_is_priceable() -> None:
    assert not unpriceable(
        "SELECT o.orderkey, (SELECT max(totalprice) FROM tpch.tiny.orders) "
        "FROM tpch.tiny.orders o"
    )


def test_unparseable_sql_is_treated_as_unpriceable() -> None:
    # Can't prove the shape is safe, so assume the risky answer (fail safe).
    assert unpriceable("this is not sql !!!")


# --- products and generators ----------------------------------------------


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


def test_repeat_manufactures_rows_despite_a_column_feed() -> None:
    # repeat(col, 10000) reads a column and invents 10000 rows per input row,
    # so "fed by a column" does not mean "bounded by the table".
    assert unpriceable(
        "SELECT t.n FROM hive.s.orders a "
        "CROSS JOIN UNNEST(repeat(a.orderkey, 10000)) AS t(n)"
    )


def test_a_small_literal_sequence_is_priceable() -> None:
    # A date spine is how an agent gap-fills a time series, and its length is
    # spelled out in the SQL: refusing it sight-unseen blocks ordinary work.
    assert not unpriceable("SELECT n FROM UNNEST(sequence(1, 12)) AS t(n)")
    assert not unpriceable(
        "SELECT d FROM UNNEST(sequence(DATE '1996-01-01', DATE '1996-01-31', "
        "INTERVAL '1' DAY)) AS t(d)"
    )
    assert not unpriceable(
        "SELECT s.d FROM hive.s.orders o CROSS JOIN UNNEST(sequence("
        "DATE '1996-01-01', DATE '1996-12-31', INTERVAL '1' DAY)) AS s(d)"
    )


def test_a_sequence_the_gate_cannot_size_stays_unpriceable() -> None:
    # The cap still binds, a column bound is not a length, and a month step is
    # not a fixed stride — each must fall back to refusing the query.
    assert unpriceable("SELECT n FROM UNNEST(sequence(1, 100000)) AS t(n)")
    assert unpriceable(
        "SELECT n FROM hive.s.orders o CROSS JOIN "
        "UNNEST(sequence(1, o.custkey)) AS t(n)"
    )
    assert unpriceable(
        "SELECT d FROM UNNEST(sequence(DATE '1900-01-01', DATE '2100-12-31', "
        "INTERVAL '1' DAY)) AS t(d)"
    )
    assert unpriceable(
        "SELECT d FROM UNNEST(sequence(DATE '1996-01-01', DATE '1996-06-01', "
        "INTERVAL '1' MONTH)) AS t(d)"
    )


def test_unnest_of_a_literal_array_is_priceable() -> None:
    # A two-element lookup table written inline invents nothing.
    assert not unpriceable(
        "SELECT s.v FROM hive.s.orders o CROSS JOIN UNNEST(ARRAY['O', 'F']) AS s(v)"
    )


def test_a_product_of_bounded_generators_is_unpriceable() -> None:
    # Each generator passes the per-generator cap; crossed, they multiply.
    # Three 1000-row sequences are a billion rows the plan prices as none.
    assert unpriceable(
        "SELECT a.n FROM UNNEST(sequence(1, 1000)) AS a(n) "
        "CROSS JOIN UNNEST(sequence(1, 1000)) AS b(n) "
        "CROSS JOIN UNNEST(sequence(1, 1000)) AS c(n)"
    )
    assert unpriceable(
        "SELECT a.n FROM UNNEST(sequence(1, 100)) AS a(n) "
        "CROSS JOIN UNNEST(sequence(1, 100)) AS b(n)"
    )


def test_a_small_generator_product_stays_priceable() -> None:
    # A date spine crossed with a short status list is ordinary gap-filling;
    # only the *product* past the cap is refused, not multiplicity itself.
    assert not unpriceable(
        "SELECT s.d, v.x FROM UNNEST(sequence(DATE '1996-01-01', "
        "DATE '1996-01-31', INTERVAL '1' DAY)) AS s(d) "
        "CROSS JOIN UNNEST(ARRAY['O', 'F', 'P']) AS v(x)"
    )


def test_the_generator_product_cap_is_the_committed_value() -> None:
    # 500 x 2 sits exactly on the 1000-row cap; 500 x 3 crosses it. A stale
    # mutation of _MAX_INLINE_ROWS fails loudly here.
    base = (
        "SELECT a.n FROM UNNEST(sequence(1, 500)) AS a(n) "
        "CROSS JOIN UNNEST(ARRAY[{items}]) AS b(v)"
    )
    assert not unpriceable(base.format(items="'x', 'y'"))
    assert unpriceable(base.format(items="'x', 'y', 'z'"))


def test_a_cte_aliased_generator_multiplies_per_reference() -> None:
    # find_all sees one generator node; the query cross-joins it three times
    # for a billion rows. Counting nodes instead of reads was the bypass.
    generator = "WITH g AS (SELECT n FROM UNNEST(sequence(1, 1000)) AS t(n)) "
    assert unpriceable(
        generator + "SELECT a.n FROM g a CROSS JOIN g b CROSS JOIN g c"
    )
    # One reference is the same 1000 rows the per-generator cap already allows.
    assert not unpriceable(generator + "SELECT n FROM g")


def test_a_bare_series_in_a_table_position_is_a_generator() -> None:
    # sequence() manufactures rows with or without an UNNEST around it;
    # guarding only the wrapper left the same series free in FROM.
    assert unpriceable("SELECT n FROM generate_series(1, 1000000000) AS t(n)")
    assert unpriceable("SELECT n FROM TABLE(sequence(1, 1000000)) AS t(n)")
    assert unpriceable(
        "SELECT o.k, t.n FROM hive.s.orders o "
        "CROSS JOIN generate_series(1, 1000000) AS t(n)"
    )


def test_a_bare_series_counts_toward_the_product() -> None:
    # Mixed shapes: an unwrapped series must multiply like a wrapped one.
    assert unpriceable(
        "SELECT a.n FROM UNNEST(sequence(1, 40)) AS a(n) "
        "CROSS JOIN generate_series(1, 1000000) AS t(n)"
    )
    assert not unpriceable(
        "SELECT a.n FROM UNNEST(sequence(1, 10)) AS a(n) "
        "CROSS JOIN generate_series(1, 10) AS t(n)"
    )


def test_a_column_fed_unnest_does_not_inflate_the_product() -> None:
    # A column's rows belong to a table the plan already priced; only the
    # inline generator's 1000 rows multiply, and 1000 sits on the cap.
    assert not unpriceable(
        "SELECT t.i, s.n FROM hive.s.orders o "
        "CROSS JOIN UNNEST(o.items) AS t(i) "
        "CROSS JOIN UNNEST(sequence(1, 1000)) AS s(n)"
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


# --- pathological CTE chains (doubling references) -------------------------


def _doubling_chain(n: int) -> str:
    # Each CTE references the previous one twice: a naive re-walk of every
    # reference's body doubles the work per level, growing as 2^n.
    head = "WITH c0 AS (SELECT orderkey FROM tpch.sf1.orders),"
    links = ",".join(
        f"c{i} AS (SELECT a.orderkey FROM c{i - 1} a JOIN c{i - 1} b "
        f"ON a.orderkey = b.orderkey)"
        for i in range(1, n)
    )
    return f"{head}{links} SELECT orderkey FROM c{n - 1} LIMIT 5"


def test_a_doubling_cte_chain_resolves_in_under_a_second_at_n24() -> None:
    import time

    start = time.perf_counter()
    result = counts(_doubling_chain(24))
    elapsed = time.perf_counter() - start
    assert elapsed < 1.0
    assert result["tpch.sf1.orders"] <= 100_000


def test_a_doubling_cte_chain_resolves_in_under_a_second_at_n40() -> None:
    import time

    start = time.perf_counter()
    result = counts(_doubling_chain(40))
    elapsed = time.perf_counter() - start
    assert elapsed < 1.0
    assert result["tpch.sf1.orders"] <= 100_000


def test_three_way_self_join_still_returns_three() -> None:
    assert counts(
        "SELECT a.orderkey FROM tpch.tiny.orders a "
        "JOIN tpch.tiny.orders b ON a.orderkey = b.orderkey "
        "JOIN tpch.tiny.orders c ON b.orderkey = c.orderkey"
    ) == {"tpch.tiny.orders": 3}


def test_a_cte_referenced_four_times_still_returns_four() -> None:
    cte = "WITH c AS (SELECT orderkey FROM tpch.tiny.orders) "
    assert counts(
        cte + "SELECT count(*) FROM ("
        "SELECT * FROM c UNION ALL SELECT * FROM c UNION ALL "
        "SELECT * FROM c UNION ALL SELECT * FROM c) z"
    ) == {"tpch.tiny.orders": 4}


def test_a_single_scan_still_returns_one() -> None:
    assert counts("SELECT orderkey FROM tpch.tiny.orders") == {
        "tpch.tiny.orders": 1
    }


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


def test_null_and_boolean_arguments_do_not_block_a_reshape() -> None:
    # A constant carries no rows, so it must not be checked as if it fed the
    # generator: null handling is ordinary and would otherwise read as a
    # manufactured row source.
    for call in (
        "coalesce(o.items, NULL)",
        "nullif(o.items, NULL)",
        "if(o.k > 1, o.items, NULL)",
        "if(true, o.items, ARRAY[1])",
        "coalesce(o.items, true)",
    ):
        assert not unpriceable(
            f"SELECT t.n FROM hive.s.orders o CROSS JOIN UNNEST({call}) AS t(n)"
        ), call


def test_a_map_written_inline_is_a_bounded_lookup() -> None:
    # MAP(ARRAY[...], ARRAY[...]) is how an agent writes a lookup table; its
    # rows are the key array's length, which the inline cap already bounds.
    assert not unpriceable(
        "SELECT t.k FROM hive.s.orders o CROSS JOIN "
        "UNNEST(MAP(ARRAY['low','high'], ARRAY[1,2])) AS t(k, v)"
    )
    wide = ",".join(f"'k{i}'" for i in range(1001))
    values = ",".join(str(i) for i in range(1001))
    assert unpriceable(
        f"SELECT t.k FROM hive.s.orders o CROSS JOIN "
        f"UNNEST(MAP(ARRAY[{wide}], ARRAY[{values}])) AS t(k, v)"
    )
    assert unpriceable(
        "SELECT t.k FROM hive.s.orders o CROSS JOIN "
        "UNNEST(MAP(sequence(1, 99999), sequence(1, 99999))) AS t(k, v)"
    )


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


def test_a_saturated_scan_count_is_reported_as_unpriceable() -> None:
    # The walk budget bounds CPU, but a partial count scales the byte quote
    # DOWN — measured, a 1,471-char CTE chain counts 3,328 reads where the
    # true number is 524,288, so a 61 GiB query quotes at 3 GiB and is
    # admitted. Saturation has to deny, not discount.
    from lagaam.core.scans import scan_counts_saturated

    parts = ["c0 AS (SELECT orderkey FROM tpch.tiny.orders)"]
    for i in range(1, 20):
        parts.append(
            f"c{i} AS (SELECT a.orderkey FROM c{i - 1} a "
            f"JOIN c{i - 1} b ON a.orderkey = b.orderkey)"
        )
    exploding = "WITH " + ", ".join(parts) + " SELECT orderkey FROM c19"

    assert scan_counts_saturated(exploding, "trino")
    assert not scan_counts_saturated(
        "SELECT a.x FROM tpch.tiny.orders a JOIN tpch.tiny.orders b ON a.k = b.k",
        "trino",
    )


def test_the_scan_count_cap_is_the_committed_value() -> None:
    # Same reason as the depth caps in test_safety: a mutation left on disk
    # by an interrupted run must fail loudly, not pass green.
    from lagaam.core.scans import _MAX_SCAN_COUNT

    assert _MAX_SCAN_COUNT == 10_000
