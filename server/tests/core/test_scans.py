"""Tests for lagaam.core.scans: row generators and table_scan_counts.

has_unpriceable_shape now flags only row generators, which the plan's own
estimates cannot see. table_scan_counts recovers the IO plan's per-table
undercount when a table is read more than once.
"""

from lagaam.core.scans import (
    generator_fanout,
    has_unpriceable_shape,
    scan_counts_saturated,
    table_scan_counts,
)


def unpriceable(sql: str) -> bool:
    return has_unpriceable_shape(sql, dialect="trino")


def fanout(sql: str) -> int:
    return generator_fanout(sql, dialect="trino")


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
    # TABLE(...) wraps the series in a node named for the keyword; reading
    # that as a scan would lend the generator a row count nothing counted.
    assert unpriceable("SELECT n FROM TABLE(sequence(1, 1000000)) AS t(n)")
    # Joined to a real table it is a multiplier the quote carries instead.
    joined = (
        "SELECT o.k, t.n FROM hive.s.orders o "
        "CROSS JOIN generate_series(1, 1000000) AS t(n)"
    )
    assert not unpriceable(joined)
    assert fanout(joined) == 1_000_000


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


def test_nesting_past_the_stack_fails_closed() -> None:
    # Deep nesting raises RecursionError, which is not a SqlglotError: the
    # gate used to propagate it instead of refusing. validate_query's depth
    # cap stops this earlier in the request path, but a module that cannot
    # read a shape must not answer "priceable" for it.
    deep = "ARRAY[1,2,3]"
    for _ in range(100):
        deep = f"array_sort({deep})"
    assert unpriceable(f"SELECT v FROM hive.s.t CROSS JOIN UNNEST({deep}) AS z(v)")


def test_a_shadowed_cte_name_is_not_sized() -> None:
    # A nested WITH re-binding an outer CTE's name used to overwrite it, so
    # the walk read a decoy body and missed the generator still referenced
    # outside it: 1000^3 rows quoted as none.
    assert unpriceable(
        "WITH a AS (SELECT x FROM UNNEST(sequence(1,1000)) AS s(x)) "
        "SELECT * FROM (WITH a AS (SELECT 1 AS x) SELECT * FROM a) z "
        "CROSS JOIN a w1 CROSS JOIN a w2 CROSS JOIN a w3"
    )
    # A decoy that collides with nothing leaves the outer body readable.
    assert unpriceable(
        "WITH a AS (SELECT x FROM UNNEST(sequence(1,1000)) AS s(x)) "
        "SELECT * FROM (WITH zz AS (SELECT 1 AS x) SELECT * FROM zz) z "
        "CROSS JOIN a w1 CROSS JOIN a w2 CROSS JOIN a w3"
    )


def test_a_shadowed_cte_name_still_counts_its_reads() -> None:
    # The same decoy hid the reads from the byte gate, which returned {} and
    # scaled nothing at all.
    assert counts(
        "WITH a AS (SELECT x FROM tpch.sf1.orders) "
        "SELECT * FROM (WITH a AS (SELECT 1 AS x) SELECT * FROM a) z "
        "CROSS JOIN a w1 CROSS JOIN a w2 CROSS JOIN a w3"
    )["tpch.sf1.orders"] >= 3


def test_a_name_reused_in_a_nested_scope_still_prices() -> None:
    # Reusing "base" or "daily" in an inner scope is ordinary SQL; refusing
    # every collision would trade one bypass for a broken product.
    assert not unpriceable(
        "WITH base AS (SELECT orderkey FROM tpch.sf1.orders) "
        "SELECT * FROM (WITH base AS (SELECT custkey FROM tpch.sf1.customer) "
        "SELECT * FROM base) z"
    )


def test_a_generator_walk_of_a_doubling_chain_stays_under_a_second() -> None:
    # A chain whose product never trips the cap runs the walk in full: at 24
    # links an unbudgeted walk took 201s on a 1.2 KB query, before any engine
    # call. The walk budget must bound it the way the byte gate's already is.
    import time

    head = "WITH c0 AS (SELECT a FROM hive.s.t),"
    links = ",".join(
        f"c{i} AS (SELECT 1 AS x FROM c{i - 1} a CROSS JOIN c{i - 1} b)"
        for i in range(1, 24)
    )
    start = time.perf_counter()
    unpriceable(f"{head}{links} SELECT x FROM c23")
    assert time.perf_counter() - start < 1.0


def test_a_doubling_chain_of_fat_bodies_stays_under_a_second() -> None:
    # Budgeting the reads alone left body size — which the attacker writes —
    # unbudgeted: 10,000 re-reads of a padded body took 2.3s on 4 KB and
    # 10.6s on 19 KB. The budget has to be spent per node visited.
    import time

    pad = "+".join(["1"] * 600)
    parts = [f"c0 AS (SELECT k, {pad} AS p FROM hive.s.t)"]
    parts += [
        f"c{i} AS (SELECT a.k, {pad} AS p FROM c{i - 1} a "
        f"JOIN c{i - 1} b ON a.k = b.k)"
        for i in range(1, 15)
    ]
    sql = "WITH " + ", ".join(parts) + " SELECT k FROM c14"
    start = time.perf_counter()
    unpriceable(sql)
    assert time.perf_counter() - start < 1.0


def test_a_column_fed_unnest_does_not_inflate_the_product() -> None:
    # A column's rows belong to a table the plan already priced; only the
    # inline generator's rows multiply.
    assert not unpriceable(
        "SELECT t.i, s.n FROM hive.s.orders o "
        "CROSS JOIN UNNEST(o.items) AS t(i) "
        "CROSS JOIN UNNEST(sequence(1, 100)) AS s(n)"
    )


# --- fanout against a table the plan prices as one scan --------------------


def test_a_generator_crossed_with_a_table_reports_its_fanout() -> None:
    # The plan sizes this join as the table's row count, so the generator's
    # rows are a multiplier on every scanned row: 6M lineitems x 1000 is 6e9
    # rows quoted as one scan. The quote carries the multiplier rather than
    # the gate guessing what a table it cannot see costs.
    sql = (
        "SELECT o.orderkey, t.n FROM tpch.sf1.lineitem o "
        "CROSS JOIN UNNEST(sequence(1, 1000)) AS t(n)"
    )
    assert not unpriceable(sql)
    assert fanout(sql) == 1000
    # Standalone, nothing downstream prices it, so its own size is the cap.
    assert fanout("SELECT n FROM UNNEST(sequence(1, 1000)) AS t(n)") == 1
    assert not unpriceable("SELECT n FROM UNNEST(sequence(1, 1000)) AS t(n)")
    assert unpriceable("SELECT n FROM UNNEST(sequence(1, 1001)) AS t(n)")


def test_an_hourly_spine_over_a_month_is_not_refused_outright() -> None:
    # 744 hourly buckets is ordinary against a small table and ruinous
    # against a large one; only the plan knows which, so the gate reports
    # the multiplier and the row budget decides.
    sql = (
        "SELECT o.k, s.n FROM hive.s.t o "
        "CROSS JOIN UNNEST(sequence(1, 744)) AS s(n)"
    )
    assert not unpriceable(sql)
    assert fanout(sql) == 744


def test_a_date_spine_against_a_table_still_prices() -> None:
    # Gap-filling a year of dates per row, or crossing a short status list,
    # is the ordinary shape this gate must not refuse.
    assert not unpriceable(
        "SELECT o.orderkey, s.d FROM tpch.sf1.orders o CROSS JOIN UNNEST("
        "sequence(DATE '1996-01-01', DATE '1996-12-31', INTERVAL '1' DAY)"
        ") AS s(d)"
    )
    assert not unpriceable(
        "SELECT o.orderkey, v.x FROM tpch.sf1.orders o "
        "CROSS JOIN UNNEST(ARRAY['O', 'F', 'P']) AS v(x)"
    )


def test_the_standalone_cap_is_the_committed_value() -> None:
    # 1000 sits on the inline cap; 1001 crosses it. A stale mutation of
    # _MAX_INLINE_ROWS fails loudly here.
    assert not unpriceable("SELECT n FROM UNNEST(sequence(1, 1000)) AS t(n)")
    assert unpriceable("SELECT n FROM UNNEST(sequence(1, 1001)) AS t(n)")


def test_union_branches_are_judged_one_at_a_time() -> None:
    # Branches add rows rather than multiply them, so each is measured on its
    # own: a fanout that is fine once is fine nine times over, and the extra
    # scans are the byte gate's business — table_scan_counts reports nine
    # reads, which scales that quote nine-fold.
    branch = (
        "SELECT o.orderkey FROM tpch.sf1.lineitem o "
        "CROSS JOIN UNNEST(sequence(1, 2)) AS t{i}(n)"
    )
    nine = " UNION ALL ".join(branch.format(i=i) for i in range(9))
    assert not unpriceable(nine)
    assert counts(nine) == {"tpch.sf1.lineitem": 9}
    # A single branch over the fanout cap is still refused, and a product
    # cannot be smuggled in by splitting it across branches.
    # The widest branch is the multiplier the whole statement carries.
    wide = (
        "SELECT o.orderkey FROM tpch.sf1.lineitem o "
        "CROSS JOIN UNNEST(sequence(1, 900)) AS t(n)"
    )
    assert fanout(f"{branch.format(i=0)} UNION ALL {wide}") == 900


# --- arrays laundered through a projection ---------------------------------


def test_an_array_built_in_a_subquery_projection_is_not_bounded() -> None:
    # A column of a derived table is not a scanned column: the projection
    # can manufacture the array. 1.5M orders x 1e6 repeats is 1.5e12 rows.
    assert unpriceable(
        "SELECT x FROM (SELECT repeat(o.orderkey, 1000000) AS arr "
        "FROM tpch.sf1.orders o) s CROSS JOIN UNNEST(s.arr) AS t(x)"
    )


def test_a_generator_laundered_through_array_agg_is_refused() -> None:
    # array_agg turns a bounded generator into a column, which used to reset
    # its rows to 1 and let three unnests of it count as 1x1x1.
    assert unpriceable(
        "SELECT count(*) FROM (SELECT array_agg(n) AS arr FROM "
        "UNNEST(sequence(1, 1000)) t(n)) g "
        "CROSS JOIN UNNEST(g.arr) a(x) CROSS JOIN UNNEST(g.arr) b(y) "
        "CROSS JOIN UNNEST(g.arr) c(z)"
    )


def test_an_alias_defined_in_terms_of_itself_terminates() -> None:
    # Resolving a column through projections must not follow a cycle: these
    # shapes recursed until the interpreter's stack gave out.
    assert unpriceable(
        "SELECT x FROM (SELECT array_sort(arr) AS arr FROM hive.s.t) s "
        "CROSS JOIN UNNEST(s.arr) AS t(x)"
    )
    assert unpriceable(
        "SELECT x FROM (SELECT array_sort(b) AS a, array_sort(a) AS b "
        "FROM hive.s.t) s CROSS JOIN UNNEST(s.a) AS t(x)"
    )


def test_laundering_through_a_chain_of_aliases_is_refused() -> None:
    # Renaming the manufactured array on the way out must not launder it.
    assert unpriceable(
        "SELECT x FROM (SELECT a1 AS a2 FROM "
        "(SELECT repeat(k, 1000000) AS a1 FROM hive.s.t) q) s "
        "CROSS JOIN UNNEST(s.a2) AS t(x)"
    )


def test_a_column_alias_list_does_not_launder_an_array() -> None:
    # WITH s(arr) AS (...) binds the name on the CTE, not in an Alias node,
    # so a projection map built only from Alias nodes never saw it.
    assert unpriceable(
        "WITH s(arr) AS (SELECT repeat(k, 1000000) FROM hive.s.orders) "
        "SELECT x FROM s CROSS JOIN UNNEST(s.arr) AS q(x)"
    )
    assert unpriceable(
        "SELECT x FROM (SELECT repeat(k, 1000000) FROM hive.s.orders) s(arr) "
        "CROSS JOIN UNNEST(s.arr) AS q(x)"
    )
    assert unpriceable(
        "WITH s(arr) AS (SELECT array_agg(n) FROM hive.s.t) "
        "SELECT x FROM s CROSS JOIN UNNEST(s.arr) q(x) "
        "CROSS JOIN UNNEST(s.arr) r(y) CROSS JOIN UNNEST(s.arr) w(z)"
    )


def test_forwarding_a_manufactured_array_does_not_launder_it() -> None:
    # Re-exporting the name through another scope must not lose what built
    # it, whether the hop renames the column or just passes it along.
    assert unpriceable(
        "WITH a(c) AS (SELECT repeat(k, 1000000) FROM hive.s.t), "
        "b(arr) AS (SELECT c FROM a) "
        "SELECT x FROM b CROSS JOIN UNNEST(b.arr) q(x)"
    )
    assert unpriceable(
        "WITH s(arr) AS (SELECT repeat(k, 1000000) FROM hive.s.t), "
        "u AS (SELECT arr FROM s) "
        "SELECT x FROM u CROSS JOIN UNNEST(u.arr) q(x)"
    )


def test_a_star_does_not_strip_an_array_of_its_history() -> None:
    # SELECT * re-exports what an inner projection built without naming it,
    # so the array reached the generator as a name nothing appeared to bind.
    assert unpriceable(
        "SELECT x FROM (SELECT * FROM "
        "(SELECT repeat(k, 1000000) AS arr FROM hive.s.orders)) s "
        "CROSS JOIN UNNEST(s.arr) AS t(x)"
    )
    # q.* parses as a Column named "*", not a Star node.
    assert unpriceable(
        "SELECT x FROM (SELECT q.* FROM "
        "(SELECT repeat(k, 1000000) AS arr FROM hive.s.orders) q) s "
        "CROSS JOIN UNNEST(s.arr) AS t(x)"
    )
    # A star over a scanned table still reads as scanned columns.
    assert not unpriceable(
        "SELECT t.i FROM (SELECT * FROM hive.s.orders) s "
        "CROSS JOIN UNNEST(s.items) AS t(i)"
    )


def test_a_later_union_arm_inherits_the_first_arms_names() -> None:
    # Output names come from the first arm, so an unnamed projection in a
    # later one still reaches the outer scope — carrying its array with it.
    assert unpriceable(
        "SELECT x FROM (SELECT ARRAY[1] AS arr FROM hive.s.t "
        "UNION ALL SELECT repeat(k, 1000000) FROM hive.s.orders) s "
        "CROSS JOIN UNNEST(s.arr) AS t(x)"
    )
    assert not unpriceable("SELECT k FROM hive.s.a UNION ALL SELECT k FROM hive.s.b")


def test_an_unrelated_alias_does_not_condemn_a_scanned_column() -> None:
    # Resolving names statement-wide made any alias poison every same-named
    # column: UNNEST(o.items) is a column of o whatever a CTE calls its own
    # aggregate. Both of these are ordinary analytics SQL.
    assert not unpriceable(
        "WITH a AS (SELECT array_agg(x) AS items FROM hive.s.t GROUP BY g) "
        "SELECT cnt FROM a, hive.s.orders o CROSS JOIN UNNEST(o.items) q(cnt)"
    )
    assert not unpriceable(
        "WITH z AS (SELECT sequence(1, 5000) AS d FROM hive.s.t) "
        "SELECT d FROM hive.s.orders o CROSS JOIN UNNEST(o.d) q(d)"
    )


def test_an_alias_chain_does_not_exhaust_the_stack() -> None:
    # Resolution followed aliases with no depth bound: 600 links in 8 KB of
    # SQL raised RecursionError out of the gate, and validate_query passes
    # it because the nesting is shallow. It must answer, and fail closed.
    chain = ", ".join(
        ["repeat(k, 5) AS a0"] + [f"a{i - 1} AS a{i}" for i in range(1, 600)]
    )
    assert unpriceable(
        f"SELECT s.a599 AS out FROM (SELECT {chain} FROM hive.s.t) s "
        "CROSS JOIN UNNEST(s.a599) AS q(x)"
    )


def test_unnesting_a_scanned_column_still_prices() -> None:
    # The ordinary array-column read must survive: its rows belong to a
    # table the plan already counted.
    assert not unpriceable(
        "SELECT t.i FROM hive.s.orders o CROSS JOIN UNNEST(o.items) AS t(i)"
    )
    assert not unpriceable(
        "SELECT t.i FROM (SELECT items FROM hive.s.orders) s "
        "CROSS JOIN UNNEST(s.items) AS t(i)"
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
