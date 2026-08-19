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
    # Asserted in both orders: with the wide branch last, "widest so far" and
    # "the last one" agree, so only one of the two orders can tell them apart.
    assert fanout(f"{branch.format(i=0)} UNION ALL {wide}") == 900
    assert fanout(f"{wide} UNION ALL {branch.format(i=0)}") == 900
    # And against a branch carrying no generator at all.
    assert fanout(f"{wide} UNION ALL SELECT o.orderkey FROM tpch.sf1.lineitem o") == 900


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


def test_a_star_over_a_cte_reference_follows_it_home() -> None:
    # A star's own subtree holds no Alias when its FROM is a CTE name: the
    # array is built in the CTE body under the WITH, which is not below it.
    assert unpriceable(
        "WITH a AS (SELECT repeat(o.k, 1000000000) AS arr FROM hive.s.orders o), "
        "b AS (SELECT * FROM a) "
        "SELECT x FROM b CROSS JOIN UNNEST(b.arr) t(x)"
    )
    # Extra hops must not help either.
    assert unpriceable(
        "WITH a AS (SELECT array_agg(v) AS arr FROM hive.s.big), "
        "b AS (SELECT * FROM a), c AS (SELECT * FROM b) "
        "SELECT x FROM c CROSS JOIN UNNEST(c.arr) v(x)"
    )
    # A star over a CTE that only reads a table still reads as scanned.
    assert not unpriceable(
        "WITH a AS (SELECT items FROM hive.s.orders), b AS (SELECT * FROM a) "
        "SELECT x FROM b CROSS JOIN UNNEST(b.items) t(x)"
    )


def test_a_cte_read_through_a_table_alias_keeps_its_bindings() -> None:
    # Bindings are keyed by the CTE's own name; a reference spelled through
    # an alias asked for z.arr, found nothing, and read as a scanned column.
    assert unpriceable(
        "WITH a AS (SELECT repeat(o.id, 10000000) AS arr FROM hive.s.orders o) "
        "SELECT count(*) AS c FROM a AS z CROSS JOIN UNNEST(z.arr) AS t(x)"
    )
    assert unpriceable(
        "WITH a AS (SELECT array_agg(o.id) AS arr FROM hive.s.orders o) "
        "SELECT count(*) FROM a z CROSS JOIN UNNEST(z.arr) AS t(x)"
    )
    # An aliased CTE that only forwards a scanned column still prices.
    assert not unpriceable(
        "WITH a AS (SELECT items FROM hive.s.orders) "
        "SELECT x FROM a AS z CROSS JOIN UNNEST(z.items) AS t(x)"
    )


def test_an_alias_does_not_speak_for_an_unrelated_relation() -> None:
    # Copying a CTE's bindings onto every same-named alias in the statement
    # merged relations that share nothing. Two relations under one alias are
    # now refused rather than merged — the gate cannot tell which one a
    # reference reads, and an aggregate's array must not be read as a scan.
    assert unpriceable(
        "WITH daily AS (SELECT d, array_agg(sku) AS items FROM hive.s.sales "
        "GROUP BY d) "
        "SELECT s FROM hive.s.orders AS o CROSS JOIN UNNEST(o.items) AS q(s) "
        "UNION ALL SELECT cardinality(x.items) FROM daily AS o CROSS JOIN daily x"
    )
    # Distinct aliases for the two relations leave the scan priceable.
    assert not unpriceable(
        "WITH daily AS (SELECT d, array_agg(sku) AS items FROM hive.s.sales "
        "GROUP BY d) "
        "SELECT s FROM hive.s.orders AS o CROSS JOIN UNNEST(o.items) AS q(s) "
        "UNION ALL SELECT cardinality(x.items) FROM daily AS y CROSS JOIN daily x"
    )
    # A CTE read through an alias must not become unpriceable for it.
    assert not unpriceable(
        "WITH daily AS (SELECT d, items FROM hive.s.sales) "
        "SELECT s FROM daily AS o CROSS JOIN UNNEST(o.items) AS q(s)"
    )
    # Forwarding a scanned array down a pipeline revisits names without
    # building anything: that is a column a table supplies, not a cycle
    # with a length nobody can read.
    assert not unpriceable(
        "WITH daily AS (SELECT d, items FROM hive.s.sales), "
        "enriched AS (SELECT o.d, o.items FROM daily AS o) "
        "SELECT e.d, s FROM enriched AS e CROSS JOIN UNNEST(e.items) AS q(s)"
    )


def test_an_ambiguous_alias_is_refused_rather_than_trusted() -> None:
    # An alias that names two relations cannot be resolved — and an
    # unresolved name used to read as a scanned column, so spelling the
    # ambiguity on purpose laundered the array. Not knowing which relation a
    # generator reads is a reason to refuse, not to price it as free.
    assert unpriceable(
        "WITH a AS (SELECT repeat(o.id, 10000000) AS arr FROM hive.s.orders o) "
        "SELECT x FROM a AS z, hive.s.other AS z CROSS JOIN UNNEST(z.arr) AS q(x)"
    )
    assert unpriceable(
        "WITH a AS (SELECT repeat(o.id, 10000000) AS arr FROM hive.s.orders o), "
        "b AS (SELECT 1 AS arr) "
        "SELECT x FROM a AS z CROSS JOIN b AS z CROSS JOIN UNNEST(z.arr) AS q(x)"
    )
    # An alias used once still resolves, and an ordinary query is untouched.
    assert not unpriceable(
        "WITH daily AS (SELECT d, items FROM hive.s.sales) "
        "SELECT s FROM daily AS o, hive.s.other AS p "
        "CROSS JOIN UNNEST(o.items) AS q(s)"
    )


def test_a_values_row_binds_its_column_alias_list() -> None:
    # VALUES carries its own TableAlias, so a manufactured array named there
    # reached UNNEST as a name nothing appeared to bind.
    assert unpriceable(
        "SELECT x FROM hive.s.t CROSS JOIN (VALUES (repeat(1, 10000))) AS v(arr) "
        "CROSS JOIN UNNEST(v.arr) AS q(x)"
    )
    assert unpriceable(
        "SELECT x FROM (VALUES (repeat(1, 10000000))) AS v(arr) "
        "CROSS JOIN UNNEST(v.arr) AS q(x)"
    )
    # A literal row spelled out inline is still a lookup table.
    assert not unpriceable(
        "SELECT x FROM hive.s.t CROSS JOIN (VALUES (ARRAY[1, 2])) AS v(arr) "
        "CROSS JOIN UNNEST(v.arr) AS q(x)"
    )


def test_a_wide_query_of_aliases_resolves_in_under_a_second() -> None:
    # Copying every binding onto every alias was quadratic in query width:
    # 600 aliases over 600 bindings cost 9.9s in 17 KB, before the budgeted
    # walk and before any engine call.
    import time

    binds = ", ".join(f"c{j} b{j}" for j in range(600))
    refs = " ".join(f"CROSS JOIN a z{i}" for i in range(600))
    sql = f"WITH a AS (SELECT {binds} FROM hive.s.t) SELECT 1 FROM a z {refs}"
    start = time.perf_counter()
    unpriceable(sql)
    assert time.perf_counter() - start < 1.0


def test_a_diamond_of_star_ctes_resolves_in_under_a_second() -> None:
    # Each link stars over the previous one twice: following every path
    # re-walked the same bodies 2^depth times, 5s on a 1 KB query, before
    # the budgeted walk begins. Visiting a body once per source is enough.
    import time

    parts = ["c0 AS (SELECT repeat(k, 10) AS arr FROM hive.s.t)"]
    parts += [
        f"c{i} AS (SELECT * FROM c{i - 1} x JOIN c{i - 1} y ON x.arr = y.arr)"
        for i in range(1, 20)
    ]
    sql = (
        "WITH " + ", ".join(parts) + " SELECT v FROM c19 "
        "CROSS JOIN UNNEST(c19.arr) AS q(v)"
    )
    start = time.perf_counter()
    unpriceable(sql)
    assert time.perf_counter() - start < 1.0


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
    # 2^23 reads, counted exactly rather than truncated by the walk budget:
    # an under-count would scale the byte quote DOWN, which is the discount
    # saturation exists to refuse.
    assert result["tpch.sf1.orders"] == 2**23
    assert scan_counts_saturated(_doubling_chain(24), "trino")


def test_a_doubling_cte_chain_resolves_in_under_a_second_at_n40() -> None:
    import time

    start = time.perf_counter()
    result = counts(_doubling_chain(40))
    elapsed = time.perf_counter() - start
    assert elapsed < 1.0
    assert result["tpch.sf1.orders"] == 2**39
    assert scan_counts_saturated(_doubling_chain(40), "trino")


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
    # Joined to a table the size is carried as a multiplier rather than
    # refused, and the budget denies it against the table's real cardinality:
    # 99,999 x a 1.5M-row table is 1.5e11 rows, far past any row budget.
    lookup = (
        "SELECT t.k FROM hive.s.orders o CROSS JOIN "
        "UNNEST(MAP(sequence(1, 99999), sequence(1, 99999))) AS t(k, v)"
    )
    assert not unpriceable(lookup)
    assert fanout(lookup) == 99_999
    # Alone, nothing downstream prices it, so the inline cap still refuses.
    assert unpriceable(
        "SELECT t.k FROM UNNEST(MAP(sequence(1, 99999), sequence(1, 99999))) AS t(k, v)"
    )


def test_a_generator_hidden_in_any_argument_is_still_found() -> None:
    # IF keeps its branches under "true"/"false", not "expressions": walking
    # .this and .expressions alone would wave the generator through. Crossed
    # with a table the finding shows up as the multiplier rather than as a
    # refusal, and a generator nobody found would carry no multiplier at all.
    for call, size in (
        ("if(true, sequence(1, 100000), o.items)", 100_000),
        ("if(false, o.items, sequence(1, 99999))", 99_999),
        ("if(o.k > 1, sequence(1, 99999), o.items)", 99_999),
        ("coalesce(sequence(1, 99999), o.items)", 99_999),
        ("nullif(sequence(1, 99999), ARRAY[1])", 99_999),
        ("CAST(sequence(1, 100000) AS ARRAY(INTEGER))", 100_000),
        ("slice(sequence(1, 100000), 1, 50000)", 100_000),
        ("filter(sequence(1, 99999), x -> x > 1)", 99_999),
    ):
        joined = f"SELECT t.n FROM hive.s.orders o CROSS JOIN UNNEST({call}) AS t(n)"
        assert fanout(joined) == size, call
        # With no table to price it against, the same shape is refused.
        assert unpriceable(f"SELECT t.n FROM UNNEST({call}) AS t(n)"), call


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


def test_a_doubling_chain_is_counted_in_full_not_discounted() -> None:
    # The danger is a partial count, which scales the byte quote DOWN: this
    # chain reads the table 2^19 times, and counting fewer prices a 61 GiB
    # query at 3 GiB. Walking each name once and applying it per reference
    # reaches the exact figure inside the budget, so the quote is now right
    # rather than merely refused.
    parts = ["c0 AS (SELECT orderkey FROM tpch.tiny.orders)"]
    for i in range(1, 20):
        parts.append(
            f"c{i} AS (SELECT a.orderkey FROM c{i - 1} a "
            f"JOIN c{i - 1} b ON a.orderkey = b.orderkey)"
        )
    exploding = "WITH " + ", ".join(parts) + " SELECT orderkey FROM c19"

    assert counts(exploding) == {"tpch.tiny.orders": 2**19}
    assert not scan_counts_saturated(exploding, "trino")
    assert not scan_counts_saturated(
        "SELECT a.x FROM tpch.tiny.orders a JOIN tpch.tiny.orders b ON a.k = b.k",
        "trino",
    )


def test_a_count_that_cannot_be_finished_is_still_reported_as_saturated() -> None:
    # Saturation must keep denying where it does occur: an under-count is a
    # discount, never a pass. A chain deep enough to outrun the budget even
    # once per name still has to refuse rather than quote what it managed.
    parts = ["c0 AS (SELECT orderkey FROM tpch.tiny.orders)"]
    for i in range(1, 400):
        parts.append(
            f"c{i} AS (SELECT a.orderkey FROM c{i - 1} a "
            f"JOIN c{i - 1} b ON a.orderkey = b.orderkey)"
        )
    deep = "WITH " + ", ".join(parts) + " SELECT orderkey FROM c399"

    assert scan_counts_saturated(deep, "trino")


def test_the_safety_caps_are_the_committed_values() -> None:
    # Same reason as the depth caps in test_safety: a mutation left on disk
    # by an interrupted run must fail loudly, not pass green. Every cap is
    # pinned in both directions — a loosened one is the dangerous half, and
    # the tests around it happened to overshoot it either way.
    from lagaam.core.scans import (
        _MAX_ALIAS_DEPTH,
        _MAX_COUNTED_READS,
        _MAX_COUNTED_ROWS,
        _MAX_INLINE_ROWS,
        _MAX_RESOLVE_STEPS,
        _MAX_SCAN_COUNT,
    )

    assert _MAX_SCAN_COUNT == 10_000
    assert _MAX_INLINE_ROWS == 1000
    assert _MAX_COUNTED_ROWS == 10_000_000
    assert _MAX_COUNTED_READS == 1_000_000
    assert _MAX_ALIAS_DEPTH == 32
    assert _MAX_RESOLVE_STEPS == 10_000


# --- a column alias list names every arm, not just the first ---------------


def test_an_alias_list_binds_every_union_arm() -> None:
    # The alias list was resolved against the first Select found under the
    # body, so a second arm's manufactured array never bound to the name and
    # UNNEST read it as a scanned column: 1e9 rows priced as one.
    assert unpriceable(
        "WITH g(arr) AS (SELECT ARRAY[1] FROM tpch.sf1.orders "
        "UNION ALL SELECT repeat(1, 1000000) FROM tpch.sf1.orders) "
        "SELECT x FROM tpch.sf1.orders o CROSS JOIN g "
        "CROSS JOIN UNNEST(g.arr) AS t(x)"
    )
    # The derived-table spelling of the same shape.
    assert unpriceable(
        "SELECT x FROM tpch.sf1.orders o CROSS JOIN "
        "(SELECT ARRAY[1] FROM tpch.sf1.orders "
        "UNION ALL SELECT repeat(1, 1000000) FROM tpch.sf1.orders) AS g(arr) "
        "CROSS JOIN UNNEST(g.arr) AS t(x)"
    )
    # Control: every arm bounded is still priceable, and keeps its size.
    both_bounded = (
        "WITH g(arr) AS (SELECT ARRAY[1, 2] FROM tpch.sf1.orders "
        "UNION ALL SELECT ARRAY[3, 4, 5] FROM tpch.sf1.orders) "
        "SELECT x FROM tpch.sf1.orders o CROSS JOIN g "
        "CROSS JOIN UNNEST(g.arr) AS t(x)"
    )
    assert not unpriceable(both_bounded)
    assert fanout(both_bounded) == 3


# --- a sequence reached through a projection is charged once ---------------


def test_a_projected_sequence_is_not_charged_twice() -> None:
    # The series was counted by the column binding AND again as a loose
    # GenerateSeries, because the suppression set only held series lexically
    # under the UNNEST. A 500-row spine quoted as 250,000 denies an ordinary
    # query on the fail-safe side, which is still a denial.
    through_derived = (
        "SELECT x FROM hive.s.orders o "
        "CROSS JOIN (SELECT sequence(1, 500) AS arr) g "
        "CROSS JOIN UNNEST(g.arr) a(x)"
    )
    assert not unpriceable(through_derived)
    assert fanout(through_derived) == 500
    through_cte = (
        "WITH g AS (SELECT sequence(1, 500) AS arr) "
        "SELECT x FROM hive.s.orders o CROSS JOIN g "
        "CROSS JOIN UNNEST(g.arr) a(x)"
    )
    assert not unpriceable(through_cte)
    assert fanout(through_cte) == 500
    # Control: a series read directly still carries its own size, and two
    # distinct ones still multiply.
    assert (
        fanout(
            "SELECT n FROM hive.s.orders o "
            "CROSS JOIN UNNEST(sequence(1, 500)) AS t(n)"
        )
        == 500
    )
    assert (
        fanout(
            "SELECT n FROM hive.s.orders o "
            "CROSS JOIN UNNEST(sequence(1, 500)) AS t(n) "
            "CROSS JOIN UNNEST(sequence(1, 3)) AS u(m)"
        )
        == 1500
    )


# --- a generator only multiplies where it meets the rows -------------------


def test_a_generator_that_cannot_multiply_carries_no_fanout() -> None:
    # The multiplier was read from the branch, not from where the generator
    # sits, so a spine that yields one value per outer row was charged as if
    # it crossed every row. Measured against the 50M default budget, a 1.5M
    # row table x 365 is 547M: an ordinary report denied.
    scalar = (
        "SELECT o.orderkey, (SELECT count(*) FROM UNNEST(sequence(1, 1000)) "
        "AS a(x)) AS c FROM tpch.sf1.orders o"
    )
    assert not unpriceable(scalar)
    assert fanout(scalar) == 1

    exists = (
        "SELECT o.orderkey FROM tpch.sf1.orders o WHERE EXISTS "
        "(SELECT 1 FROM UNNEST(sequence(1, 1000)) AS a(x) WHERE a.x = o.orderkey)"
    )
    assert not unpriceable(exists)
    assert fanout(exists) == 1

    semijoin = (
        "SELECT o.orderkey FROM tpch.sf1.orders o WHERE o.orderkey IN "
        "(SELECT x FROM UNNEST(sequence(1, 100)) AS a(x))"
    )
    assert not unpriceable(semijoin)
    assert fanout(semijoin) == 1

    # Control: the shapes that DO multiply keep their multiplier.
    crossed = (
        "SELECT o.orderkey FROM tpch.sf1.orders o "
        "CROSS JOIN UNNEST(sequence(1, 1000)) AS a(x)"
    )
    assert fanout(crossed) == 1000
    comma = (
        "SELECT o.orderkey FROM tpch.sf1.orders o, "
        "UNNEST(sequence(1, 1000)) AS a(x)"
    )
    assert fanout(comma) == 1000
    joined = (
        "SELECT o.orderkey FROM tpch.sf1.orders o "
        "JOIN UNNEST(sequence(1, 1000)) AS a(x) ON a.x = o.orderkey"
    )
    assert fanout(joined) == 1000


def test_an_aggregate_over_a_generator_still_pays_for_the_widest_step() -> None:
    # The first cut of the collapsing rule read "this select aggregates" and
    # dropped the multiplier — but an aggregate at the branch's own level runs
    # AFTER the generator's rows exist, so the engine still builds them. The
    # property is whether the rows must LEAVE a scope, not whether an
    # aggregate appears somewhere above.
    assert (
        fanout(
            "SELECT count(*) FROM tpch.sf1.orders o "
            "CROSS JOIN UNNEST(sequence(1, 1000)) AS a(x)"
        )
        == 1000
    )
    assert (
        fanout(
            "SELECT o.orderkey, count(*) FROM tpch.sf1.orders o "
            "CROSS JOIN UNNEST(sequence(1, 1000)) AS a(x) GROUP BY o.orderkey"
        )
        == 1000
    )
    # Collapsed on the way out of a scope, it really is one row per group.
    assert (
        fanout(
            "SELECT 1 FROM tpch.sf1.orders o CROSS JOIN "
            "(SELECT count(*) AS n FROM UNNEST(sequence(1, 1000)) a(x)) g"
        )
        == 1
    )
    assert (
        fanout(
            "WITH g AS (SELECT count(*) AS n FROM UNNEST(sequence(1, 1000)) a(x)) "
            "SELECT 1 FROM tpch.sf1.orders o CROSS JOIN g"
        )
        == 1
    )
    # The same scope without an aggregate still multiplies.
    assert (
        fanout(
            "WITH g AS (SELECT x FROM UNNEST(sequence(1, 1000)) a(x)) "
            "SELECT 1 FROM tpch.sf1.orders o CROSS JOIN g"
        )
        == 1000
    )


def test_a_collapsed_generator_is_still_sized_before_it_is_excused() -> None:
    # Skipping the multiplier must not skip the SIZING: an unbounded spine
    # hidden behind an aggregate or a predicate subquery has to stay refused,
    # or the collapse rule becomes the laundering route.
    assert unpriceable(
        "SELECT 1 FROM tpch.sf1.orders o CROSS JOIN "
        "(SELECT count(*) AS n FROM UNNEST(repeat(o.orderkey, 1000000)) a(x)) g"
    )
    assert unpriceable(
        "SELECT 1 FROM tpch.sf1.orders o WHERE EXISTS "
        "(SELECT 1 FROM UNNEST(repeat(o.orderkey, 1000000)) a(x))"
    )
    assert unpriceable(
        "SELECT o.orderkey, (SELECT count(*) FROM "
        "UNNEST(repeat(o.orderkey, 1000000)) a(x)) FROM tpch.sf1.orders o"
    )


def test_a_wide_query_of_aliases_and_columns_stays_under_a_second() -> None:
    # Resolving aliases copied every bound column to every alias naming its
    # relation, unbudgeted and before the walk budget was charged: the cost
    # is the alias x column cross-product, not the recursion the visited set
    # already bounds. Measured at 165 KB — inside the 200,000-char cap — that
    # was 33 seconds and 12 GB of RSS before Trino was ever contacted.
    import time

    columns = ", ".join(f"1 AS c{i}" for i in range(3000))
    aliases = ", ".join(f"b z{i}" for i in range(3000))
    wide = (
        f"WITH b AS (SELECT {columns} FROM tpch.tiny.orders) "
        f"SELECT 1 AS c FROM b, {aliases}"
    )

    start = time.perf_counter()
    unpriceable(wide)
    assert time.perf_counter() - start < 1.0

    # The over-block half of the same defect: the budget was spent re-walking
    # a body once per reference, so its WIDTH — which the analyst writes —
    # exhausted it before any generator was reached. A 400-column CTE read
    # eight times is an ordinary wide fact table, and it must keep its quote.
    ordinary = (
        "WITH b AS (SELECT "
        + ", ".join(f"1 AS c{i}" for i in range(400))
        + " FROM tpch.tiny.orders) SELECT 1 AS c FROM b, "
        + ", ".join(f"b z{i}" for i in range(8))
    )
    assert not unpriceable(ordinary)


def test_an_aggregate_over_a_joined_generator_still_pays_for_it() -> None:
    # The collapse rule's first cut excused any aggregating scope, so wrapping
    # a table-crossed generator in one quoted 6 billion rows as six million —
    # a 1000x discount, and the same defect class the rule was fixing, only
    # pointing the other way. An aggregate spares the work only where the
    # scope builds nothing of its own.
    through_cte = (
        "WITH big AS (SELECT count(*) AS c FROM tpch.sf1.lineitem l "
        "CROSS JOIN UNNEST(sequence(1, 1000)) AS t(n)) SELECT * FROM big"
    )
    through_derived = (
        "SELECT * FROM (SELECT count(*) AS c FROM tpch.sf1.lineitem l "
        "CROSS JOIN UNNEST(sequence(1, 1000)) AS t(n)) big"
    )
    plain = (
        "SELECT count(*) AS c FROM tpch.sf1.lineitem l "
        "CROSS JOIN UNNEST(sequence(1, 1000)) AS t(n)"
    )
    # Identical real work, so identical quotes whichever way it is spelled.
    assert fanout(through_cte) == 1000
    assert fanout(through_derived) == 1000
    assert fanout(plain) == 1000
    # A scope that really does build the rows alone still collapses them.
    assert (
        fanout(
            "WITH g AS (SELECT count(*) AS n FROM UNNEST(sequence(1, 1000)) a(x)) "
            "SELECT 1 FROM tpch.sf1.orders o CROSS JOIN g"
        )
        == 1
    )


def test_a_wide_statement_of_ctes_and_unnests_stays_bounded() -> None:
    # Resolving a generator's input was the one path nothing charged: the
    # depth cap bounds a single chain of names, not how many chains a wide
    # statement writes. Measured before the budget reached the resolvers,
    # 71 KB of this cost 62 seconds of CPU — pre-auth, no engine contacted.
    import time

    ctes = ",".join(
        f"c{i} AS (SELECT o_orderkey AS arr FROM tpch.sf1.orders)" for i in range(1100)
    )
    joins = " ".join(f"CROSS JOIN UNNEST(arr) AS t{j}(x{j})" for j in range(260))
    wide = f"WITH {ctes} SELECT 1 FROM c0 {joins} LIMIT 10"

    start = time.perf_counter()
    unpriceable(wide)
    fanout(wide)
    assert time.perf_counter() - start < 2.0


def test_a_resolve_that_runs_out_of_budget_refuses() -> None:
    # Exhaustion has to fail closed on both sides: an input nobody finished
    # reading is not vouched for, and a width nobody finished measuring is
    # not a small one.
    import sqlglot

    from lagaam.core.scans import _MAX_COUNTED_ROWS, _generator_rows, _is_bounded_input

    spent = [0]
    array = sqlglot.parse_one("ARRAY[1, 2, 3]", dialect="trino")
    assert not _is_bounded_input(array, {}, frozenset(), spent)
    unnest = sqlglot.parse_one(
        "SELECT x FROM UNNEST(ARRAY[1, 2, 3]) t(x)", dialect="trino"
    )
    assert _generator_rows(unnest, {}, frozenset(), [0]) == _MAX_COUNTED_ROWS


def test_a_long_spine_against_a_table_is_priced_not_refused() -> None:
    # The flat 1000-row cap survived in _is_bounded_input, so it refused the
    # very shapes ADR 0006 says to price: seven years of days against a
    # 25-row table is 63,950 rows, and a year of hourly buckets is ordinary
    # reporting. Joined to a table the size is a multiplier the budget
    # applies to a real cardinality.
    spine = (
        "SELECT n.n_name, t.d FROM tpch.sf1.nation n CROSS JOIN "
        "UNNEST(sequence(date '2020-01-01', date '2026-12-31', "
        "interval '1' day)) AS t(d)"
    )
    assert not unpriceable(spine)
    assert fanout(spine) == 2557

    hours = (
        "SELECT n.n_name, t.n FROM tpch.sf1.nation n "
        "CROSS JOIN UNNEST(sequence(1, 8760)) AS t(n)"
    )
    assert not unpriceable(hours)
    assert fanout(hours) == 8760

    # With no table to price it against, the inline cap still binds: nothing
    # downstream would carry the multiplier.
    assert unpriceable("SELECT n FROM UNNEST(sequence(1, 1001)) AS t(n)")
    assert unpriceable("SELECT n FROM UNNEST(sequence(1, 8760)) AS t(n)")
    # And a spine past what counting will follow is still refused outright.
    assert unpriceable(
        "SELECT n.n_name, t.n FROM tpch.sf1.nation n "
        "CROSS JOIN UNNEST(sequence(1, 100000000)) AS t(n)"
    )


def test_a_table_the_generator_never_meets_does_not_raise_the_cap() -> None:
    # The loosened cap asks whether the plan will carry these rows, and a
    # table inside EXISTS, an uncorrelated scalar subquery or an IN predicate
    # carries nothing: the generator still runs alone. Reading "a table is
    # mentioned somewhere" instead let a 10,000,000-row spine through where
    # 1000 is the limit.
    for where in (
        "WHERE EXISTS (SELECT 1 FROM tpch.sf1.orders o)",
        "WHERE n IN (SELECT o.orderkey FROM tpch.sf1.orders o)",
    ):
        assert unpriceable(
            f"SELECT n FROM UNNEST(sequence(1, 5000000)) AS t(n) {where}"
        ), where
    assert unpriceable(
        "SELECT n, (SELECT count(*) FROM tpch.sf1.orders) "
        "FROM UNNEST(sequence(1, 5000000)) AS t(n)"
    )
    assert unpriceable(
        "SELECT n FROM UNNEST(sequence(1, 5000000)) AS t(n) "
        "WHERE n NOT IN (SELECT o.orderkey FROM tpch.sf1.orders o)"
    )
    # Controls: a table the generator really is crossed with prices it, in
    # every spelling, and an unrelated EXISTS alongside changes nothing.
    spine = "UNNEST(sequence(1, 5000000)) AS t(n)"
    for joined in (
        f"SELECT o.orderkey, t.n FROM tpch.sf1.orders o CROSS JOIN {spine}",
        f"SELECT o.orderkey, t.n FROM tpch.sf1.orders o, {spine}",
        (
            "WITH b AS (SELECT orderkey FROM tpch.sf1.orders) "
            f"SELECT b.orderkey, t.n FROM b CROSS JOIN {spine}"
        ),
        (
            f"SELECT o.orderkey, t.n FROM tpch.sf1.orders o CROSS JOIN {spine} "
            "WHERE EXISTS (SELECT 1 FROM tpch.sf1.nation x)"
        ),
    ):
        assert not unpriceable(joined), joined
        assert fanout(joined) == 5_000_000, joined


def test_an_alias_list_binds_every_set_operation_arm() -> None:
    # Binding every UNION arm was not enough: _union_branches recursed only
    # on exp.Union, so one pair of parentheses (which parses as a Subquery)
    # or an INTERSECT/EXCEPT hid the arm that built the array, and UNNEST
    # read it as a scanned column again.
    hidden = "repeat(1, 1000000)"
    for body in (
        f"SELECT ARRAY[1] UNION ALL (SELECT ARRAY[2] UNION ALL SELECT {hidden})",
        f"SELECT ARRAY[1] EXCEPT SELECT {hidden}",
        f"SELECT ARRAY[1] INTERSECT SELECT {hidden}",
        f"(SELECT {hidden}) UNION ALL SELECT ARRAY[1]",
    ):
        assert unpriceable(
            f"WITH v(arr) AS ({body}) SELECT o.orderkey, x "
            "FROM tpch.sf1.orders o CROSS JOIN v CROSS JOIN UNNEST(v.arr) AS t(x)"
        ), body
    # Control: every arm bounded stays priceable, and keeps its widest size.
    bounded = (
        "WITH v(arr) AS (SELECT ARRAY[1] UNION ALL "
        "(SELECT ARRAY[2, 3] UNION ALL SELECT ARRAY[4, 5, 6])) "
        "SELECT o.orderkey, x FROM tpch.sf1.orders o "
        "CROSS JOIN v CROSS JOIN UNNEST(v.arr) AS t(x)"
    )
    assert not unpriceable(bounded)
    assert fanout(bounded) == 3


def test_a_scope_that_narrows_nothing_does_not_excuse_the_multiplier() -> None:
    # The collapse rule asked whether a scope holds an aggregate node. The
    # property that matters is whether it REDUCES cardinality, and GROUP BY
    # on an already-distinct spine is the identity: 10,000 rows in, 10,000
    # out. A window function is an AggFunc node and collapses nothing at all.
    spine = "SELECT x{extra} FROM UNNEST(sequence(1, 10000)) AS t(x){tail}"
    for label, extra, tail in (
        ("group by the spine key", "", " GROUP BY x"),
        ("window function", ", count(*) OVER () AS c", ""),
        ("aggregate only in a scalar subquery", ", (SELECT max(1)) AS m", ""),
    ):
        sql = (
            "SELECT l.orderkey, s.x FROM tpch.sf1.lineitem l CROSS JOIN ("
            + spine.format(extra=extra, tail=tail)
            + ") s"
        )
        assert fanout(sql) == 10_000, label

    # Control: a real reduction to one row still excuses the multiplier.
    assert (
        fanout(
            "SELECT l.orderkey, s.n FROM tpch.sf1.lineitem l CROSS JOIN "
            "(SELECT count(*) AS n FROM UNNEST(sequence(1, 10000)) AS t(x)) s"
        )
        == 1
    )
    # And a group whose key is not the spine still collapses it: the rows
    # leaving are the distinct keys, which the generator does not set.
    assert (
        fanout(
            "SELECT l.orderkey, s.c FROM tpch.sf1.lineitem l CROSS JOIN "
            "(SELECT count(*) AS c FROM UNNEST(sequence(1, 10000)) AS t(x) "
            "GROUP BY x % 7) s"
        )
        == 1
    )


def test_a_group_by_collapses_only_where_sql_can_say_so() -> None:
    # Whether a GROUP BY reduces rows is a cardinality question, and ADR 0004
    # keeps those with the plan. The one case SQL settles alone is a key list
    # naming exactly what the select's generators enumerate.
    outer = "SELECT l.orderkey, s.v FROM tpch.sf1.lineitem l CROSS JOIN ({body}) s"

    # Keys are the whole of what two spines make: the identity, so the
    # multiplier stands.
    assert (
        fanout(
            outer.format(
                body="SELECT x AS v FROM UNNEST(sequence(1, 10000)) AS t(x) "
                "CROSS JOIN UNNEST(ARRAY[1, 2]) AS u(y) GROUP BY x, y"
            )
        )
        == 20_000
    )
    # A subset of them is a reduction the SQL cannot size.
    assert (
        fanout(
            outer.format(
                body="SELECT x AS v FROM UNNEST(sequence(1, 10000)) AS t(x) "
                "CROSS JOIN UNNEST(ARRAY[1, 2]) AS u(y) GROUP BY x"
            )
        )
        == 1
    )
    # A scope that also reads a table keeps the multiplier whatever it
    # groups by: the join may filter the spine to nothing or to all of it,
    # and which one is the plan's answer. Charging it is the safe half of
    # that ignorance, and the same over-quote ADR 0006 records for an
    # equi-joined generator.
    assert (
        fanout(
            outer.format(
                body="SELECT t.x AS v FROM UNNEST(sequence(1, 10000)) AS t(x) "
                "JOIN tpch.sf1.nation n ON n.nationkey = t.x GROUP BY t.x"
            )
        )
        == 10_000
    )


def test_a_rollup_over_a_spine_still_pays_for_it() -> None:
    # ROLLUP, CUBE and GROUPING SETS hold their keys in their own args, not
    # in group.expressions, so the identity test read them as an empty key
    # list and excused the multiplier. They are also the one grouping shape
    # that ADDS rows — a rollup emits the subtotal rows on top of the groups
    # — so excusing them was doubly wrong.
    outer = "SELECT l.orderkey, s.v FROM tpch.sf1.lineitem l CROSS JOIN ({body}) s"
    for grouping in (
        "GROUP BY ROLLUP(x)",
        "GROUP BY CUBE(x)",
        "GROUP BY GROUPING SETS ((x), ())",
    ):
        sql = outer.format(
            body=f"SELECT x AS v FROM UNNEST(sequence(1, 10000)) AS t(x) {grouping}"
        )
        assert fanout(sql) == 10_000, grouping


def test_a_group_key_is_read_with_its_qualifier() -> None:
    # The identity test compared key names to generator column names with
    # the qualifier dropped, so a key naming a different relation's "x" read
    # as the spine's own. A qualifier is exactly what tells them apart.
    outer = "SELECT l.orderkey, s.v FROM tpch.sf1.lineitem l CROSS JOIN ({body}) s"
    two = (
        "SELECT a.x AS v FROM UNNEST(sequence(1, 1000)) AS a(x) "
        "CROSS JOIN UNNEST(sequence(1, 1000)) AS b(y) GROUP BY {keys}"
    )
    # Both generators' columns, each qualified: the identity.
    assert fanout(outer.format(body=two.format(keys="a.x, b.y"))) == 1_000_000
    # A qualifier naming a relation that does not supply the column is a
    # shape this cannot read, so the multiplier stands.
    assert fanout(outer.format(body=two.format(keys="a.x, a.y"))) == 1
    # Unqualified keys name whichever generator supplies them, which is
    # unambiguous exactly when one does.
    assert fanout(outer.format(body=two.format(keys="x, y"))) == 1_000_000
    assert (
        fanout(
            outer.format(
                body="SELECT x AS v FROM UNNEST(sequence(1, 10000)) AS t(x) GROUP BY x"
            )
        )
        == 10_000
    )
    # A name two generators share says nothing about which one a bare key
    # reads, so the multiplier stands rather than being guessed away.
    shared = (
        "SELECT a.x AS v FROM UNNEST(sequence(1, 1000)) AS a(x) "
        "CROSS JOIN UNNEST(sequence(1, 1000)) AS b(x) GROUP BY x"
    )
    assert fanout(outer.format(body=shared)) == 1


def test_a_values_arm_binds_like_a_select_arm() -> None:
    # VALUES is a set-operation arm that projects without a Select node, so
    # walking arms and asking each for its Select skipped it entirely and the
    # array it spelled out never bound to the name.
    assert unpriceable(
        "WITH v(arr) AS (SELECT ARRAY[1] UNION ALL VALUES (repeat(1, 1000000))) "
        "SELECT o.orderkey, x FROM tpch.sf1.orders o CROSS JOIN v "
        "CROSS JOIN UNNEST(v.arr) AS t(x)"
    )
    assert unpriceable(
        "SELECT o.orderkey, x FROM tpch.sf1.orders o CROSS JOIN "
        "(SELECT ARRAY[1] UNION ALL VALUES (repeat(1, 1000000))) AS v(arr) "
        "CROSS JOIN UNNEST(v.arr) AS t(x)"
    )
    # Control: bounded arms stay priceable, widest size carried.
    bounded = (
        "WITH v(arr) AS (SELECT ARRAY[1] UNION ALL VALUES (ARRAY[1, 2, 3])) "
        "SELECT o.orderkey, x FROM tpch.sf1.orders o CROSS JOIN v "
        "CROSS JOIN UNNEST(v.arr) AS t(x)"
    )
    assert not unpriceable(bounded)
    assert fanout(bounded) == 3


def test_a_table_a_subquery_hides_does_not_raise_the_spine_cap() -> None:
    # Detaching relations under EXISTS and IN was not enough: a scalar or
    # quantified subquery in WHERE, HAVING or ORDER BY carries a table the
    # generator never meets just the same, and it unlocked the 10,000,000
    # cap for a spine that runs alone.
    for clause in (
        "WHERE n = (SELECT count(*) FROM tpch.sf1.orders)",
        "WHERE n = ANY (SELECT orderkey FROM tpch.sf1.orders)",
        "WHERE n > ALL (SELECT orderkey FROM tpch.sf1.orders)",
        "WHERE n < (SELECT max(orderkey) FROM tpch.sf1.orders)",
        "GROUP BY n HAVING count(*) > (SELECT count(*) FROM tpch.sf1.orders)",
        "ORDER BY (SELECT count(*) FROM tpch.sf1.orders)",
    ):
        assert unpriceable(
            f"SELECT n FROM UNNEST(sequence(1, 5000000)) AS t(n) {clause}"
        ), clause
    # Control: a table the spine really is crossed with still prices it,
    # even with one of those subqueries alongside.
    joined = (
        "SELECT o.orderkey, t.n FROM tpch.sf1.orders o "
        "CROSS JOIN UNNEST(sequence(1, 5000000)) AS t(n) "
        "WHERE o.orderkey < (SELECT max(nationkey) FROM tpch.sf1.nation)"
    )
    assert not unpriceable(joined)
    assert fanout(joined) == 5_000_000


def test_an_ordinality_counter_counts_as_a_column_the_generator_makes() -> None:
    # WITH ORDINALITY names its counter outside the alias list, so a key list
    # naming both it and the value read as a mismatch and excused the
    # multiplier — the identity case the rule exists to protect, priced at 1.
    outer = "SELECT n.n_name, s.v FROM tpch.tiny.nation n CROSS JOIN ({body}) s"
    spine = (
        "SELECT t.x AS v FROM UNNEST(sequence(1, 10000)) "
        "WITH ORDINALITY AS t(x, ord) GROUP BY {keys}"
    )
    # The counter numbers the rows, so it and the value each identify a row
    # on their own: all three keyings are one group per row produced.
    assert fanout(outer.format(body=spine.format(keys="t.x, t.ord"))) == 10_000
    assert fanout(outer.format(body=spine.format(keys="t.x"))) == 10_000
    assert fanout(outer.format(body=spine.format(keys="t.ord"))) == 10_000
    # A key over an expression can still merge rows, counter or not.
    assert fanout(outer.format(body=spine.format(keys="t.ord % 7"))) == 1


def test_a_doubling_chain_of_generator_ctes_stays_under_a_second() -> None:
    # _reads_a_table follows CTE references with no budget, so a chain each
    # link of which reads the previous one twice doubled its work per level:
    # measured, 1,072 characters cost 90 seconds before Trino was contacted.
    import time

    parts = ["c0 AS (SELECT x FROM UNNEST(sequence(1, 10)) AS t(x))"]
    for i in range(1, 22):
        parts.append(f"c{i} AS (SELECT a.x FROM c{i - 1} a CROSS JOIN c{i - 1} b)")
    chain = "WITH " + ", ".join(parts) + " SELECT x FROM c21"

    start = time.perf_counter()
    unpriceable(chain)
    fanout(chain)
    assert time.perf_counter() - start < 1.0


def test_deep_set_operation_nesting_refuses_rather_than_raising() -> None:
    # Walking a set operation's arms recurses once per arm, so a long chain
    # of UNIONs exhausted the interpreter's stack AFTER the parse — where
    # neither entry point was catching it — and _estimate_cost saw an
    # exception instead of the "no quote" this module promises.
    deep = (
        "WITH v(arr) AS ("
        + " UNION ALL ".join(["SELECT ARRAY[1]"] * 1500)
        + ") SELECT x FROM v CROSS JOIN UNNEST(v.arr) AS t(x)"
    )
    assert unpriceable(deep)
    assert fanout(deep) == 1


def test_group_by_all_is_read_as_the_grouping_it_is() -> None:
    # GROUP BY ALL groups by every non-aggregate projection, which over a
    # lone spine is the identity — but it leaves group.expressions empty, so
    # the identity test read it as "no keys" and excused the multiplier.
    outer = "SELECT n.n_name, s.v FROM tpch.tiny.nation n CROSS JOIN ({body}) s"
    assert (
        fanout(
            outer.format(
                body="SELECT x AS v FROM UNNEST(sequence(1, 10000)) AS t(x) "
                "GROUP BY ALL"
            )
        )
        == 10_000
    )
    # GROUP BY () is the empty grouping: one row, a real collapse.
    assert (
        fanout(
            outer.format(
                body="SELECT count(*) AS v FROM UNNEST(sequence(1, 10000)) AS t(x) "
                "GROUP BY ()"
            )
        )
        == 1
    )


def test_an_identity_key_is_read_by_what_it_names_not_how_it_is_spelled() -> None:
    # The identity test required each key to be a bare Column node, which is
    # the key's syntax rather than the partition it makes: GROUP BY 1 and
    # GROUP BY (x) group by the same column and were excused anyway. And a
    # counter is a bijection of the row it counts, so grouping by either
    # half of an ORDINALITY pair is still one group per row produced.
    outer = "SELECT o.orderkey, g.v FROM tpch.sf1.orders o CROSS JOIN ({body}) g"
    spine = "UNNEST(sequence(1, 10000))"
    numbered = f"{spine} WITH ORDINALITY AS t(x, n)"
    for body in (
        f"SELECT x AS v FROM {spine} AS t(x) GROUP BY x",
        f"SELECT x AS v FROM {spine} AS t(x) GROUP BY 1",
        f"SELECT x AS v FROM {spine} AS t(x) GROUP BY (x)",
        f"SELECT x AS v FROM {numbered} GROUP BY x",
        f"SELECT n AS v FROM {numbered} GROUP BY n",
    ):
        assert fanout(outer.format(body=body)) == 10_000, body
    # An ordinal past the projection list names nothing, and a key over an
    # expression really can merge rows: both stay charged as reductions.
    assert (
        fanout(
            outer.format(
                body="SELECT count(*) AS v FROM UNNEST(sequence(1, 10000)) AS t(x) "
                "GROUP BY x % 7"
            )
        )
        == 1
    )


def test_a_table_a_join_predicate_hides_does_not_raise_the_spine_cap() -> None:
    # Keeping the whole `joins` argument kept the tables inside a JOIN's ON
    # clause too, so the very predicate the gate refuses in WHERE admitted a
    # 5,000,000-row invented spine when written in ON instead.
    spine = (
        "SELECT a.x, b.y FROM UNNEST(sequence(1, 10000)) AS a(x) "
        "CROSS JOIN UNNEST(sequence(1, 500)) AS b(y) "
        "JOIN (VALUES (1)) AS v(k) ON {predicate}"
    )
    for predicate in (
        "EXISTS (SELECT 1 FROM tpch.sf1.orders)",
        "v.k IN (SELECT orderkey FROM tpch.sf1.orders)",
        "v.k = (SELECT count(*) FROM tpch.sf1.orders)",
    ):
        assert unpriceable(spine.format(predicate=predicate)), predicate
    # Control: a table the JOIN really pairs with still prices the spine.
    joined = (
        "SELECT o.orderkey, t.n FROM tpch.sf1.orders o "
        "JOIN UNNEST(sequence(1, 5000000)) AS t(n) ON t.n = o.orderkey"
    )
    assert not unpriceable(joined)
    assert fanout(joined) == 5_000_000


def test_counting_reads_refuses_rather_than_raising_on_deep_nesting() -> None:
    # The read walk recurses once per CTE reference and only the parse was
    # guarded, so a chain of a thousand links raised out of both counting
    # entry points — and engine.py treats neither as an engine failure, so
    # the quote crashed instead of being withheld.
    parts = ["a0 AS (SELECT orderkey FROM tpch.tiny.orders)"]
    for i in range(1, 1200):
        parts.append(f"a{i} AS (SELECT orderkey FROM a{i - 1})")
    chain = "WITH " + ", ".join(parts) + " SELECT orderkey FROM a1199"

    assert table_scan_counts(chain, "trino") == {}
    assert scan_counts_saturated(chain, "trino")


def test_an_ordinal_outside_the_projection_list_names_nothing() -> None:
    # A positional key is only a column where the position exists: without
    # the bounds check an out-of-range ordinal raised IndexError out of the
    # gate, and GROUP BY 0 read the last projection as the first.
    outer = "SELECT o.orderkey, g.v FROM tpch.sf1.orders o CROSS JOIN ({body}) g"
    spine = "SELECT x AS v FROM UNNEST(sequence(1, 10000)) AS t(x) GROUP BY {key}"
    assert fanout(outer.format(body=spine.format(key="1"))) == 10_000
    for key in ("0", "-1", "9"):
        assert fanout(outer.format(body=spine.format(key=key))) == 1, key


def test_the_relation_a_join_names_is_kept() -> None:
    # Stripping a JOIN's predicate must not strip the relation it joins: a
    # spine joined to a real table is priced from that table's cardinality,
    # and dropping it would refuse an ordinary query outright.
    joined = (
        "SELECT t.n FROM UNNEST(sequence(1, 5000000)) AS t(n) "
        "JOIN tpch.sf1.orders o ON o.orderkey = t.n"
    )
    assert not unpriceable(joined)
    assert fanout(joined) == 5_000_000


def test_a_constant_group_key_adds_no_partition_and_hides_nothing() -> None:
    # A constant key partitions nothing: GROUP BY x and GROUP BY x, TRUE are
    # the same partition. Treating "this key names no column" as "this is not
    # the identity" let one extra token drop a 10,000,000x multiplier.
    outer = "SELECT o.orderkey, g.v FROM tpch.sf1.orders o CROSS JOIN ({body}) g"
    spine = "SELECT x AS v FROM UNNEST(sequence(1, 10000)) AS t(x) GROUP BY {keys}"
    for keys in (
        "x",
        "x, TRUE",
        "x, NULL",
        "x, 'k'",
        "TRUE, x",
        "1, TRUE",
        "x, current_date",
        "x, 1 + 1",
        "x, abs(-1)",
        "x, -1",
    ):
        assert fanout(outer.format(body=spine.format(keys=keys))) == 10_000, keys
    # A call over the column is not a constant, whatever it is called.
    for keys in ("x, abs(x)", "x, x + 1", "x, cast(x AS varchar)"):
        assert fanout(outer.format(body=spine.format(keys=keys))) == 1, keys
    # A constant alone partitions everything into one group, which is a real
    # collapse, and an expression over a column still merges rows.
    assert fanout(outer.format(body=spine.format(keys="TRUE"))) == 1
    assert fanout(outer.format(body=spine.format(keys="x % 7"))) == 1
    assert fanout(outer.format(body=spine.format(keys="x % 7, TRUE"))) == 1


def test_an_ordinal_that_points_at_itself_terminates() -> None:
    # Positional keys resolve through the projection they name, and two
    # projections that name each other's position are a cycle: it exhausted
    # the stack, and the RecursionError was swallowed into "no multiplier".
    outer = "SELECT o.orderkey, g.v FROM tpch.sf1.orders o CROSS JOIN ({body}) g"
    cyclic = (
        "SELECT x AS v, 3 AS a, 2 AS c FROM UNNEST(sequence(1, 10000)) AS t(x) "
        "GROUP BY 1, 2, 3"
    )
    # Keys 2 and 3 name each other, so they name no column: a reduction the
    # SQL cannot size, charged rather than followed.
    assert fanout(outer.format(body=cyclic)) == 1
    assert not unpriceable(outer.format(body=cyclic))
    self_pointing = (
        "SELECT 1 AS v FROM UNNEST(sequence(1, 10000)) AS t(x) GROUP BY 1"
    )
    assert fanout(outer.format(body=self_pointing)) == 1


def test_a_key_that_varies_per_row_is_not_a_constant() -> None:
    # "Reads no column" was standing in for "has one value per row", and
    # rand() and uuid() are the two expressions in SQL furthest from
    # constant. Dropped as if they added no groups, they forged an identity
    # out of a key list that was a genuine reduction: measured on Trino,
    # 1000x1000 spines grouped by t.x alone yield 1,000 groups and by
    # t.x, uuid() a million.
    outer = "SELECT o.orderkey, g.v FROM tpch.sf1.orders o CROSS JOIN ({body}) g"
    two = (
        "SELECT t.x AS v FROM UNNEST(sequence(1, 3000)) AS t(x) "
        "CROSS JOIN UNNEST(sequence(1, 3000)) AS s(y) GROUP BY {keys}"
    )
    for keys in ("t.x, s.y", "t.x, uuid()", "t.x, rand()", "t.x, coalesce(rand(), 0)"):
        assert fanout(outer.format(body=two.format(keys=keys))) == 9_000_000, keys
    # A key that really is one value per row still adds no groups.
    assert fanout(outer.format(body=two.format(keys="t.x, s.y, TRUE"))) == 9_000_000
    assert fanout(outer.format(body=two.format(keys="t.x, TRUE"))) == 1


def test_a_key_the_gate_cannot_read_keeps_the_multiplier() -> None:
    # An unreadable key was answering "not an identity", which DROPS the
    # multiplier — the under-quote direction. What the gate cannot read must
    # not be able to discount a query: one extra token took a 10,000,000x
    # multiplier to 1.
    outer = "SELECT o.orderkey, g.v FROM tpch.sf1.orders o CROSS JOIN ({body}) g"
    spine = "SELECT x AS v FROM UNNEST(sequence(1, 10000)) AS t(x) GROUP BY {keys}"
    for keys in ("x", "x, TRUE", "x, (SELECT 1)"):
        assert fanout(outer.format(body=spine.format(keys=keys))) == 10_000, keys
    # A key that genuinely merges rows still collapses.
    assert fanout(outer.format(body=spine.format(keys="x % 7"))) == 1


def test_a_statement_of_many_generators_stays_under_a_second() -> None:
    # Deciding where each generator's rows land walks the branch again per
    # generator, and that walk was the one the budget never charged: the
    # cost is quadratic in generator count, so 49 KB of SQL took 5.7s before
    # Trino was contacted.
    import time

    generators = ",".join(
        f"UNNEST(sequence(1, 1)) AS t{i}(x{i})" for i in range(1200)
    )
    keys = ",".join(f"x{i}" for i in range(1200))
    wide = (
        "SELECT 1 FROM tpch.sf1.orders o, "
        f"(SELECT 1 AS v FROM {generators} GROUP BY {keys}) g"
    )

    start = time.perf_counter()
    unpriceable(wide)
    fanout(wide)
    assert time.perf_counter() - start < 1.0


def test_a_correlated_subquery_key_is_not_one_valued() -> None:
    # A scalar subquery answers once for the statement — unless it reads the
    # row, which is what makes it correlated. Treating every subquery as
    # one-valued dropped it from the key list, so it could neither make nor
    # break an identity; a correlated one reshapes the row's own value and
    # can merge rows, measured on Trino at 700 groups where the columns
    # alone give 10,000. It is charged as the reduction it may be.
    outer = "SELECT o.orderkey, g.v FROM tpch.sf1.orders o CROSS JOIN ({body}) g"
    two = (
        "SELECT t.x AS v FROM UNNEST(sequence(1, 3000)) AS t(x) "
        "CROSS JOIN UNNEST(sequence(1, 3000)) AS s(y) GROUP BY {keys}"
    )
    correlated = "t.x, (SELECT max(z) FROM UNNEST(ARRAY[s.y % 7]) AS q(z))"
    assert fanout(outer.format(body=two.format(keys=correlated))) == 1
    # An uncorrelated one really is the same value for every row, so it is
    # dropped and the keys that remain decide: here a subset, which reduces.
    assert fanout(outer.format(body=two.format(keys="t.x, (SELECT 1)"))) == 1
    # And on a lone spine the same uncorrelated key leaves the identity.
    lone = "SELECT x AS v FROM UNNEST(sequence(1, 10000)) AS t(x) GROUP BY {keys}"
    assert fanout(outer.format(body=lone.format(keys="x, (SELECT 1)"))) == 10_000
