"""Spot queries whose row work no plan estimate can see.

The budget prices a query from Trino's own plan: the widest row count any
operator would build (see adapters/trino/plan.py). That covers every shape
where rows multiply through joins — products, low-cardinality keys,
correlated nested loops — because the planner estimates all of them.

One shape it cannot see is a row generator. UNNEST(sequence(1, 10000))
manufactures rows from an argument rather than from a table, and
contributes no operator the planner sizes: measured on Trino 476, such a
query over a 60,175-row table plans as 60,175 rows and produces 601
million. That is what this module detects, and all it detects.

table_scan_counts() serves a different gate: the IO plan reports one entry
per table however often the query reads it, so a self-join's *bytes* are
undercounted even though its rows are not.
"""

from collections.abc import Callable, Mapping
from datetime import date

import sqlglot
from sqlglot import exp

_GENERATORS = (exp.Unnest, exp.Explode, exp.Posexplode)

# Interval units with a fixed number of days, so a sequence() over them has a
# length arithmetic can find. MONTH and YEAR vary in length and are excluded.
_FIXED_INTERVAL_DAYS = {"DAY": 1, "WEEK": 7}

# A chain of CTEs each referenced twice doubles table_scan_counts' walk work
# per level: 24 links is a ~1,700-char request and 8M walk steps. Real queries
# measure in the dozens of references at most, so this leaves orders of
# magnitude of headroom while still bounding the walk to milliseconds.
_MAX_SCAN_COUNT = 10_000

# Reads past this many stop being counted exactly. Walking each name once
# reaches numbers a re-walk never survived to produce — a 400-deep doubling
# chain is 2^399 — and a count that large would scale a byte quote through
# arbitrary-precision arithmetic to say what any budget already denies.
_MAX_COUNTED_READS = 1_000_000

# A relation an agent spells out inline is a lookup table, not a multiplier.
# Past this many rows it stops being one: crossing a 1.5M-row scan against
# 20,000 inline values is 30 billion rows the plan prices as one scan.
_MAX_INLINE_ROWS = 1000

# Counting stops here. A product this large is already past anything the row
# budget would clear against a real table, and past what a table-less query
# may invent, so the exact figure beyond it buys nothing.
_MAX_COUNTED_ROWS = 10_000_000

# Aliases resolve through one another; a chain this long is not analytics, and
# following it unbounded exhausts the interpreter's stack on 8 KB of SQL.
_MAX_ALIAS_DEPTH = 32

# Stands for an alias that names more than one relation: not a CTE name, and
# not something a lookup may treat as an unbound (therefore scanned) column.
_AMBIGUOUS = "\x00ambiguous"

# Stands for "this alias reads that relation": one entry per alias instead of
# one per alias and column, so a wide query costs what it reads, not the
# product of its width and its aliases.
_ALIAS_OF = "\x00alias_of"

# Prefix for the by-bare-name index: an unqualified reference reads every
# binding of that name, and finding them by sweeping the keys made each
# lookup cost the whole statement's width.
_BY_NAME = "\x00by_name\x00"

# Steps a standalone resolve may take when no walk lends it one. Callers
# inside the gate share the walk's budget; this only bounds a direct call.
_MAX_RESOLVE_STEPS = 10_000

# Functions that reshape an array without changing how many rows it yields.
# Anything else feeding a generator is assumed to invent rows: a denylist is
# unsound here, because a function sqlglot does not model natively parses as
# Anonymous and would simply be invisible.
_ROW_PRESERVING_FUNCS = {
    "array_sort",
    "array_distinct",
    "reverse",
    "shuffle",
    # Trino's slice() parses as ArraySlice, whose sql_name is array_slice.
    "array_slice",
    "trim_array",
    "filter",
    "transform",
    "cast",
    "try_cast",
    "coalesce",
    "if",
    "nullif",
    # MAP(ARRAY[...], ARRAY[...]) is a lookup table written inline; its rows
    # are its key array's length, which the bounded-input check already caps.
    "map",
    "map_from_entries",
}


def _func_name(func: exp.Func) -> str:
    """The function's source name.

    Anonymous holds it in .name and its sql_name() is the useless literal
    "ANONYMOUS"; every other node holds its name in sql_name() while .name
    returns an *argument's* name — CAST(o.items AS ...) reports "items".
    """
    if isinstance(func, exp.Anonymous):
        return str(func.name or "").lower()
    return str(func.sql_name() or "").lower()


# Arguments that carry no rows of their own: a lambda is applied per element
# (confirmed on Trino 476 — transform(a, x -> sequence(1,10)) nests, it does
# not lengthen), and a datatype, a constant or a condition describes the
# reshaping rather than feeds it.
_NOT_A_ROW_SOURCE = (
    exp.Lambda,
    exp.DataType,
    exp.Literal,
    exp.Boolean,
    exp.Null,
    exp.Predicate,
)


def _func_arguments(func: exp.Func) -> list[exp.Expr]:
    """Every expression a function call feeds on.

    Reading .this and .expressions alone misses whole shapes: IF holds its
    branches under "true"/"false", so an unbounded one would go unchecked.
    Anonymous is the exception — its .this is the function's own name.
    """
    skip = {"this"} if isinstance(func, exp.Anonymous) else set()
    arguments: list[exp.Expr] = []
    for key, value in func.args.items():
        if key in skip:
            continue
        for item in value if isinstance(value, list) else [value]:
            if isinstance(item, exp.Expr):
                arguments.append(item)
    return arguments


def _generates_rows(tree: exp.Expr) -> bool:
    """True if the query invents rows the IO plan cannot see.

    UNNEST over a column expands rows of a table already in the plan; UNNEST
    over sequence(...) or repeat(col, 10000) manufactures them from an
    argument. Anything the gate cannot recognise counts as manufacturing:
    the plan prices scans, and a generator is not one.

    Bounded generators multiply where they meet: three 1000-row sequences
    cross-joined are a billion rows, each within the per-generator cap. The
    cap therefore binds their *product*. A generator inside a CTE multiplies
    once per reference to that CTE, not once per node in the tree.

    A generator joined to a table is not refused here for its size: it is a
    multiplier on rows the plan already counted, and generator_fanout() hands
    that multiplier to the caller so the plan's own estimate can carry it.
    What this refuses is a generator whose size cannot be read at all. Each
    UNION branch is judged on its own — branches add rows rather than multiply
    them, so a statement-wide product would overstate ordinary queries.
    """
    bodies = _cte_bodies(tree)
    # A doubling CTE chain re-walks each body once per reference, so the walk
    # grows exponentially in a query that stays under 1.5 KB: the same budget
    # that bounds the byte gate's walk bounds this one.
    budget = [_MAX_SCAN_COUNT]
    projections = _projected_expressions(tree)
    detached = _without_cte_bodies(tree)
    for branch in _union_branches(detached):
        product = _generator_product(branch, bodies, frozenset(), budget, projections)
        if product is None:
            return True
        # A branch that reads no table is only as large as what it invents,
        # and nothing downstream prices that — so the cap binds here.
        if product > _MAX_INLINE_ROWS and not _reads_a_table(
            branch, bodies, frozenset()
        ):
            return True
    return False


def _union_branches(node: exp.Expr) -> list[exp.Expr]:
    """Each arm rows are ADDED across, or the query itself if there is none.

    Only UNION: an INTERSECT or EXCEPT arm cannot add rows to the result, so
    judging it separately would let each arm carry its own product. Left
    whole, the widest generator in either arm binds the statement — the
    conservative reading.
    """
    if isinstance(node, exp.Union):
        return [*_union_branches(node.this), *_union_branches(node.expression)]
    return [node]


def _arm_projections(arm: exp.Expr) -> list[list[exp.Expr]]:
    """Each row of values this arm puts under the alias list's names.

    A SELECT arm projects one row shape; a VALUES arm projects one per row it
    spells out, and it carries no Select at all — asking every arm for its
    Select skipped VALUES entirely, and the array it spelled out never bound.
    """
    if isinstance(arm, exp.Values):
        return [
            row.expressions if isinstance(row, exp.Tuple) else [row]
            for row in arm.expressions
        ]
    select = arm.find(exp.Select)
    return [select.expressions] if select is not None else []


def _projecting_arms(node: exp.Expr) -> list[exp.Expr]:
    """Every arm of a set operation that can put a value under a name.

    Wider than _union_branches on purpose: this answers "what could this
    name hold", and INTERSECT and EXCEPT arms project just as UNION arms do.
    A parenthesised arm parses as a Subquery, so one pair of brackets hid an
    arm from a walk that only recursed on Union — and the array it built
    read as a scanned column.
    """
    if isinstance(node, exp.SetOperation):
        return [
            *_projecting_arms(node.this),
            *_projecting_arms(node.expression),
        ]
    if isinstance(node, exp.Subquery):
        return _projecting_arms(node.this)
    return [node]


def generator_fanout(sql: str, dialect: str) -> int:
    """How many times a generator multiplies each row the plan counted.

    The plan sizes orders CROSS JOIN UNNEST(sequence(1, 1000)) as the table
    alone — measured on Trino 476, 1,500,000 rows for 1.5 billion produced.
    Returning the multiplier lets the caller scale the plan's own estimate
    instead of the gate guessing what a table it cannot see costs: a spine
    of 744 hours is ordinary against a small table and ruinous against a
    large one, and only the plan knows which this is.

    1 for SQL with no generator, and for one the shape check has already
    refused — a query with no quote needs no multiplier.
    """
    # Anything this cannot read leaves the multiplier at 1, which is only
    # safe because has_unpriceable_shape has already refused the same SQL —
    # it walks the same tree and answers the same exceptions.
    try:
        tree = sqlglot.parse_one(sql, dialect=dialect)
        bodies = _cte_bodies(tree)
        budget = [_MAX_SCAN_COUNT]
        projections = _projected_expressions(tree)
        widest = 1
        for branch in _union_branches(_without_cte_bodies(tree)):
            if not _reads_a_table(branch, bodies, frozenset()):
                continue
            product = _generator_product(
                branch, bodies, frozenset(), budget, projections, for_pricing=True
            )
            if product is None:
                return 1
            widest = max(widest, product)
        return widest
    except (sqlglot.errors.SqlglotError, RecursionError):
        return 1


def _without_unmet_relations(node: exp.Expr) -> exp.Expr:
    """The subtree with everything but its own relations detached.

    A table a generator's rows can multiply is one the FROM or a JOIN puts
    beside them. A table anywhere else — under EXISTS, an IN, a scalar or
    quantified comparison in WHERE, a HAVING, an ORDER BY — answers a
    question about each row instead of pairing with it. Listing the places
    that do not count meant every clause SQL grows is a new hole, so this
    keeps the relation positions and drops the rest.
    """
    copy = node.copy()
    for select in list(copy.find_all(exp.Select)):
        for key, value in list(select.args.items()):
            if key in {"from", "from_", "joins", "with", "with_"}:
                continue
            _detach_relations(value)
    # A JOIN is a relation position, but its ON is a predicate like any
    # other: the same EXISTS the gate refuses in WHERE was admitting an
    # invented spine when written in ON instead.
    for join in list(copy.find_all(exp.Join)):
        for key, value in list(join.args.items()):
            if key == "this":
                continue
            _detach_relations(value)
    return copy


def _detach_relations(value: object) -> None:
    """Remove every table under this argument, whatever shape it holds."""
    for item in value if isinstance(value, list) else [value]:
        if not isinstance(item, exp.Expr):
            continue
        for relation in list(item.find_all(exp.Table)):
            relation.pop()


def _reads_a_table(
    node: exp.Expr,
    bodies: Mapping[str, list[exp.Expr]],
    pending: frozenset[str],
    answered: dict[tuple[int, frozenset[str]], bool] | None = None,
) -> bool:
    """True if this subtree scans a table, following CTE references into it.

    Answers are kept per body and scope: a chain whose every link reads the
    previous one twice re-asked the same question 2^n times, and 1,072
    characters of it cost 90 seconds before any engine was contacted.
    """
    answered = answered if answered is not None else {}
    key = (id(node), pending)
    remembered = answered.get(key)
    if remembered is not None:
        return remembered
    # Recorded before recursing so a cycle answers itself rather than looping.
    answered[key] = False
    for table in node.find_all(exp.Table):
        # A table expression is not always a table: TABLE(sequence(...)) wraps
        # a generator in a node named for the keyword, and reading that as a
        # scan would lend the generator a row count nothing counted.
        if any(table.find_all(exp.GenerateSeries, *_GENERATORS)):
            continue
        parts = [part.name for part in table.parts]
        name = parts[0].lower()
        if len(parts) > 1 or name not in bodies:
            answered[key] = True
            return True
        if name in pending:
            continue
        if any(
            _reads_a_table(body, bodies, pending | {name}, answered)
            for body in bodies[name]
        ):
            answered[key] = True
            return True
    return False


def _generator_product(
    node: exp.Expr,
    bodies: Mapping[str, list[exp.Expr]],
    pending: frozenset[str],
    budget: list[int],
    projections: Mapping[str, list[exp.Expr]],
    for_pricing: bool = False,
) -> int | None:
    """Rows this subtree's generators multiply out to, or None past the cap.

    None also stands for a generator the gate cannot bound at all, and for a
    walk that ran out of budget: all three mean the same thing to the caller
    — no quote — so they share a return.

    `for_pricing` asks for the multiplier a caller will apply to the plan's
    own estimate, so a generator whose rows collapse before they meet the
    branch contributes nothing. The refusal path leaves it False: whether a
    generator can be *sized* is a question about the generator, not about
    where it sits, and skipping one there would let an unbounded spine hide
    inside an aggregate.
    """
    if budget[0] <= 0:
        return None
    # Whether the plan will carry this branch's rows decides how large a
    # spelled-out spine may be: crossed with a table it is a multiplier the
    # budget applies to a real cardinality, and alone it is all there is.
    # It has to be a table the generator actually MEETS — one inside EXISTS,
    # an IN predicate or an uncorrelated scalar subquery carries nothing, and
    # counting it let a 10,000,000-row spine through where 1000 is the limit.
    priced = _reads_a_table(_without_unmet_relations(node), bodies, pending)
    # One answer per select for "does this scope narrow" and "does it scan":
    # both are about the select alone, and asking per generator made the walk
    # quadratic in generator count.
    landing: dict[tuple[str, int], bool] = {}
    product = 1
    wrapped: set[int] = set()
    loose: list[exp.GenerateSeries] = []
    references: list[str] = []
    # One walk, charged per node: a body re-read once per reference costs its
    # own size every time, and only counting the reads left that size — which
    # the attacker writes — outside the budget entirely.
    for child in node.walk():
        budget[0] -= 1
        if budget[0] <= 0:
            return None
        if isinstance(child, _GENERATORS):
            # Resolving a generator's input shares the walk's budget: the
            # depth cap bounds a single chain of names, not how many chains a
            # wide statement writes, and the resolvers were the one path
            # nothing charged. 71 KB of CTEs and unnests cost a minute of CPU
            # before Trino was contacted.
            if not _expands_a_bounded_value(
                child, projections, budget=budget, priced_by_a_table=priced
            ):
                return None
            if for_pricing and not _multiplies_its_branch(child, node, landing):
                continue
            product *= _generator_rows(child, projections, budget=budget)
            if product > _MAX_COUNTED_ROWS:
                return None
            wrapped.update(id(series) for series in child.find_all(exp.GenerateSeries))
        elif isinstance(child, exp.GenerateSeries):
            loose.append(child)
        elif isinstance(child, exp.Table):
            parts = [part.name for part in child.parts]
            name = parts[0].lower()
            if len(parts) == 1 and name in bodies and name not in pending:
                references.append(name)
    # A series in a table position manufactures rows without an UNNEST to
    # wrap it; guarding only the wrapper would leave the same sequence free.
    for series in loose:
        if id(series) in wrapped:
            continue
        if for_pricing and not _multiplies_its_branch(series, node, landing):
            continue
        length = _sequence_length(series)
        if length is None or length > _MAX_COUNTED_ROWS:
            return None
        product *= length
        if product > _MAX_COUNTED_ROWS:
            return None
    # The same name read twice reads the same bodies, so its cost is walked
    # once and applied per reference: re-walking spent the budget on the
    # body's WIDTH, which is the analyst's to write, and refused an ordinary
    # 400-column CTE read eight times before it reached any generator.
    per_name: dict[str, int] = {}
    for name in references:
        if name not in per_name:
            # A name bound more than once costs whatever its dearest binding
            # costs: scope decides which one a reference reads, and charging
            # the cheapest is what let a decoy body hide a generator.
            widest = 1
            for body in bodies[name]:
                nested = _generator_product(
                    body, bodies, pending | {name}, budget, projections, for_pricing
                )
                if nested is None:
                    return None
                # Rows leave a CTE through its own select, so an aggregate
                # there collapses them before any reference can multiply by
                # them — the sizing question stays open, only the multiplier.
                # Only where the body builds nothing of its own: an aggregate
                # over a generator already crossed with a table runs AFTER
                # those rows exist, and excusing it quoted 6 billion rows as
                # six million.
                if (
                    for_pricing
                    and _collapses_on_the_way_out(body)
                    and not _reads_a_table(body, bodies, pending | {name})
                ):
                    nested = 1
                widest = max(widest, nested)
            per_name[name] = widest
        product *= per_name[name]
        if product > _MAX_COUNTED_ROWS:
            return None
    return product


def _remembered(
    question: str,
    select: exp.Select,
    answer: "Callable[[exp.Select], bool]",
    answered: dict[tuple[str, int], bool] | None,
) -> bool:
    """One answer per select, not one per generator that asks.

    Both questions are about the select alone, but they were re-asked once
    per generator and each walks the whole subtree: the cost was quadratic
    in generator count, and 49 KB of SQL took 5.7 seconds.
    """
    if answered is None:
        return answer(select)
    key = (question, id(select))
    if key not in answered:
        answered[key] = answer(select)
    return answered[key]


def _scans_a_table(select: exp.Select) -> bool:
    """True if this select's own FROM or JOIN reads a table, generators aside.

    A table named anywhere else — in a scalar subquery among the group keys,
    in a WHERE predicate — brings no rows this select's rows pair with, and
    counting it said the scope builds relations of its own when it does not.
    """
    for table in _without_unmet_relations(select).find_all(exp.Table):
        if any(table.find_all(exp.GenerateSeries, *_GENERATORS)):
            continue
        return True
    return False


def _narrows_its_rows(select: exp.Select) -> bool:
    """True if fewer rows leave this select than its FROM produced.

    The question is cardinality, not syntax. A bare aggregate yields exactly
    one row. A GROUP BY yields one row per distinct key — a reduction only if
    the key is something other than what the generator itself enumerates:
    GROUP BY over a distinct spine is the identity, and reading it as a
    collapse priced 60 billion rows as six million. A window function is an
    aggregate node that adds a column and removes no row at all.
    """
    grouped = select.args.get("group")
    if grouped is not None:
        # ROLLUP, CUBE, GROUPING SETS and WITH TOTALS emit subtotal rows on
        # top of the groups, so they are the grouping shapes that can ADD
        # rows. They also keep their keys in their own args, leaving
        # group.expressions empty — which the identity test below would have
        # read as "no keys" and called a reduction.
        if any(
            grouped.args.get(shape)
            for shape in ("rollup", "cube", "grouping_sets", "totals")
        ):
            return False
        # GROUP BY ALL groups by every non-aggregate projection and leaves
        # the key list empty too. Over a lone spine that is the identity, so
        # it is read as the keys it stands for rather than as none.
        if grouped.args.get("all") and not grouped.expressions:
            return not _groups_by_what_the_generators_make(
                select, grouped, keys=_non_aggregate_projections(select)
            )
        # Whether a GROUP BY reduces anything is a cardinality question, and
        # ADR 0004 keeps those with the plan. The one case SQL settles on its
        # own is a key list that names exactly the columns the generators in
        # this select produce: one row per row they made, the identity, which
        # read as a collapse priced 60 billion rows as six million. Anything
        # else — a key from a joined relation, an expression, a subset — is
        # charged as a reduction, the fail-safe direction for a multiplier.
        return not _groups_by_what_the_generators_make(select, grouped)
    # A window function carries an OVER clause; only a plain aggregate with
    # no grouping reduces the select to a single row.
    return any(
        aggregate.find_ancestor(exp.Window) is None
        for aggregate in select.find_all(exp.AggFunc, bfs=False)
        if _is_in_this_select(aggregate, select)
    )


def _non_aggregate_projections(select: exp.Select) -> list[exp.Expr]:
    """What GROUP BY ALL stands for: every projection that is not an
    aggregate, unwrapped from any alias."""
    standing: list[exp.Expr] = []
    for projection in select.expressions:
        value = projection.this if isinstance(projection, exp.Alias) else projection
        if any(
            _is_in_this_select(aggregate, select)
            for aggregate in value.find_all(exp.AggFunc)
        ):
            continue
        standing.append(value)
    return standing


# Calls that answer the same for every row of a query. Anything absent is
# assumed to vary — rand() and uuid() read no column and differ per row, and
# an allowlist is the only sound direction when the failure mode is a
# multiplier silently dropped.
_ONE_VALUE_PER_QUERY = {
    "current_date",
    "current_time",
    "current_timestamp",
    "currenttimestamp",
    "currentdate",
    "currenttime",
    "now",
    "localtime",
    "localtimestamp",
    "abs",
    "ceil",
    "floor",
    "round",
    "length",
    "upper",
    "lower",
    "concat",
    "typeof",
    "cast",
    "try_cast",
    "coalesce",
    "nullif",
    "if",
    "array",
    "struct",
    "map",
    "interval",
}

# What a group key contributes to the partition: a column it names, the
# nothing a constant adds, a split of some column this cannot follow, or a
# value that varies without reading a column at all.
_NAMES_NO_COLUMN = "unreadable"
_PARTITIONS_NOTHING = "constant"
_ONLY_SPLITS_FURTHER = "row-varying"


def _every_column_is_inside_a_subquery(key: exp.Expr) -> bool:
    """True if this key reads the row only through subqueries of its own.

    Such a key adds the row's value to the partition rather than replacing a
    column with a reshaping of it, so it can only split the partition the
    other keys make. A column read directly — x % 7 — is the reshaping.
    """
    if key.find(exp.Subquery) is None:
        return False
    return all(
        column.find_ancestor(exp.Subquery) is not None
        for column in key.find_all(exp.Column, exp.Star)
    )


def _group_key_column(
    key: exp.Expr, select: exp.Select, seen: frozenset[int] = frozenset()
) -> exp.Column | str:
    """What a group key contributes: the column it names, or why it names none.

    A key is read by what it partitions on, not by how it is spelled:
    parentheses wrap the same column, and a positional ordinal points at a
    projection which may itself be one. An expression over a column names no
    column — grouping by it can merge rows. A constant names none either, but
    it also partitions nothing, so it neither makes an identity nor breaks
    one: GROUP BY x and GROUP BY x, TRUE are the same partition, and reading
    the second as a reduction dropped a 10,000,000x multiplier.
    """
    while isinstance(key, exp.Paren):
        key = key.this
    if isinstance(key, exp.Column):
        return key
    if isinstance(key, exp.Literal) and not key.is_string:
        try:
            position = int(key.name)
        except ValueError:
            return _NAMES_NO_COLUMN
        if not 1 <= position <= len(select.expressions):
            return _NAMES_NO_COLUMN
        # Two projections can name each other's position; following that
        # exhausted the stack, and the error was swallowed as "no multiplier".
        if position in seen:
            return _NAMES_NO_COLUMN
        projected = select.expressions[position - 1]
        if isinstance(projected, exp.Alias):
            projected = projected.this
        return _group_key_column(projected, select, seen | {position})
    if _partitions_nothing(key):
        return _PARTITIONS_NOTHING
    # A key that reads no column cannot merge rows the columns separated —
    # it can only cut them finer, so it neither makes nor breaks an identity.
    # One that does read a column may reshape it (x % 7 merges seven rows
    # into one), and that this cannot follow.
    if key.find(exp.Column, exp.Star) is None:
        return _ONLY_SPLITS_FURTHER
    # A correlated subquery adds the row's own value to the partition rather
    # than replacing a key with a reshaping of it: measured on Trino, two
    # 100x100 spines grouped by `t.x, (SELECT s.y)` give 10,000 groups where
    # `t.x` alone gives 100. It can also merge — `(SELECT s.y % 7)` gives 700
    # — but never below what the other keys already separate, so it cannot
    # take the partition under one group per row the generators produced.
    if _every_column_is_inside_a_subquery(key):
        return _ONLY_SPLITS_FURTHER
    return _NAMES_NO_COLUMN


def _reads_an_outer_column(subquery: exp.Subquery) -> bool:
    """True if this subquery names a column none of its own relations supply.

    Read by qualifier: a correlated reference is one qualified by a relation
    the subquery does not itself introduce, or left bare where the subquery
    has no relation of its own to have meant.
    """
    relations: set[str] = set()
    columns: set[str] = set()
    # A derived table and a VALUES list are relations the subquery owns just
    # as a table is; counting only tables and generators left the supplied
    # set empty, and every name then read as reaching outside.
    for relation in subquery.find_all(
        exp.Table, exp.Unnest, exp.Subquery, exp.Values, exp.Lateral
    ):
        if relation is subquery:
            continue
        alias = relation.args.get("alias")
        name = getattr(alias, "name", "") or getattr(relation, "alias_or_name", "")
        if name:
            relations.add(name.lower())
        for column in getattr(alias, "columns", []) or []:
            columns.add(column.name.lower())
        ordinality = relation.args.get("offset")
        if isinstance(ordinality, exp.Identifier):
            columns.add(ordinality.name.lower())
        # A derived relation names its columns by what it projects.
        projected = relation.find(exp.Select) if relation is not subquery else None
        for projection in getattr(projected, "expressions", []) or []:
            name = projection.alias_or_name
            if name:
                columns.add(name.lower())
    for column in subquery.find_all(exp.Column):
        qualifier = str(column.table or "").lower()
        if qualifier:
            if qualifier not in relations:
                return True
            continue
        # Unqualified, it belongs to whichever of the subquery's own
        # relations names it. Reading "the subquery has some relation" as
        # "the name must be its own" let a reference to the outer row pass
        # as uncorrelated whenever any relation was in scope.
        if column.name.lower() not in columns:
            return True
    return False


def _partitions_nothing(key: exp.Expr) -> bool:
    """True if this key provably has one value for every row.

    "Reads no column" is not that property: rand() and uuid() read nothing
    and differ on every row, and dropping them as constants forged an
    identity out of a key list that really did reduce. So this is an
    allowlist of the shapes that cannot vary — literals and arithmetic over
    them, and the deterministic calls that take no argument at all. A
    function not named here varies until something proves otherwise, which
    keeps the multiplier rather than discounting it.
    """
    if isinstance(key, exp.Literal | exp.Boolean | exp.Null):
        return True
    # The empty grouping: every row falls in one group.
    if isinstance(key, exp.Tuple) and not key.expressions:
        return True
    # A scalar subquery answers once for the statement — unless it reads the
    # row, which is what makes it correlated. Its own FROM supplies some of
    # the columns it names; a name no relation inside it supplies comes from
    # the row outside, and then it varies per row like any column.
    if isinstance(key, exp.Subquery):
        return not _reads_an_outer_column(key)
    
    if isinstance(key, exp.Paren | exp.Neg | exp.Cast | exp.TryCast):
        return _partitions_nothing(key.this)
    if isinstance(key, exp.Binary):
        return _partitions_nothing(key.this) and _partitions_nothing(key.expression)
    if isinstance(key, exp.Func):
        if _func_name(key) not in _ONE_VALUE_PER_QUERY:
            return False
        return all(
            _partitions_nothing(argument)
            for argument in _func_arguments(key)
            if not isinstance(argument, exp.DataType)
        )
    return False


def _groups_by_what_the_generators_make(
    select: exp.Select,
    grouped: exp.Group,
    keys: list[exp.Expr] | None = None,
) -> bool:
    """True if the group keys are exactly the columns this select's
    generators enumerate, so grouping returns one row per row they made."""
    stated: list[exp.Expr] = grouped.expressions if keys is None else keys
    # Read what each key NAMES, not how it is written: GROUP BY 1 and
    # GROUP BY (x) partition exactly as GROUP BY x does, and requiring a
    # bare Column node excused both.
    resolved = [_group_key_column(key, select) for key in stated]
    # A key over a column may reshape it — x % 7 merges seven rows into one —
    # and that this cannot follow, so it stays a reduction.
    if any(key is _NAMES_NO_COLUMN for key in resolved):
        return False
    # A constant is dropped rather than counted: it adds no groups, so it
    # neither makes the partition an identity nor stops it being one. A key
    # that only splits is dropped for the same reason — but it can only split
    # something, so it decides nothing on its own: GROUP BY (SELECT 1 WHERE
    # x > 0) alone is one group, not one per row.
    columns = [key for key in resolved if isinstance(key, exp.Column)]
    if not columns:
        return False
    splitting = any(key is _ONLY_SPLITS_FURTHER for key in resolved)
    # Keyed by the relation that supplies each column, not by name alone: a
    # qualifier is what distinguishes one relation's "x" from another's, and
    # dropping it let a key that names a different relation read as the
    # spine's own.
    produced: set[tuple[str, str]] = set()
    counters: set[tuple[str, str]] = set()
    relations = 0
    generators = 0
    for source in select.find_all(exp.Table, exp.Unnest, bfs=False):
        relations += 1
        if not isinstance(source, exp.Unnest):
            continue
        generators += 1
        alias = source.args.get("alias")
        source_name = getattr(alias, "name", "").lower()
        for column in getattr(alias, "columns", []) or []:
            produced.add((source_name, column.name.lower()))
        # WITH ORDINALITY numbers the rows it produces, so the counter and
        # the value each identify a row on their own: grouping by either is
        # still one group per row. It is recorded as an alternative to the
        # value columns rather than as another required key.
        ordinality = source.args.get("offset")
        if isinstance(ordinality, exp.Identifier):
            counters.add((source_name, ordinality.name.lower()))
    # Every relation in the FROM must be one of those generators: a joined
    # table brings rows the keys do not identify, and how many is the plan's
    # answer, not the SQL's.
    if relations != generators or not produced:
        return False
    # An unqualified key names whichever generator supplies it, which is
    # unambiguous exactly when one does; a name two of them share says
    # nothing, and a qualified key must match the relation it names.
    by_name: dict[str, list[str]] = {}
    for source_name, column in produced | counters:
        by_name.setdefault(column, []).append(source_name)
    named: set[tuple[str, str]] = set()
    for key in columns:
        column = key.name.lower()
        qualifier = str(key.table or "").lower()
        if not qualifier:
            suppliers = by_name.get(column, [])
            if len(suppliers) != 1:
                return False
            qualifier = suppliers[0]
        named.add((qualifier, column))
    if named == produced:
        return True
    # A splitting key alongside a subset restores the rest: the columns say
    # which rows are already separate, and a per-row value separates the
    # remainder. It cannot merge below them, so the partition is at least
    # one group per row the generators produced.
    if splitting and named <= produced:
        return True
    # A counter stands in for the value columns of the generator that made
    # it: one relation numbered per row means grouping by the number is the
    # same partition as grouping by the row.
    substituted = set(named)
    for source_name, column in counters:
        if (source_name, column) in substituted:
            substituted.discard((source_name, column))
            substituted |= {pair for pair in produced if pair[0] == source_name}
    return substituted == produced


def _is_in_this_select(node: exp.Expr, select: exp.Select) -> bool:
    """True if the nearest enclosing select is this one, not a nested query."""
    return node.find_ancestor(exp.Select) is select


def _collapses_on_the_way_out(body: exp.Expr) -> bool:
    """True if this scope's select narrows its rows before they leave it."""
    select = body if isinstance(body, exp.Select) else body.find(exp.Select)
    if select is None:
        return False
    return _narrows_its_rows(select)


def _multiplies_its_branch(
    generator: exp.Expr,
    root: exp.Expr,
    answered: dict[tuple[str, int], bool] | None = None,
) -> bool:
    """True if this generator's rows meet the branch's rows and multiply them.

    A generator in a FROM or a JOIN crosses whatever else the branch reads.
    Two shapes do not: rows an aggregate collapses on the way out (a spine
    counted into one number, then joined), and a predicate subquery, where
    EXISTS and IN ask whether a match exists rather than pairing with each
    one. Charging those anyway priced an ordinary 365-day report at 365x its
    real work, which the row budget then denied — over-quoting refuses a
    query as surely as under-quoting admits one.
    """
    # The branch's own select is where the widest step is built: an aggregate
    # there runs after the generator's rows already exist, so it spares the
    # engine nothing. Only a scope the rows must leave — a derived table, a
    # CTE, a subquery — can collapse them before they reach the branch.
    branch_select = root if isinstance(root, exp.Select) else root.find(exp.Select)
    node: exp.Expr | None = generator
    while node is not None and node is not root:
        parent = node.parent
        if isinstance(parent, exp.Select):
            # A subquery in the SELECT list is scalar: it contributes a value
            # to each row rather than rows of its own.
            if node.arg_key == "expressions":
                return False
            # A narrowing scope spares the work only where it builds nothing
            # of its own: one over a generator already crossed with a table
            # runs after those rows exist, and excusing it quoted 6 billion
            # rows as six million.
            if (
                parent is not branch_select
                and _remembered("narrows", parent, _narrows_its_rows, answered)
                and not _remembered("scans", parent, _scans_a_table, answered)
            ):
                return False
        # EXISTS and IN test for a match; neither pairs the outer row with
        # every row the subquery could produce.
        if isinstance(parent, exp.Exists | exp.In):
            return False
        node = parent
    return True


def _cte_bodies(tree: exp.Expr) -> dict[str, list[exp.Expr]]:
    """Every body each CTE name is bound to, in definition order.

    A nested WITH may re-bind a name the outer query also uses, and which
    body a given reference means is scope's answer, not a dict's. Keying
    flatly let the inner body silently replace the outer one — a decoy that
    hid a generator behind a name still referenced outside it. Keeping every
    binding lets a reader charge for the most expensive one instead of
    guessing, which neither under-counts the decoy shape nor refuses the
    ordinary query that reuses a name like "base" in a nested scope.
    """
    bodies: dict[str, list[exp.Expr]] = {}
    for cte in tree.find_all(exp.CTE):
        bodies.setdefault(cte.alias_or_name.lower(), []).append(cte.this)
    return bodies


def _expands_a_bounded_value(
    generator: exp.Expr,
    projections: Mapping[str, list[exp.Expr]] | None = None,
    seen: frozenset[str] = frozenset(),
    budget: list[int] | None = None,
    priced_by_a_table: bool = False,
) -> bool:
    """True if the generator's input is bounded by something already priced.

    Two bounded shapes: a literal the agent spelled out (UNNEST(ARRAY['O','F'])
    is a two-row lookup table), and a column of a table the IO plan already
    counted, reshaped by functions that cannot change its length.
    """
    inputs = [
        value
        for value in (generator.this, *(generator.expressions or []))
        if value is not None
    ]
    if not inputs:
        return False
    return all(
        _is_bounded_input(value, projections, seen, budget, priced_by_a_table)
        for value in inputs
    )


def _projected_expressions(tree: exp.Expr) -> dict[str, list[exp.Expr]]:
    """Every expression a subquery or CTE binds a name to, keyed by source.

    A generator fed by a column is priced as the table's own rows, which is
    right for a scanned column and wrong for one a projection built:
    repeat(k, 1000000) AS arr is a column by the time UNNEST reads it.

    Keys are "source.column" where the source is the CTE or derived-table
    alias the name is visible under, plus a bare "column" fallback for an
    unqualified reference. Keying on the column name alone made an alias
    poison every same-named column in the statement — UNNEST(o.items) is a
    scanned column of o whatever some unrelated CTE calls its aggregate.

    Both spellings of a binding count: SELECT expr AS name, and the column
    alias list on a CTE or derived table, which names the projections
    positionally and carries no Alias node at all.
    """
    projections: dict[str, list[exp.Expr]] = {}
    bound_columns: dict[str, dict[str, list[exp.Expr]]] = {}
    stars: list[tuple[str, exp.Select]] = []

    def bind(source: str | None, column: str, value: exp.Expr) -> None:
        key = f"{source.lower()}.{column.lower()}" if source else column.lower()
        projections.setdefault(key, []).append(value)
        # The same binding under a bare-name index, so an unqualified
        # reference finds it by lookup rather than by sweeping every key.
        projections.setdefault(f"{_BY_NAME}{column.lower()}", []).append(value)
        if source:
            bound_columns.setdefault(source.lower(), {}).setdefault(
                column.lower(), []
            ).append(value)

    def bind_alias_list(alias: exp.TableAlias | None, body: exp.Expr) -> None:
        if alias is None or not alias.columns:
            return
        source = alias.name
        # Every arm binds the name, not just the first one found: a set
        # operation carries whatever any arm projects, and reading only the
        # leading Select let a later arm's manufactured array pass as a
        # scanned column.
        for branch in _projecting_arms(body):
            for projected in _arm_projections(branch):
                for column, projection in zip(
                    alias.columns, projected, strict=False
                ):
                    bind(source, column.name, projection)

    for cte in tree.find_all(exp.CTE):
        bind_alias_list(cte.args.get("alias"), cte.this)
    for subquery in tree.find_all(exp.Subquery):
        bind_alias_list(subquery.args.get("alias"), subquery.this)
    # VALUES holds its own alias and column list, and its row is a projection
    # like any other: (VALUES (repeat(1, 10000))) AS v(arr) builds an array.
    for values in tree.find_all(exp.Values):
        alias = values.args.get("alias")
        if alias is None or not alias.columns:
            continue
        for row in values.expressions:
            items = row.expressions if isinstance(row, exp.Tuple) else [row]
            for column, item in zip(alias.columns, items, strict=False):
                bind(alias.name, column.name, item)

    for select in tree.find_all(exp.Select):
        source = _select_source_name(select)
        for projection in select.expressions:
            if isinstance(projection, exp.Alias):
                bind(source, projection.alias, projection.this)
                bind(None, projection.alias, projection.this)
            elif isinstance(projection, exp.Column) and source:
                # q.* parses as a Column whose name is the star, not a Star
                # node, so a qualified star re-exports like a bare one.
                if projection.name == "*":
                    stars.append((source, select))
                # SELECT arr FROM s re-exports the name under a new source,
                # so the outer scope can follow it back to what built it.
                # Binding it under its own source would only point at itself.
                elif projection.table.lower() != source.lower():
                    bind(source, projection.name, projection)
            elif isinstance(projection, exp.Star) and source:
                stars.append((source, select))
            elif source and _positional_name(select, projection):
                # A UNION arm names its columns by position from the first
                # arm; an unnamed projection in a later one still reaches the
                # outer scope under that name.
                bind(source, _positional_name(select, projection) or "", projection)
    # A star re-exports every name its own scope can see, so what an inner
    # projection built passes outward unnamed. Resolving each one to the
    # names below it keeps a manufactured array from shedding its history
    # by being selected with *. A star whose FROM is a CTE name sees nothing
    # in its own subtree — the bindings live in that CTE's body under the
    # WITH — so the reference is followed there too.
    cte_bodies = {
        cte.alias_or_name.lower(): cte.this for cte in tree.find_all(exp.CTE)
    }

    # A body reached twice binds the same names both times, so visiting it
    # once per source is enough: without this a diamond of star CTEs walks
    # every path, and 1 KB of SQL costs seconds before the budgeted walk.
    visited: set[tuple[str, int]] = set()

    def bind_star(source: str, select: exp.Select) -> None:
        if (source, id(select)) in visited:
            return
        visited.add((source, id(select)))
        for inner in select.find_all(exp.Alias):
            bind(source, inner.alias, inner.this)
        for table in select.find_all(exp.Table):
            parts = [part.name for part in table.parts]
            name = parts[0].lower()
            if len(parts) > 1 or name not in cte_bodies:
                continue
            body = cte_bodies[name].find(exp.Select)
            if body is not None:
                bind_star(source, body)

    for source, select in stars:
        bind_star(source, select)

    # FROM a AS z asks for z.arr while the binding sits under the CTE's own
    # name. Only aliases that name exactly one CTE resolve: an alias used for
    # two relations, or one that shadows a real table, means more than a name
    # can say, and guessing merged unrelated scans with an aggregate's array.
    alias_sources = _alias_sources(tree)
    for alias, name in alias_sources.items():
        if name is _AMBIGUOUS:
            # A marker key: any column read through this alias resolves to
            # something the gate cannot read, which fails closed.
            projections[f"{alias}.{_AMBIGUOUS}"] = []
            continue
        # Recorded as an indirection rather than copied: writing every bound
        # column under every alias that names its relation is the alias x
        # column cross-product, which 3,000 of each turned into nine million
        # entries — seconds of CPU and gigabytes of RSS on SQL well inside the
        # length cap, spent before the walk budget is charged at all.
        if name in bound_columns:
            projections[f"{alias}.{_ALIAS_OF}"] = [exp.Identifier(this=name)]
    return projections


def _alias_sources(tree: exp.Expr) -> dict[str, str]:
    """Which CTE each table alias reads, for aliases that name exactly one.

    FROM a AS z asks for z.arr while the binding was recorded under the CTE's
    own name, so the alias needs resolving. Copying the bindings themselves
    was both quadratic in query width and blind to scope — it merged every
    relation sharing an alias letter. An alias used for two different CTEs
    resolves to neither: which one a reference means is more than a name can
    say, and guessing is what condemned unrelated scans.
    """
    cte_names = {cte.alias_or_name.lower() for cte in tree.find_all(exp.CTE)}
    # What each alias names, whether a CTE or a table: an alias that names two
    # relations is ambiguous whatever kind they are, while one that names a
    # single table is ordinary SQL and resolves to no CTE at all.
    named: dict[str, set[str]] = {}
    sources: dict[str, str] = {}
    for table in tree.find_all(exp.Table):
        parts = [part.name for part in table.parts]
        name = parts[0].lower()
        alias = table.alias.lower()
        if not alias or alias == name:
            continue
        named.setdefault(alias, set()).add(".".join(parts).lower())
        if len(parts) == 1 and name in cte_names:
            sources[alias] = name
    ambiguous = {alias for alias, relations in named.items() if len(relations) > 1}
    resolved = {
        alias: name for alias, name in sources.items() if alias not in ambiguous
    }
    # An alias naming more than one relation resolves to nothing readable.
    # It is recorded so a reference through it can be refused: an alias the
    # gate cannot follow is a shape it cannot size, and spelling that
    # ambiguity on purpose is how a manufactured array hid behind it.
    for alias in ambiguous:
        resolved.setdefault(alias, _AMBIGUOUS)
    return resolved


def _positional_name(select: exp.Select, projection: exp.Expr) -> str | None:
    """The name a set operation gives this projection, taken from the first
    arm, which is where SQL fixes the output column names."""
    union = select.parent
    if not isinstance(union, exp.Union):
        return None
    first = union.this.find(exp.Select) if union.this else None
    if first is None or first is select:
        return None
    try:
        position = select.expressions.index(projection)
    except ValueError:
        return None
    if position >= len(first.expressions):
        return None
    named = first.expressions[position]
    return named.alias_or_name or None


def _column_key(column: exp.Column) -> str:
    """How this reference names itself: qualified where the SQL qualifies it."""
    source = column.table
    name = column.name.lower()
    return f"{source.lower()}.{name}" if source else name


def _column_bindings(
    column: exp.Column,
    projections: Mapping[str, list[exp.Expr]],
) -> list[exp.Expr]:
    """What a projection bound this reference to, if anything did.

    A qualified reference reads only bindings visible under that source, so
    an unrelated CTE binding the same name cannot speak for it. An
    unqualified one has no source to check and falls back to the bare name.
    """
    key = _column_key(column)
    bindings = projections.get(key)
    if bindings is not None:
        return bindings
    if column.table:
        # FROM cte AS z asks for z.arr while the binding sits under the CTE's
        # own name; the alias resolves through its recorded relation instead
        # of every column having been copied under it.
        source = column.table.lower()
        alias_of = projections.get(f"{source}.{_ALIAS_OF}")
        if alias_of:
            relation = alias_of[0].name.lower()
            return list(projections.get(f"{relation}.{column.name.lower()}", []))
        return []
    # Unqualified, the SQL does not say which source this reads, so every
    # binding of the name answers for it: a projection that only forwards
    # a name (SELECT c FROM a) would otherwise lose the array behind it.
    # Bindings that resolve to this same reference are skipped — a name
    # forwarded from a table is a scanned column, not a manufactured one.
    #
    # The name is looked up in an index built once with the bindings, rather
    # than swept for out of every key: an O(keys) scan per lookup sat inside
    # an unbudgeted recursion, and 37 KB of CTEs and unnests cost seconds of
    # CPU before the engine was ever asked. A name nothing binds indexes to an
    # empty list, so a miss is a lookup too.
    return [
        bound
        for bound in projections.get(f"{_BY_NAME}{column.name.lower()}", [])
        if not (isinstance(bound, exp.Column) and _column_key(bound) == key)
    ]


def _select_source_name(select: exp.Select) -> str | None:
    """The alias a SELECT's rows are visible under, if it has one."""
    parent = select.parent
    while parent is not None:
        if isinstance(parent, exp.Subquery | exp.CTE):
            alias = parent.args.get("alias")
            return alias.name if alias is not None else None
        if isinstance(parent, exp.Select):
            return None
        parent = parent.parent
    return None


def _is_bounded_input(
    value: exp.Expr,
    projections: Mapping[str, list[exp.Expr]] | None = None,
    seen: frozenset[str] = frozenset(),
    budget: list[int] | None = None,
    priced_by_a_table: bool = False,
) -> bool:
    """True if this generator argument yields a length something else fixes.

    The depth cap bounds how far a chain of names is followed; it does not
    bound how MANY there are, and breadth is what a wide statement of CTEs
    and unnests buys cheaply. Running out of budget answers False — an input
    nobody finished reading is not one the gate may vouch for.
    """
    projections = projections if projections is not None else {}
    budget = budget if budget is not None else [_MAX_RESOLVE_STEPS]
    budget[0] -= 1
    if budget[0] <= 0:
        return False
    if isinstance(value, exp.Array | exp.Struct):
        # A literal spells out its own length — but spelling out 20,000 of
        # them multiplies every scanned row by 20,000 just the same.
        if len(value.expressions or []) > _MAX_INLINE_ROWS:
            return False
        # The elements themselves must be literal: ARRAY[sequence(1, 1e9)]
        # spells out one element and yields a billion rows.
        return all(
            isinstance(element, exp.Literal)
            for element in (value.expressions or [])
        )
    if isinstance(value, exp.Column):
        # A name a projection bound is only as bounded as what it was bound
        # to; a name nothing in the statement binds is a scanned column, whose
        # rows the plan already counted.
        key = _column_key(value)
        # An alias naming more than one relation says nothing about which one
        # this reads, and a generator whose input cannot be identified is not
        # one the gate may vouch for.
        if value.table and f"{value.table.lower()}.{_AMBIGUOUS}" in projections:
            return False
        # A chain longer than any real query writes exhausts the stack, so it
        # stops here rather than vouch for the column.
        if len(seen) >= _MAX_ALIAS_DEPTH:
            return False
        bindings = _column_bindings(value, projections)
        if key in seen:
            # A name reached twice through nothing but other names is a
            # column some table supplies: a pipeline of CTEs forwarding an
            # array column revisits names without building anything. A cycle
            # that passes through an expression has a length nobody reads.
            return all(isinstance(bound, exp.Column) for bound in bindings)
        return all(
            _is_bounded_input(
                bound, projections, seen | {key}, budget, priced_by_a_table
            )
            for bound in bindings
        )
    if isinstance(value, exp.GenerateSeries):
        # sequence() spells its own length out when both ends are literal, so
        # a date spine can be priced instead of refused. Anything the length
        # cannot be computed from — a column bound, a calendar step, a span
        # over the cap — stays unpriceable.
        length = _sequence_length(value)
        if length is None:
            return False
        # The flat cap binds only where nothing downstream prices the rows.
        # Joined to a table, the size is a multiplier generator_fanout() hands
        # to the budget, which decides with the table's real cardinality in
        # hand — a seven-year daily spine against a 25-row table is 63,950
        # rows, and refusing it sight-unseen is the flat cap ADR 0006 dropped.
        if priced_by_a_table:
            return length <= _MAX_COUNTED_ROWS
        return length <= _MAX_INLINE_ROWS
    if isinstance(value, exp.Func):
        if _func_name(value) not in _ROW_PRESERVING_FUNCS:
            return False
        arguments = [
            argument
            for argument in _func_arguments(value)
            if not isinstance(argument, _NOT_A_ROW_SOURCE)
        ]
        # A reshaping function with no argument left to check is reshaping a
        # literal, which cannot add rows.
        return all(
            _is_bounded_input(argument, projections, seen, budget, priced_by_a_table)
            for argument in arguments
        )
    return False


def _sequence_length(value: exp.GenerateSeries) -> int | None:
    """How many rows this sequence() yields, or None if that is not knowable.

    Only fixed strides count. A month or year step covers a variable number
    of days, so its length is not arithmetic on the endpoints and the gate
    keeps refusing it.
    """
    start = _literal_point(value.args.get("start"))
    end = _literal_point(value.args.get("end"))
    if start is None or end is None:
        return None
    step = value.args.get("step")
    stride = 1
    if isinstance(step, exp.Interval):
        unit = (step.text("unit") or "").upper()
        if unit not in _FIXED_INTERVAL_DAYS:
            return None
        # Trino writes an interval magnitude as a quoted literal: INTERVAL '1'.
        magnitude = _literal_int(step.this, allow_string=True)
        if magnitude is None or magnitude <= 0:
            return None
        stride = magnitude * _FIXED_INTERVAL_DAYS[unit]
    elif step is not None:
        magnitude = _literal_int(step)
        if magnitude is None or magnitude == 0:
            return None
        stride = abs(magnitude)
    span = abs(end - start)
    return span // stride + 1


def _literal_point(value: exp.Expr | None) -> int | None:
    """An endpoint as an integer: a plain number, or a date as its ordinal."""
    if value is None:
        return None
    if isinstance(value, exp.Cast):
        inner = value.this
        text = inner.name if isinstance(inner, exp.Literal) else None
        if text is None:
            return None
        try:
            return date.fromisoformat(text).toordinal()
        except ValueError:
            return None
    return _literal_int(value)


def _literal_int(value: exp.Expr | None, allow_string: bool = False) -> int | None:
    """A literal integer, or None for anything the agent could vary."""
    if isinstance(value, exp.Neg):
        magnitude = _literal_int(value.this, allow_string)
        return None if magnitude is None else -magnitude
    if not isinstance(value, exp.Literal):
        return None
    if value.is_string and not allow_string:
        return None
    try:
        return int(value.name)
    except ValueError:
        return None


def _generator_rows(
    generator: exp.Expr,
    projections: Mapping[str, list[exp.Expr]] | None = None,
    seen: frozenset[str] = frozenset(),
    budget: list[int] | None = None,
) -> int:
    """Rows a bounded generator yields: a literal array's length, or 1 for a
    scanned column, whose rows belong to a table the plan already priced.

    Every literal array under the generator counts, not only a direct child:
    MAP(ARRAY[...], ARRAY[...]) yields its key array's length, and reading
    only the top node would price a 5000-entry lookup as one row. A column a
    projection bound counts as whatever it was bound to — array_agg over a
    1000-row sequence is a 1000-element array by the time UNNEST reads it.

    Exhausting the budget returns the cap, not the width found so far: this
    figure becomes a multiplier, and a partial answer is a discount.
    """
    projections = projections if projections is not None else {}
    budget = budget if budget is not None else [_MAX_RESOLVE_STEPS]
    widest = 1
    for value in generator.find_all(exp.Array, exp.Struct):
        widest = max(widest, len(value.expressions or []))
    for series in generator.find_all(exp.GenerateSeries):
        length = _sequence_length(series)
        if length is not None:
            widest = max(widest, length)
    for column in generator.find_all(exp.Column):
        budget[0] -= 1
        if budget[0] <= 0:
            return _MAX_COUNTED_ROWS
        key = _column_key(column)
        if key in seen or len(seen) >= _MAX_ALIAS_DEPTH:
            continue
        for bound in _column_bindings(column, projections):
            widest = max(
                widest, _generator_rows(bound, projections, seen | {key}, budget)
            )
    return widest


def has_unpriceable_shape(sql: str, dialect: str) -> bool:
    """True if the plan's estimates would miss this query's real row work.

    Only row generators qualify. Measured on Trino 476,
    UNNEST(sequence(1, 10000)) over a 60,175-row table plans as 60,175 rows
    and produces 601 million: a generator manufactures rows from an argument
    and contributes no operator the planner sizes. Every other row explosion
    — products, low-cardinality join keys, correlated nested loops — the
    planner does size, and the budget prices from the plan itself.

    Unparseable SQL counts as unpriceable: if we cannot prove the shape is
    safe, we assume the risky answer. Nesting deep enough to exhaust the
    interpreter's stack raises RecursionError rather than a parser error, and
    that is the same answer — a shape nobody read is not a shape anybody
    vouched for. validate_query's depth cap stops such SQL earlier; this
    keeps the module's own contract if it is ever called without it.
    """
    # The walk recurses too — a long chain of set operations exhausts the
    # stack after the parse, and an exception escaping here would crash the
    # quote instead of withholding it.
    try:
        tree = sqlglot.parse_one(sql, dialect=dialect)
        return _generates_rows(tree)
    except (sqlglot.errors.SqlglotError, RecursionError):
        return True


def table_scan_counts(sql: str, dialect: str) -> dict[str, int]:
    """How many times the SQL reads each fully-qualified table.

    Measured against Trino 476, the IO plan reports one entry per table no
    matter how often the query reads it — a 4-times-referenced CTE and a
    3-way self-join both return a single entry, while processing 4x and 3x
    the rows. Comparing these counts against the entries the plan returned is
    what recovers the shortfall.

    A CTE reference is not skipped: it re-reads whatever the CTE body scans,
    so the body's tables count once per reference to it.

    Returns {} for unparseable SQL — the shape check has already refused it.
    """
    return _walk_scans(sql, dialect)[0]


def _walk_scans(sql: str, dialect: str) -> tuple[dict[str, int], bool]:
    """The read counts, and whether the walk budget ran out reaching them."""
    # The read walk recurses once per reference too, so the guard covers the
    # whole count, not just the parse: nesting past the interpreter's stack
    # raises RecursionError rather than a parser error, and engine.py treats
    # neither as an engine failure. A count nobody finished is saturated,
    # which denies — the same answer the shape check gives such SQL.
    try:
        tree = sqlglot.parse_one(sql, dialect=dialect)
        return _counted_reads(tree)
    except (sqlglot.errors.SqlglotError, RecursionError):
        return {}, True


def _counted_reads(tree: exp.Expr) -> tuple[dict[str, int], bool]:
    """How many times the SQL reads each table, and whether counting ran out."""

    bodies = _cte_bodies(tree)
    counts: dict[str, int] = {}
    # A chain of CTEs each referenced twice doubles walk work per level of
    # depth; a running total caught before recursing bounds that growth
    # instead of only bounding the counts it would have produced.
    budget = [_MAX_SCAN_COUNT]

    # What one read of a name costs, walked once and then applied per
    # reference: re-walking a body for every reference spent the budget on the
    # body's width, so a 5,000-column CTE read 5,000 times took 16 seconds
    # before any engine was contacted. The counts are identical either way —
    # a read is still charged once per reference.
    per_name: dict[tuple[str, frozenset[str]], dict[str, int]] = {}

    def reads(node: exp.Expr, pending: frozenset[str]) -> dict[str, int]:
        found: dict[str, int] = {}
        for table in node.find_all(exp.Table):
            if budget[0] <= 0:
                return found
            parts = [part.name for part in table.parts]
            name = parts[0].lower()
            if len(parts) == 1 and name in bodies:
                # A CTE that references itself would recurse forever, and its
                # depth is the engine's business, not the quote's.
                if name in pending:
                    continue
                # Keyed by the enclosing scope as well: the same name inside
                # a different set of pending bindings can resolve elsewhere.
                memo_key = (name, pending)
                if memo_key not in per_name:
                    once: dict[str, int] = {}
                    # A name bound more than once is counted through every
                    # body it could mean: scope decides which, and skipping
                    # the others let a decoy binding hide the reads.
                    for body in bodies[name]:
                        budget[0] -= 1
                        for table_key, count in reads(body, pending | {name}).items():
                            once[table_key] = once.get(table_key, 0) + count
                    per_name[memo_key] = once
                for table_key, count in per_name[memo_key].items():
                    found[table_key] = found.get(table_key, 0) + count
                continue
            key = ".".join(parts).lower()
            found[key] = found.get(key, 0) + 1
            budget[0] -= 1
        return found

    # From the query with its WITH detached: a CTE body counts once per
    # reference to it, not once where it is defined.
    for table_key, count in reads(_without_cte_bodies(tree), frozenset()).items():
        counts[table_key] = counts.get(table_key, 0) + count
    # Counting each name once reaches figures a re-walk never survived to
    # produce: a 400-deep doubling chain is 2^399 reads, a 121-digit integer
    # that would then scale a byte quote through bignum arithmetic. Past this
    # many reads the exact number buys nothing — the query is already beyond
    # any budget — so it saturates, which denies rather than quotes.
    saturated = budget[0] <= 0 or any(
        count > _MAX_COUNTED_READS for count in counts.values()
    )
    return counts, saturated


def scan_counts_saturated(sql: str, dialect: str) -> bool:
    """True if counting this query's reads hit the walk budget.

    A saturated count is not a small count: the byte quote is scaled UP by
    how many reads the plan folded together, so counting fewer reads than
    the query really does makes it cheaper. Measured, a 1,471-character CTE
    chain reads a table 524,288 times and counts 3,328 — a 61 GiB query
    priced at 3 GiB. The caller denies on this rather than take the discount.
    """
    return _walk_scans(sql, dialect)[1]


def _without_cte_bodies(tree: exp.Expr) -> exp.Expr:
    """The query with its WITH clause detached, so CTE bodies are counted
    where they are referenced rather than once where they are defined."""
    copy = tree.copy()
    for with_clause in list(copy.find_all(exp.With)):
        with_clause.pop()
    return copy
