# 0005 — Gate on maximum intermediate rows, not output rows

**Status:** Accepted

## Context

Given a plan, which number to gate on? The query's output row count is
trivially launderable. Measured on Trino 476: a 902,625,000-row cross join
reports 10 output rows under `LIMIT 10`, 1 under `count(*)`, and 15,000
under `DISTINCT` or `GROUP BY` — while doing the full 902M rows of work
either way.

## Decision

Gate on the **maximum `outputRowCount` over every operator in the plan** —
the widest intermediate the query would build. New budget dimension
`LAGAAM_MAX_INTERMEDIATE_ROWS`, default 50,000,000: measured legitimate
analytics peaks at 6,001,215 rows at sf1 scale, the cheapest measured
explosion is 225,000,000, and the default sits between (8.3x headroom,
4.5x margin). When a join's own estimate is NaN and it has no equality
criteria, it is charged the product of its children — catching a cross
join laundered behind `LIKE '%x%'`. A NaN join *with* equality criteria
takes the max of its children instead: charging a product through a
decorrelated correlated subquery invented 12 billion rows for a query that
touches 10,000.

## Consequences

- `LIMIT` cannot mask expensive work; the denial text says so explicitly.
- The default fits sf1-class data; larger deployments must raise it.
- Distinct from `LAGAAM_MAX_ROWS` (rows *scanned*), which remains a
  separate, still-supported dimension.
