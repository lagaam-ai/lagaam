# Pinot dialect measurements — 2026-09-20

Every valid Pinot statement an agent writes is re-rendered by sqlglot on the
way to the broker (`validate_query` injects LIMIT, `two_part_sql` strips the
synthetic catalog). Under the generic dialect (`""`) that re-render rewrote
function names and types. Measured on Pinot 1.5.1 (batch quickstart, broker
`localhost:8000`), single-stage (SSE) and multi-stage (MSE) engines, raw SQL
against the re-rendered form. Probe scripts: `definitive.py` (below) over a
55-shape table; a 400-name survey of sqlglot's generic FUNCTIONS table was
used to pick the candidates (every name that renders under a different
spelling or drops an argument, filtered to names Pinot itself recognises).

Verdict legend: **BREAKS** = raw runs, re-rendered errors; **DIFF** = both
run, rows differ; `fixes` = raw errors, re-rendered runs; `same` = identical.

## Before (generic dialect)

| shape | rendered as | SSE raw | SSE rendered | MSE raw | MSE rendered | verdict |
|---|---|---|---|---|---|---|
| CAST STRING | `CAST(ArrDelay AS TEXT) AS y` | OK [["13"]] | ERR Caught exception while initi | OK [["13"]] | ERR QueryValidationError: From l | **BREAKS** |
| JSON_EXTRACT_SCALAR | `JSON_EXTRACT_SCALAR(Carrier, '$.a') AS y` | OK [["x"]] | ERR SQLParsingError: Expect 3 or | OK [["x"]] | ERR QueryValidationError: From l | **BREAKS** |
| SUBSTR 3 | `SUBSTRING(Carrier, 0, 1) AS y` | OK [["A"]] | OK [[""]] | OK [["A"]] | OK [[""]] | **DIFF** |
| SUBSTR 2 | `SUBSTRING(Carrier, 1) AS y` | OK [["A"]] | OK [["AA"]] | OK [["A"]] | OK [["AA"]] | **DIFF** |
| STRPOS | `STR_POSITION(Carrier, 'A') AS y` | OK [[0]] | ERR Unsupported function: strpos | OK [[0]] | ERR QueryValidationError: From l | **BREAKS** |
| LOG10 | `LOG(10, 100) AS y` | OK [[2.0]] | ERR Caught exception while initi | OK [[2.0]] | ERR QueryValidationError: From l | **BREAKS** |
| LOG2 | `LOG(2, 8) AS y` | OK [[3.0]] | ERR Caught exception while initi | OK [[3.0]] | ERR QueryValidationError: From l | **BREAKS** |
| TRUNCATE 2 | `TRUNC(1.234, 2) AS y` | OK [[1.23]] | ERR Unsupported function: trunc | OK [[1.23]] | ERR QueryValidationError: From l | **BREAKS** |
| TRUNCATE 1 | `TRUNC(1.234) AS y` | OK [[1.0]] | ERR Unsupported function: trunc | OK [[1.0]] | ERR QueryValidationError: From l | **BREAKS** |
| VAR_POP | `VARIANCE_POP(ArrDelay) AS y` | OK [[8.002872922675329e+17]] | ERR Unsupported function: varian | OK [[8.002872922675328e+17]] | ERR QueryValidationError: From l | **BREAKS** |
| VAR_SAMP | `VARIANCE(ArrDelay) AS y` | OK [[8.00369415129746e+17]] | ERR Unsupported function: varian | OK [[8.00369415129746e+17]] | ERR QueryValidationError: From l | **BREAKS** |
| BOOL_AND | `LOGICAL_AND(ArrDelay > -1000) AS y` | OK [[false]] | ERR Unsupported function: logica | OK [[false]] | ERR QueryValidationError: From l | **BREAKS** |
| BOOL_OR | `LOGICAL_OR(ArrDelay > 0) AS y` | OK [[true]] | ERR Unsupported function: logica | OK [[true]] | ERR QueryValidationError: From l | **BREAKS** |
| ARRAY literal | `ARRAY('a', 'b') AS y` | OK [[["a", "b"]]] | OK [[["a", "b"]]] | OK [[["a", "b"]]] | ERR QueryValidationError:  | **BREAKS** |
| ARRAY_AGG 2 | `ARRAY_AGG(Carrier) AS y` | OK [[["DL"]]] | ERR 1 servers [172.17.0.2_O] not | OK [[["DL"]]] | ERR Received 1 error from stage  | **BREAKS** |
| ARRAY_AGG 3 | REJECTED by validate_query: The SQL could not be parsed as  (line 1, col 41): The number | | | | | **rejects valid SQL** |
| MOD | `ArrDelay % 2 AS y` | OK [[1.0]] | OK [[1.0]] | OK [[1]] | OK [[1]] | same |
| percent op | `ArrDelay % 2 AS y` | OK [[1.0]] | OK [[1.0]] | OK [[1]] | OK [[1]] | same |
| FROM_BASE64 | `` | OK [["4141"]] | OK [["4141"]] | OK [["4141"]] | OK [["4141"]] | same |
| CEILING | `CEIL(1.2) AS y` | OK [[2.0]] | OK [[2.0]] | OK [[2]] | OK [[2]] | same |
| DAYOFMONTH | `DAY_OF_MONTH(CAST(DaysSinceEpoch * 86400000 AS TIMESTAMP)) A` | OK [[1]] | OK [[1]] | OK [[1]] | OK [[1]] | same |
| DAYOFWEEK | `DAY_OF_WEEK(CAST(DaysSinceEpoch * 86400000 AS TIMESTAMP)) AS` | OK [[3]] | OK [[3]] | OK [[3]] | OK [[3]] | same |
| DAYOFYEAR | `DAY_OF_YEAR(CAST(DaysSinceEpoch * 86400000 AS TIMESTAMP)) AS` | OK [[1]] | OK [[1]] | OK [[1]] | OK [[1]] | same |
| WEEKOFYEAR | `WEEK_OF_YEAR(CAST(DaysSinceEpoch * 86400000 AS TIMESTAMP)) A` | OK [[1]] | OK [[1]] | OK [[1]] | OK [[1]] | same |
| YEAROFWEEK | `YEAR_OF_WEEK(CAST(DaysSinceEpoch * 86400000 AS TIMESTAMP)) A` | OK [[2014]] | OK [[2014]] | OK [[2014]] | OK [[2014]] | same |
| ISNAN | `IS_NAN(1.0) AS y` | OK [[0]] | OK [[0]] | OK [[0]] | OK [[0]] | same |
| ENDSWITH | `ENDS_WITH(Carrier, 'A') AS y` | OK [[true]] | OK [[true]] | OK [[true]] | OK [[true]] | same |
| STARTSWITH | `STARTS_WITH(Carrier, 'A') AS y` | OK [[true]] | OK [[true]] | OK [[true]] | OK [[true]] | same |
| POW | `POWER(2, 3) AS y` | OK [[8.0]] | OK [[8.0]] | OK [[8.0]] | OK [[8.0]] | same |
| TIMESTAMP_DIFF | `TIMESTAMPDIFF(DAY, CAST(0 AS TIMESTAMP), CAST(86400000 AS TI` | ERR SQLParsingError: Caught exce | ERR SQLParsingError: It seems th | ERR SQLParsingError: Caught exce | OK [[1]] | fixes |
| IFNULL | `COALESCE(ArrDelay, 0) AS y` | ERR Unsupported function: ifnull | OK [[13]] | ERR QueryValidationError: From l | OK [[13]] | fixes |
| NVL | `COALESCE(ArrDelay, 0) AS y` | ERR Unsupported function: nvl | OK [[13]] | ERR QueryValidationError: From l | OK [[13]] | fixes |
| IF | `CASE WHEN ArrDelay > 0 THEN 'late' ELSE 'ok' END AS y` | ERR Unsupported function: if | OK [["late"]] | ERR QueryValidationError: From l | OK [["late"]] | fixes |
| REGEXP_EXTRACT | `REGEXP_EXTRACT(Carrier, '(A)', 1) AS y` | OK [["A"]] | OK [["A"]] | OK [["A"]] | OK [["A"]] | same |
| REPEAT | `REPEAT(Carrier, 2) AS y` | OK [["AAAA"]] | OK [["AAAA"]] | OK [["AAAA"]] | OK [["AAAA"]] | same |
| LEFT/RIGHT | `LEFT(Carrier, 1) AS a, RIGHT(Carrier, 1) AS b` | OK [["A", "A"]] | OK [["A", "A"]] | OK [["A", "A"]] | OK [["A", "A"]] | same |
| SPLIT | `SPLIT(Carrier, 'A') AS y` | OK [[[""]]] | OK [[[""]]] | OK [[[""]]] | OK [[[""]]] | same |
| REPLACE | `REPLACE(Carrier, 'A', 'B') AS y` | OK [["BB"]] | OK [["BB"]] | OK [["BB"]] | OK [["BB"]] | same |
| ROUND 2 | `ROUND(1.234, 2) AS y` | OK [[0]] | OK [[0]] | OK [[0]] | OK [[0]] | same |
| TRIM | `TRIM(Carrier) AS y` | OK [["AA"]] | OK [["AA"]] | OK [["AA"]] | OK [["AA"]] | same |
| LTRIM | `LTRIM(Carrier) AS y` | OK [["AA"]] | OK [["AA"]] | OK [["AA"]] | OK [["AA"]] | same |
| LPAD | `LPAD(Carrier, 4, '-') AS y` | OK [["--AA"]] | OK [["--AA"]] | OK [["--AA"]] | OK [["--AA"]] | same |
| STRING_TO_ARRAY | `STRING_TO_ARRAY('a,b', ',') AS y` | OK [[["a", "b"]]] | OK [[["a", "b"]]] | OK [[["a", "b"]]] | OK [[["a", "b"]]] | same |
| REGEXP_LIKE 3 | `Carrier` | OK [["AA"]] | OK [["AA"]] | OK [["AA"]] | OK [["AA"]] | same |
| MD5/SHA | `MD5(TOUTF8(Carrier)) AS a, SHA(TOUTF8(Carrier)) AS b` | OK [["3b98e2dffc6cb06a89dcb0d5c | OK [["3b98e2dffc6cb06a89dcb0d5c | OK [["3b98e2dffc6cb06a89dcb0d5c | OK [["3b98e2dffc6cb06a89dcb0d5c | same |
| TO_BASE64 | `TO_BASE64(TOUTF8(Carrier)) AS y` | OK [["QUE="]] | OK [["QUE="]] | OK [["QUE="]] | OK [["QUE="]] | same |
| NULLIF | `NULLIF(ArrDelay, 13) AS y` | ERR Query execution error on: Se | ERR Query execution error on: Se | OK [[0]] | OK [[0]] | same |
| ROW_NUMBER | `Carrier, ROW_NUMBER() OVER (ORDER BY ArrDelay) AS y` | ERR SQLParsingError: It seems th | ERR SQLParsingError: It seems th | OK [["AA", 1]] | OK [["AA", 1]] | same |
| LAG | `Carrier, LAG(ArrDelay, 1) OVER (ORDER BY ArrDelay) AS y` | ERR SQLParsingError: It seems th | ERR SQLParsingError: It seems th | OK [["AA", null]] | OK [["AA", null]] | same |
| EXTRACT | `EXTRACT(DAY` | OK [[1]] | OK [[1]] | OK [[1]] | OK [[1]] | same |
| CAST LONG | `CAST(ArrDelay AS BIGINT) AS y` | OK [[13]] | OK [[13]] | OK [[13]] | OK [[13]] | same |
| CAST DOUBLE/FLOAT | `CAST(ArrDelay AS DOUBLE) AS a, CAST(ArrDelay AS FLOAT) AS b` | OK [[13.0, 13.0]] | OK [[13.0, 13.0]] | OK [[13.0, 13.0]] | OK [[13.0, 13.0]] | same |
| CAST BIG_DECIMAL | `CAST(ArrDelay AS BIG_DECIMAL) AS y` | OK [["13"]] | OK [["13"]] | OK [["13"]] | OK [["13"]] | same |
| CAST JSON (sse) | `CAST(ArrDelay AS JSON) AS y` | OK [["13"]] | OK [["13"]] | ERR QueryValidationError: From l | ERR QueryValidationError: From l | same |
| CAST TIMESTAMP | `CAST(0 AS TIMESTAMP) AS y` | ERR SQLParsingError: It seems th | ERR SQLParsingError: It seems th | OK [["1970-01-01 00:00:00.0"]] | OK [["1970-01-01 00:00:00.0"]] | same |
| CAST BOOLEAN | `CAST(1 AS BOOLEAN) AS y` | OK [[true]] | OK [[true]] | OK [[false]] | OK [[false]] | same |

Two `DIFF` rows above are not rewrites: `VAR_POP` / `VAR_SAMP` differ in the
last digits between *any* two runs (float summation order), and
`PERCENTILETDIGEST` is an approximation. Confirmed by running the raw form
twice.

## After (the Pinot dialect: preserve spellings, STRING type, ARRAY[...] inline, typed aggregates renamed at render)

Re-run against the shipped implementation, not a prototype: the probe
imports the card's dialect. Verdicts: 50 same, 4 fixes, 2 DIFF, 0 BREAKS.

| shape | rendered as | SSE raw | SSE rendered | MSE raw | MSE rendered | verdict |
|---|---|---|---|---|---|---|
| CAST STRING | `CAST(ArrDelay AS STRING) AS y` | OK [["13"]] | OK [["13"]] | OK [["13"]] | OK [["13"]] | same |
| JSON_EXTRACT_SCALAR | `JSON_EXTRACT_SCALAR(Carrier, '$.a', 'STRING', 'x') AS y` | OK [["x"]] | OK [["x"]] | OK [["x"]] | OK [["x"]] | same |
| SUBSTR 3 | `SUBSTR(Carrier, 0, 1) AS y` | OK [["A"]] | OK [["A"]] | OK [["A"]] | OK [["A"]] | same |
| SUBSTR 2 | `SUBSTR(Carrier, 1) AS y` | OK [["A"]] | OK [["A"]] | OK [["A"]] | OK [["A"]] | same |
| STRPOS | `STRPOS(Carrier, 'A') AS y` | OK [[0]] | OK [[0]] | OK [[0]] | OK [[0]] | same |
| LOG10 | `LOG10(100) AS y` | OK [[2.0]] | OK [[2.0]] | OK [[2.0]] | OK [[2.0]] | same |
| LOG2 | `LOG2(8) AS y` | OK [[3.0]] | OK [[3.0]] | OK [[3.0]] | OK [[3.0]] | same |
| TRUNCATE 2 | `TRUNCATE(1.234, 2) AS y` | OK [[1.23]] | OK [[1.23]] | OK [[1.23]] | OK [[1.23]] | same |
| TRUNCATE 1 | `TRUNCATE(1.234) AS y` | OK [[1.0]] | OK [[1.0]] | OK [[1.0]] | OK [[1.0]] | same |
| VAR_POP | `VAR_POP(ArrDelay) AS y` | OK [[8.002872922675329e+17]] | OK [[8.002872922675328e+17]] | OK [[8.002872922675329e+17]] | OK [[8.002872922675328e+17]] | **DIFF** |
| VAR_SAMP | `VAR_SAMP(ArrDelay) AS y` | OK [[8.003694151297457e+17]] | OK [[8.003694151297458e+17]] | OK [[8.00369415129746e+17]] | OK [[8.003694151297459e+17]] | **DIFF** |
| BOOL_AND | `BOOL_AND(ArrDelay > -1000) AS y` | OK [[false]] | OK [[false]] | OK [[false]] | OK [[false]] | same |
| BOOL_OR | `BOOL_OR(ArrDelay > 0) AS y` | OK [[true]] | OK [[true]] | OK [[true]] | OK [[true]] | same |
| ARRAY literal | `ARRAY['a', 'b'] AS y` | OK [[["a", "b"]]] | OK [[["a", "b"]]] | OK [[["a", "b"]]] | OK [[["a", "b"]]] | same |
| ARRAY_AGG 2 | `ARRAY_AGG(Carrier, 'STRING') AS y` | OK [[["DL"]]] | OK [[["DL"]]] | OK [[["DL"]]] | OK [[["DL"]]] | same |
| ARRAY_AGG 3 | `ARRAY_AGG(Carrier, 'STRING', TRUE) AS y` | OK [[["DL"]]] | OK [[["DL"]]] | OK [[["DL"]]] | OK [[["DL"]]] | same |
| MOD | `ArrDelay % 2 AS y` | OK [[1.0]] | OK [[1.0]] | OK [[1]] | OK [[1]] | same |
| percent op | `ArrDelay % 2 AS y` | OK [[1.0]] | OK [[1.0]] | OK [[1]] | OK [[1]] | same |
| FROM_BASE64 | `` | OK [["4141"]] | OK [["4141"]] | OK [["4141"]] | OK [["4141"]] | same |
| CEILING | `CEIL(1.2) AS y` | OK [[2.0]] | OK [[2.0]] | OK [[2]] | OK [[2]] | same |
| DAYOFMONTH | `DAY_OF_MONTH(CAST(DaysSinceEpoch * 86400000 AS TIMESTAMP)) A` | OK [[1]] | OK [[1]] | OK [[1]] | OK [[1]] | same |
| DAYOFWEEK | `DAY_OF_WEEK(CAST(DaysSinceEpoch * 86400000 AS TIMESTAMP)) AS` | OK [[3]] | OK [[3]] | OK [[3]] | OK [[3]] | same |
| DAYOFYEAR | `DAY_OF_YEAR(CAST(DaysSinceEpoch * 86400000 AS TIMESTAMP)) AS` | OK [[1]] | OK [[1]] | OK [[1]] | OK [[1]] | same |
| WEEKOFYEAR | `WEEK_OF_YEAR(CAST(DaysSinceEpoch * 86400000 AS TIMESTAMP)) A` | OK [[1]] | OK [[1]] | OK [[1]] | OK [[1]] | same |
| YEAROFWEEK | `YEAR_OF_WEEK(CAST(DaysSinceEpoch * 86400000 AS TIMESTAMP)) A` | OK [[2014]] | OK [[2014]] | OK [[2014]] | OK [[2014]] | same |
| ISNAN | `IS_NAN(1.0) AS y` | OK [[0]] | OK [[0]] | OK [[0]] | OK [[0]] | same |
| ENDSWITH | `ENDS_WITH(Carrier, 'A') AS y` | OK [[true]] | OK [[true]] | OK [[true]] | OK [[true]] | same |
| STARTSWITH | `STARTS_WITH(Carrier, 'A') AS y` | OK [[true]] | OK [[true]] | OK [[true]] | OK [[true]] | same |
| POW | `POWER(2, 3) AS y` | OK [[8.0]] | OK [[8.0]] | OK [[8.0]] | OK [[8.0]] | same |
| TIMESTAMP_DIFF | `TIMESTAMPDIFF(DAY, CAST(0 AS TIMESTAMP), CAST(86400000 AS TI` | ERR SQLParsingError: Caught exce | ERR SQLParsingError: It seems th | ERR SQLParsingError: Caught exce | OK [[1]] | fixes |
| IFNULL | `COALESCE(ArrDelay, 0) AS y` | ERR Unsupported function: ifnull | OK [[13]] | ERR QueryValidationError: From l | OK [[13]] | fixes |
| NVL | `COALESCE(ArrDelay, 0) AS y` | ERR Unsupported function: nvl | OK [[13]] | ERR QueryValidationError: From l | OK [[13]] | fixes |
| IF | `CASE WHEN ArrDelay > 0 THEN 'late' ELSE 'ok' END AS y` | ERR Unsupported function: if | OK [["late"]] | ERR QueryValidationError: From l | OK [["late"]] | fixes |
| REGEXP_EXTRACT | `REGEXP_EXTRACT(Carrier, '(A)', 1) AS y` | OK [["A"]] | OK [["A"]] | OK [["A"]] | OK [["A"]] | same |
| REPEAT | `REPEAT(Carrier, 2) AS y` | OK [["AAAA"]] | OK [["AAAA"]] | OK [["AAAA"]] | OK [["AAAA"]] | same |
| LEFT/RIGHT | `LEFT(Carrier, 1) AS a, RIGHT(Carrier, 1) AS b` | OK [["A", "A"]] | OK [["A", "A"]] | OK [["A", "A"]] | OK [["A", "A"]] | same |
| SPLIT | `SPLIT(Carrier, 'A') AS y` | OK [[[""]]] | OK [[[""]]] | OK [[[""]]] | OK [[[""]]] | same |
| REPLACE | `REPLACE(Carrier, 'A', 'B') AS y` | OK [["BB"]] | OK [["BB"]] | OK [["BB"]] | OK [["BB"]] | same |
| ROUND 2 | `ROUND(1.234, 2) AS y` | OK [[0]] | OK [[0]] | OK [[0]] | OK [[0]] | same |
| TRIM | `TRIM(Carrier) AS y` | OK [["AA"]] | OK [["AA"]] | OK [["AA"]] | OK [["AA"]] | same |
| LTRIM | `LTRIM(Carrier) AS y` | OK [["AA"]] | OK [["AA"]] | OK [["AA"]] | OK [["AA"]] | same |
| LPAD | `LPAD(Carrier, 4, '-') AS y` | OK [["--AA"]] | OK [["--AA"]] | OK [["--AA"]] | OK [["--AA"]] | same |
| STRING_TO_ARRAY | `STRING_TO_ARRAY('a,b', ',') AS y` | OK [[["a", "b"]]] | OK [[["a", "b"]]] | OK [[["a", "b"]]] | OK [[["a", "b"]]] | same |
| REGEXP_LIKE 3 | `Carrier` | OK [["AA"]] | OK [["AA"]] | OK [["AA"]] | OK [["AA"]] | same |
| MD5/SHA | `MD5(TOUTF8(Carrier)) AS a, SHA(TOUTF8(Carrier)) AS b` | OK [["3b98e2dffc6cb06a89dcb0d5c | OK [["3b98e2dffc6cb06a89dcb0d5c | OK [["3b98e2dffc6cb06a89dcb0d5c | OK [["3b98e2dffc6cb06a89dcb0d5c | same |
| TO_BASE64 | `TO_BASE64(TOUTF8(Carrier)) AS y` | OK [["QUE="]] | OK [["QUE="]] | OK [["QUE="]] | OK [["QUE="]] | same |
| NULLIF | `NULLIF(ArrDelay, 13) AS y` | ERR Query execution error on: Se | ERR Query execution error on: Se | OK [[0]] | OK [[0]] | same |
| ROW_NUMBER | `Carrier, ROW_NUMBER() OVER (ORDER BY ArrDelay) AS y` | ERR SQLParsingError: It seems th | ERR SQLParsingError: It seems th | OK [["AA", 1]] | OK [["AA", 1]] | same |
| LAG | `Carrier, LAG(ArrDelay, 1) OVER (ORDER BY ArrDelay) AS y` | ERR SQLParsingError: It seems th | ERR SQLParsingError: It seems th | OK [["AA", null]] | OK [["AA", null]] | same |
| EXTRACT | `EXTRACT(DAY` | OK [[1]] | OK [[1]] | OK [[1]] | OK [[1]] | same |
| CAST LONG | `CAST(ArrDelay AS BIGINT) AS y` | OK [[13]] | OK [[13]] | OK [[13]] | OK [[13]] | same |
| CAST DOUBLE/FLOAT | `CAST(ArrDelay AS DOUBLE) AS a, CAST(ArrDelay AS FLOAT) AS b` | OK [[13.0, 13.0]] | OK [[13.0, 13.0]] | OK [[13.0, 13.0]] | OK [[13.0, 13.0]] | same |
| CAST BIG_DECIMAL | `CAST(ArrDelay AS BIG_DECIMAL) AS y` | OK [["13"]] | OK [["13"]] | OK [["13"]] | OK [["13"]] | same |
| CAST JSON (sse) | `CAST(ArrDelay AS JSON) AS y` | OK [["13"]] | OK [["13"]] | ERR QueryValidationError: From l | ERR QueryValidationError: From l | same |
| CAST TIMESTAMP | `CAST(0 AS TIMESTAMP) AS y` | ERR SQLParsingError: It seems th | ERR SQLParsingError: It seems th | OK [["1970-01-01 00:00:00.0"]] | OK [["1970-01-01 00:00:00.0"]] | same |
| CAST BOOLEAN | `CAST(1 AS BOOLEAN) AS y` | OK [[true]] | OK [[true]] | OK [[false]] | OK [[false]] | same |

The two remaining `DIFF` rows are the variance float noise above.
`TIMESTAMP_DIFF` shows as `fixes` on MSE only because Pinot's own spelling is
`TIMESTAMPDIFF` and sqlglot's rename happens to land on it.

One correction against the prototype's design: sqlglot's own
`inline_array_sql` emits a bare `['a', 'b']`, not `ARRAY['a', 'b']`, and
Pinot's parser rejects that on both engines. The shipped generator prefixes
the keyword. Measured both ways:

| rendering | SSE | MSE |
|---|---|---|
| `SELECT ['a', 'b'] AS y` | ERR SQLParsingError: Caught exception while parsing query | ERR SQLParsingError: Caught exception while parsing query |
| `SELECT ARRAY['a', 'b'] AS y` | OK [[["a", "b"]]] | OK [[["a", "b"]]] |

The variance noise is Pinot's, not the re-render's: three raw runs of
`SELECT VAR_SAMP(ArrDelay) FROM airlineStats LIMIT 5` on SSE returned
8.00369415129746e+17, 8.003694151297462e+17, 8.00369415129746e+17.
