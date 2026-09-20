import json, urllib.request, sqlglot
from lagaam.adapters.pinot.dialect import PINOT_DIALECT_CARD
from lagaam.core.safety import validate_query
from lagaam.adapters.pinot.names import two_part_sql
DIALECT = PINOT_DIALECT_CARD.sqlglot_dialect
def run(sql, mse):
    body = json.dumps({"sql": sql, "queryOptions": f"useMultistageEngine={'true' if mse else 'false'}"}).encode()
    req = urllib.request.Request("http://localhost:8000/query/sql", data=body, headers={"Content-Type": "application/json"})
    try: d = json.load(urllib.request.urlopen(req, timeout=30))
    except Exception as e: return ("HTTP", str(e)[:60])
    exc = d.get("exceptions") or []
    if exc: return ("ERR", exc[0].get("message", "")[:90].replace("\n", " "))
    return ("OK", json.dumps(d.get("resultTable", {}).get("rows"))[:60])
T="pinot.default.airlineStats"
Q = {
 "CAST STRING":        f"SELECT CAST(ArrDelay AS STRING) AS y FROM {T} LIMIT 1",
 "JSON_EXTRACT_SCALAR": f"SELECT JSON_EXTRACT_SCALAR(Carrier, '$.a', 'STRING', 'x') AS y FROM {T} LIMIT 1",
 "SUBSTR 3":           f"SELECT SUBSTR(Carrier, 0, 1) AS y FROM {T} WHERE Carrier = 'AA' LIMIT 1",
 "SUBSTR 2":           f"SELECT SUBSTR(Carrier, 1) AS y FROM {T} WHERE Carrier = 'AA' LIMIT 1",
 "STRPOS":             f"SELECT STRPOS(Carrier, 'A') AS y FROM {T} WHERE Carrier = 'AA' LIMIT 1",
 "LOG10":              f"SELECT LOG10(100) AS y FROM {T} LIMIT 1",
 "LOG2":               f"SELECT LOG2(8) AS y FROM {T} LIMIT 1",
 "TRUNCATE 2":         f"SELECT TRUNCATE(1.234, 2) AS y FROM {T} LIMIT 1",
 "TRUNCATE 1":         f"SELECT TRUNCATE(1.234) AS y FROM {T} LIMIT 1",
 "VAR_POP":            f"SELECT VAR_POP(ArrDelay) AS y FROM {T}",
 "VAR_SAMP":           f"SELECT VAR_SAMP(ArrDelay) AS y FROM {T}",
 "BOOL_AND":           f"SELECT BOOL_AND(ArrDelay > -1000) AS y FROM {T}",
 "BOOL_OR":            f"SELECT BOOL_OR(ArrDelay > 0) AS y FROM {T}",
 "ARRAY literal":      f"SELECT ARRAY['a','b'] AS y FROM {T} LIMIT 1",
 "ARRAY_AGG 2":        f"SELECT ARRAY_AGG(Carrier, 'STRING') AS y FROM {T} WHERE ArrDelay > 500",
 "ARRAY_AGG 3":        f"SELECT ARRAY_AGG(Carrier, 'STRING', true) AS y FROM {T} WHERE ArrDelay > 500",
 "MOD":                f"SELECT MOD(ArrDelay, 2) AS y FROM {T} WHERE Carrier = 'AA' AND ArrDelay = 13 LIMIT 1",
 "percent op":         f"SELECT ArrDelay % 2 AS y FROM {T} WHERE Carrier = 'AA' AND ArrDelay = 13 LIMIT 1",
 "FROM_BASE64":        f"SELECT FROM_BASE64('QUE=') AS y FROM {T} LIMIT 1",
 "CEILING":            f"SELECT CEILING(1.2) AS y FROM {T} LIMIT 1",
 "DAYOFMONTH":         f"SELECT DAYOFMONTH(CAST(DaysSinceEpoch * 86400000 AS TIMESTAMP)) AS y FROM {T} WHERE DaysSinceEpoch = 16071 LIMIT 1",
 "DAYOFWEEK":          f"SELECT DAYOFWEEK(CAST(DaysSinceEpoch * 86400000 AS TIMESTAMP)) AS y FROM {T} WHERE DaysSinceEpoch = 16071 LIMIT 1",
 "DAYOFYEAR":          f"SELECT DAYOFYEAR(CAST(DaysSinceEpoch * 86400000 AS TIMESTAMP)) AS y FROM {T} WHERE DaysSinceEpoch = 16071 LIMIT 1",
 "WEEKOFYEAR":         f"SELECT WEEKOFYEAR(CAST(DaysSinceEpoch * 86400000 AS TIMESTAMP)) AS y FROM {T} WHERE DaysSinceEpoch = 16071 LIMIT 1",
 "YEAROFWEEK":         f"SELECT YEAROFWEEK(CAST(DaysSinceEpoch * 86400000 AS TIMESTAMP)) AS y FROM {T} WHERE DaysSinceEpoch = 16071 LIMIT 1",
 "ISNAN":              f"SELECT ISNAN(1.0) AS y FROM {T} LIMIT 1",
 "ENDSWITH":           f"SELECT ENDSWITH(Carrier, 'A') AS y FROM {T} WHERE Carrier = 'AA' LIMIT 1",
 "STARTSWITH":         f"SELECT STARTSWITH(Carrier, 'A') AS y FROM {T} WHERE Carrier = 'AA' LIMIT 1",
 "POW":                f"SELECT POW(2, 3) AS y FROM {T} LIMIT 1",
 "TIMESTAMP_DIFF":     f"SELECT TIMESTAMP_DIFF(DAY, CAST(0 AS TIMESTAMP), CAST(86400000 AS TIMESTAMP)) AS y FROM {T} LIMIT 1",
 "IFNULL":             f"SELECT IFNULL(ArrDelay, 0) AS y FROM {T} WHERE Carrier = 'AA' AND ArrDelay = 13 LIMIT 1",
 "NVL":                f"SELECT NVL(ArrDelay, 0) AS y FROM {T} WHERE Carrier = 'AA' AND ArrDelay = 13 LIMIT 1",
 "IF":                 f"SELECT IF(ArrDelay > 0, 'late', 'ok') AS y FROM {T} WHERE Carrier = 'AA' AND ArrDelay = 13 LIMIT 1",
 "REGEXP_EXTRACT":     f"SELECT REGEXP_EXTRACT(Carrier, '(A)', 1) AS y FROM {T} WHERE Carrier = 'AA' LIMIT 1",
 "REPEAT":             f"SELECT REPEAT(Carrier, 2) AS y FROM {T} WHERE Carrier = 'AA' LIMIT 1",
 "LEFT/RIGHT":         f"SELECT LEFT(Carrier, 1) AS a, RIGHT(Carrier, 1) AS b FROM {T} WHERE Carrier = 'AA' LIMIT 1",
 "SPLIT":              f"SELECT SPLIT(Carrier, 'A') AS y FROM {T} WHERE Carrier = 'AA' LIMIT 1",
 "REPLACE":            f"SELECT REPLACE(Carrier, 'A', 'B') AS y FROM {T} WHERE Carrier = 'AA' LIMIT 1",
 "ROUND 2":            f"SELECT ROUND(1.234, 2) AS y FROM {T} LIMIT 1",
 "TRIM":               f"SELECT TRIM(Carrier) AS y FROM {T} WHERE Carrier = 'AA' LIMIT 1",
 "LTRIM":              f"SELECT LTRIM(Carrier) AS y FROM {T} WHERE Carrier = 'AA' LIMIT 1",
 "LPAD":               f"SELECT LPAD(Carrier, 4, '-') AS y FROM {T} WHERE Carrier = 'AA' LIMIT 1",
 "STRING_TO_ARRAY":    f"SELECT STRING_TO_ARRAY('a,b', ',') AS y FROM {T} LIMIT 1",
 "REGEXP_LIKE 3":      f"SELECT Carrier FROM {T} WHERE REGEXP_LIKE(Carrier, '^a', 'i') LIMIT 1",
 "MD5/SHA":            f"SELECT MD5(TOUTF8(Carrier)) AS a, SHA(TOUTF8(Carrier)) AS b FROM {T} WHERE Carrier = 'AA' LIMIT 1",
 "TO_BASE64":          f"SELECT TO_BASE64(TOUTF8(Carrier)) AS y FROM {T} WHERE Carrier = 'AA' LIMIT 1",
 "NULLIF":             f"SELECT NULLIF(ArrDelay, 13) AS y FROM {T} WHERE Carrier = 'AA' AND ArrDelay = 13 LIMIT 1",
 "ROW_NUMBER":         f"SELECT Carrier, ROW_NUMBER() OVER (ORDER BY ArrDelay) AS y FROM {T} WHERE Carrier = 'AA' AND ArrDelay = 13 LIMIT 1",
 "LAG":                f"SELECT Carrier, LAG(ArrDelay, 1) OVER (ORDER BY ArrDelay) AS y FROM {T} WHERE Carrier = 'AA' AND ArrDelay = 13 LIMIT 1",
 "EXTRACT":            f"SELECT EXTRACT(DAY FROM CAST(DaysSinceEpoch * 86400000 AS TIMESTAMP)) AS y FROM {T} WHERE DaysSinceEpoch = 16071 LIMIT 1",
 "CAST LONG":          f"SELECT CAST(ArrDelay AS LONG) AS y FROM {T} WHERE Carrier = 'AA' AND ArrDelay = 13 LIMIT 1",
 "CAST DOUBLE/FLOAT":  f"SELECT CAST(ArrDelay AS DOUBLE) AS a, CAST(ArrDelay AS FLOAT) AS b FROM {T} WHERE Carrier = 'AA' AND ArrDelay = 13 LIMIT 1",
 "CAST BIG_DECIMAL":   f"SELECT CAST(ArrDelay AS BIG_DECIMAL) AS y FROM {T} WHERE Carrier = 'AA' AND ArrDelay = 13 LIMIT 1",
 "CAST JSON (sse)":    f"SELECT CAST(ArrDelay AS JSON) AS y FROM {T} WHERE Carrier = 'AA' AND ArrDelay = 13 LIMIT 1",
 "CAST TIMESTAMP":     f"SELECT CAST(0 AS TIMESTAMP) AS y FROM {T} LIMIT 1",
 "CAST BOOLEAN":       f"SELECT CAST(1 AS BOOLEAN) AS y FROM {T} LIMIT 1",
}
print("| shape | rendered as | SSE raw | SSE rendered | MSE raw | MSE rendered | verdict |")
print("|---|---|---|---|---|---|---|")
for name, src in Q.items():
    try: rendered = two_part_sql(validate_query(src, DIALECT, 5))
    except Exception as e:
        print(f"| {name} | REJECTED by validate_query: {str(e)[:60]} | | | | | **rejects valid SQL** |"); continue
    raw = src.replace("pinot.default.", "")
    cells = []; verdict = "same"
    for mse in (False, True):
        rr = run(raw, mse); rn = run(rendered, mse)
        cells += [f"{rr[0]} {rr[1][:28]}", f"{rn[0]} {rn[1][:28]}"]
        if rr[0]=="OK" and rn[0]!="OK": verdict = "**BREAKS**"
        elif rr[0]=="OK" and rn[1]!=rr[1] and verdict!="**BREAKS**": verdict = "**DIFF**"
        elif rr[0]!="OK" and rn[0]=="OK" and verdict=="same": verdict = "fixes"
    r_expr = rendered[len("SELECT "):rendered.index(" FROM")]
    print(f"| {name} | `{r_expr[:60]}` | " + " | ".join(c.replace("|","/") for c in cells) + f" | {verdict} |")
