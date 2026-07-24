"""Parsing Trino's EXPLAIN (TYPE IO, FORMAT JSON) into a CostEstimate.

The JSON shape is pinned per Trino version and not guaranteed stable, so the
parser is deliberately defensive: missing or NaN estimates degrade to low
confidence rather than throwing or inventing a number.
"""

import json

from lagaam.adapters.trino.explain import parse_io_estimate

# Trimmed but real shape of EXPLAIN (TYPE IO, FORMAT JSON) on Trino 476.
_IO_WITH_STATS = json.dumps(
    {
        "inputTableColumnInfos": [
            {
                "table": {"catalog": "tpch", "schemaTable": {"schema": "tiny",
                          "table": "orders"}},
                "columnConstraints": [],
                "estimate": {
                    "outputRowCount": 15000.0,
                    "outputSizeInBytes": 1_800_000.0,
                },
            }
        ],
        "estimate": {
            "outputRowCount": 15000.0,
            "outputSizeInBytes": 1_800_000.0,
        },
    }
)

_IO_NO_STATS = json.dumps(
    {
        "inputTableColumnInfos": [
            {
                "table": {"catalog": "memory", "schemaTable": {"schema": "s",
                          "table": "t"}},
                "columnConstraints": [],
                "estimate": {
                    "outputRowCount": "NaN",
                    "outputSizeInBytes": "NaN",
                },
            }
        ],
        "estimate": {"outputRowCount": "NaN", "outputSizeInBytes": "NaN"},
    }
)


def test_estimate_with_stats_is_high_confidence() -> None:
    est = parse_io_estimate(_IO_WITH_STATS)
    assert est.confidence == "high"
    assert est.scanned_bytes == 1_800_000
    assert est.row_estimate == 15000


def test_nan_estimate_degrades_to_low_confidence() -> None:
    est = parse_io_estimate(_IO_NO_STATS)
    assert est.confidence == "low"
    assert est.scanned_bytes is None


def test_sums_bytes_across_multiple_scanned_tables() -> None:
    # A join reads two tables; the quotation is their combined scan.
    payload = json.dumps(
        {
            "inputTableColumnInfos": [
                {"estimate": {"outputRowCount": 100.0,
                              "outputSizeInBytes": 1000.0}},
                {"estimate": {"outputRowCount": 200.0,
                              "outputSizeInBytes": 2000.0}},
            ],
            "estimate": {"outputRowCount": 300.0, "outputSizeInBytes": 3000.0},
        }
    )
    est = parse_io_estimate(payload)
    assert est.scanned_bytes == 3000
    assert est.confidence == "high"


def test_one_missing_table_estimate_taints_the_whole_quote() -> None:
    # If any scanned table lacks stats, the total is untrustworthy -> low.
    payload = json.dumps(
        {
            "inputTableColumnInfos": [
                {"estimate": {"outputRowCount": 100.0,
                              "outputSizeInBytes": 1000.0}},
                {"estimate": {"outputRowCount": "NaN",
                              "outputSizeInBytes": "NaN"}},
            ],
            "estimate": {"outputRowCount": "NaN", "outputSizeInBytes": "NaN"},
        }
    )
    est = parse_io_estimate(payload)
    assert est.confidence == "low"


def test_empty_or_malformed_json_fails_safe() -> None:
    assert parse_io_estimate("{}").confidence == "low"
    assert parse_io_estimate("not json").confidence == "low"


def test_no_input_tables_is_low_confidence_not_a_crash() -> None:
    # SELECT 1 scans nothing; low confidence is the safe read, and it must
    # not throw. (Real Trino emits inputTableColumnInfos: [].)
    est = parse_io_estimate(json.dumps({"inputTableColumnInfos": []}))
    assert est.confidence == "low"
    assert est.scanned_bytes is None


def test_infinity_is_rejected_not_summed_into_a_giant_number() -> None:
    # An unbounded cost renders as "Infinity"; it must taint, not slip through
    # (round(inf) would crash, and inf==inf would look "finite").
    for bad in ("Infinity", "-Infinity"):
        payload = json.dumps(
            {
                "inputTableColumnInfos": [
                    {"estimate": {"outputRowCount": bad,
                                  "outputSizeInBytes": bad}}
                ]
            }
        )
        assert parse_io_estimate(payload).confidence == "low"


def test_columnless_scan_is_not_quoted_as_zero_bytes() -> None:
    # count(*) / SELECT 1 project no columns: Trino reports 0 bytes for a full
    # scan of 1.5M rows. A 0-byte high-confidence quote would let an expensive
    # aggregate slip past the budget — must degrade to low.
    payload = json.dumps(
        {
            "inputTableColumnInfos": [
                {"estimate": {"outputRowCount": 1_500_000.0,
                              "outputSizeInBytes": 0.0}}
            ]
        }
    )
    est = parse_io_estimate(payload)
    assert est.confidence == "low"
    assert est.scanned_bytes is None


def test_zero_bytes_is_never_trustworthy() -> None:
    # 0 rows AND 0 bytes reads as an honest empty table, but a stats-less
    # connector reports the same shape for a full scan. Blocking a truly
    # empty table is recoverable; clearing an unpriced scan is not.
    payload = json.dumps(
        {
            "inputTableColumnInfos": [
                {"estimate": {"outputRowCount": 0.0, "outputSizeInBytes": 0.0}}
            ]
        }
    )
    est = parse_io_estimate(payload)
    assert est.confidence == "low"
    assert est.scanned_bytes is None


def test_negative_size_is_rejected() -> None:
    payload = json.dumps(
        {
            "inputTableColumnInfos": [
                {"estimate": {"outputRowCount": -1.0,
                              "outputSizeInBytes": -1.0}}
            ]
        }
    )
    assert parse_io_estimate(payload).confidence == "low"


def test_null_estimate_member_fails_safe() -> None:
    # A JSON null is valid JSON with the key present, so it survives the
    # KeyError guard — .get() returns None, not the default.
    payload = json.dumps({"inputTableColumnInfos": [{"estimate": None}]})
    est = parse_io_estimate(payload)
    assert est.confidence == "low"
    assert est.scanned_bytes is None


def test_null_table_list_fails_safe() -> None:
    payload = json.dumps({"inputTableColumnInfos": None})
    est = parse_io_estimate(payload)
    assert est.confidence == "low"


def test_non_dict_table_list_fails_safe() -> None:
    payload = json.dumps({"inputTableColumnInfos": "not a list"})
    est = parse_io_estimate(payload)
    assert est.confidence == "low"
