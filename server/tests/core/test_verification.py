"""Result verification: warn the agent about results it might misread.

A query can succeed yet still mislead — zero rows that look like "no data",
a truncated page treated as the whole answer, a column that's entirely NULL.
These warnings ride along with the result so the agent double-checks before
drawing a conclusion. They never block; they inform.
"""

from lagaam.core.models import QueryResult
from lagaam.core.verification import verify_result


def test_clean_result_has_no_warnings() -> None:
    result = QueryResult(
        columns=["orderkey", "total"],
        rows=[[1, 10.0], [2, 20.0]],
        row_count=2,
    )
    assert verify_result(result) == []


def test_empty_result_is_flagged() -> None:
    result = QueryResult(columns=["orderkey"], rows=[], row_count=0)
    warnings = verify_result(result)
    assert any("no rows" in w.lower() for w in warnings)


def test_truncated_result_is_flagged() -> None:
    result = QueryResult(
        columns=["orderkey"],
        rows=[[i] for i in range(5)],
        row_count=5,
        truncated=True,
    )
    warnings = verify_result(result)
    assert any("truncat" in w.lower() or "more rows" in w.lower() for w in warnings)


def test_all_null_column_is_flagged_by_name() -> None:
    result = QueryResult(
        columns=["orderkey", "discount"],
        rows=[[1, None], [2, None], [3, None]],
        row_count=3,
    )
    warnings = verify_result(result)
    assert any("discount" in w for w in warnings)


def test_partially_null_column_is_not_flagged() -> None:
    result = QueryResult(
        columns=["orderkey", "discount"],
        rows=[[1, None], [2, 0.5]],
        row_count=2,
    )
    assert verify_result(result) == []


def test_empty_result_does_not_also_flag_null_columns() -> None:
    # No rows means every column is trivially "all null" — don't double-warn.
    result = QueryResult(columns=["a", "b"], rows=[], row_count=0)
    warnings = verify_result(result)
    assert len(warnings) == 1
    assert "no rows" in warnings[0].lower()


def test_truncated_result_does_not_claim_a_column_is_all_null() -> None:
    # We only see one page; a column null here may have values in unseen rows.
    result = QueryResult(
        columns=["orderkey", "discount"],
        rows=[[1, None], [2, None]],
        row_count=2,
        truncated=True,
    )
    warnings = verify_result(result)
    assert all("discount" not in w for w in warnings)
    assert any("more rows" in w.lower() for w in warnings)


def test_ragged_rows_do_not_crash_verification() -> None:
    # verify_result is pure; a malformed row must not take down the response.
    result = QueryResult(columns=["a", "b"], rows=[[1]], row_count=1)
    assert verify_result(result) == []
