"""Turning engine execution failures into hints an agent can act on.

Raw engine error codes (COLUMN_NOT_FOUND, EXCEEDED_TIME_LIMIT, ...) mean
nothing to an agent. The verify layer maps each to a next action — describe
the table, add a filter, fix the syntax — so a failed query becomes a
recoverable one.
"""

from lagaam.core.query_errors import hint_for_engine_error


def test_column_not_found_points_at_describe_table() -> None:
    hint = hint_for_engine_error("COLUMN_NOT_FOUND")
    assert "describe_table" in hint


def test_table_not_found_points_at_list_catalogs() -> None:
    hint = hint_for_engine_error("TABLE_NOT_FOUND")
    assert "list_catalogs" in hint or "describe_table" in hint


def test_memory_limit_suggests_shrinking_the_query() -> None:
    hint = hint_for_engine_error("EXCEEDED_GLOBAL_MEMORY_LIMIT")
    assert "filter" in hint.lower() or "aggregat" in hint.lower()


def test_time_limit_suggests_narrowing() -> None:
    hint = hint_for_engine_error("EXCEEDED_TIME_LIMIT")
    assert "filter" in hint.lower() or "narrow" in hint.lower()


def test_syntax_error_points_at_the_dialect() -> None:
    hint = hint_for_engine_error("SYNTAX_ERROR")
    assert "dialect" in hint.lower() or "syntax" in hint.lower()


def test_permission_denied_is_explained() -> None:
    hint = hint_for_engine_error("PERMISSION_DENIED")
    assert "permission" in hint.lower() or "access" in hint.lower()


def test_unknown_error_gets_a_generic_retry_hint() -> None:
    # An unmapped code still returns something actionable, never None.
    hint = hint_for_engine_error("SOME_NEW_TRINO_CODE")
    assert hint
    assert "retry" in hint.lower() or "report" in hint.lower()


def test_lookup_is_case_insensitive() -> None:
    assert hint_for_engine_error("column_not_found") == hint_for_engine_error(
        "COLUMN_NOT_FOUND"
    )


def test_is_self_correctable_recognizes_mapped_codes() -> None:
    from lagaam.core.query_errors import is_self_correctable

    assert is_self_correctable("EXCEEDED_TIME_LIMIT")
    assert is_self_correctable("column_not_found")  # case-insensitive
    assert not is_self_correctable("SOME_INTERNAL_FAULT")
    assert not is_self_correctable(None)
