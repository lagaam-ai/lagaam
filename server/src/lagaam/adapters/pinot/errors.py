"""Pinot errorCode plus message prefix to a core hint code.

The code alone is not enough. The same logical error carries different codes
per engine — a bad column is 710 on the single-stage engine and 700 on the
multi-stage one — and 700 folds bad column, bad function and unsupported DML
together, so the message prefix decides between them. The message itself never
travels further than this function: it names broker and server IPs, ports and
request ids, and the agent gets core's curated hint instead.
"""

# The multi-stage engine reports an unknown column as a column that "depends
# on itself" — measured on 1.5.1; without this it would read as unsupported.
_MSE_UNKNOWN_COLUMN = "depends on itself"

_UNKNOWN = "PINOT_UNKNOWN"


def classify(error_code: int, message: str) -> str:
    """The core hint code for one Pinot exceptions[] entry.

    Returns a code core does not know when nothing matches, so
    is_self_correctable reads it as an engine fault rather than the
    agent's fault — a query is never blamed for a failure we cannot name.
    """
    if error_code == 150:
        return "SYNTAX_ERROR"
    if error_code == 190:
        return "TABLE_NOT_FOUND"
    if error_code == 710:
        return "COLUMN_NOT_FOUND"
    if error_code == 245:
        return "EXCEEDED_ROW_LIMIT"
    if error_code in (400, 427):
        return "EXCEEDED_TIME_LIMIT"
    if error_code == 503:
        return "RESPONSE_TOO_LARGE"
    if error_code == 700:
        if "UnknownColumnError" in message or _MSE_UNKNOWN_COLUMN in message:
            return "COLUMN_NOT_FOUND"
        if "Unsupported function" in message or "No match found for function" in message:
            return "FUNCTION_NOT_FOUND"
        return "NOT_SUPPORTED"
    return _UNKNOWN
