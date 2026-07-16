"""Cost helpers shared by the estimate model and the budget gate.

Kept tiny and engine-agnostic: adapters produce the raw byte numbers,
core turns them into words an agent can act on.
"""

_UNITS = ("B", "KB", "MB", "GB", "TB", "PB")


def human_bytes(n: int) -> str:
    """Render a byte count the way a budget message should read: '48.0 GB'."""
    if n < 1024:
        return f"{n} B"
    size = float(n)
    for unit in _UNITS[1:]:
        size /= 1024
        if size < 1024 or unit == _UNITS[-1]:
            return f"{size:.1f} {unit}"
    return f"{size:.1f} {_UNITS[-1]}"
