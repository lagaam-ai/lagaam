"""Native Pinot adapter for the QueryEngine port."""

# Core parses with the card's dialect string, so the Pinot dialect class must
# be registered before any parse — importing the module is what registers it.
from lagaam.adapters.pinot import dialect as dialect

__all__ = ["dialect"]
