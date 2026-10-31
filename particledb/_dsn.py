"""Shared URL syntax validation for SDK connection entry points."""
from urllib.parse import SplitResult, urlsplit


def parse_connection_url(dsn: str) -> SplitResult:
    """Return parsed parts with a validated port; callers select allowed schemes.

    Invalid syntax raises a static ValueError without chaining the original
    parser exception.
    """
    try:
        parts = urlsplit(dsn)
        _ = parts.port
    except ValueError:
        pass
    else:
        return parts
    raise ValueError("invalid connection URL") from None
