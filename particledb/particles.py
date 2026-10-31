"""Particles — the authoring decorators (``@scalar`` / ``@aggregate`` /
``@window`` / ``@cast`` / ``@type``).

You write ordinary, unit-testable Python and mark each function with the SQL
concept it exposes. The decorators attach a ``__pdb_particle__`` metadata dict
and return your object unchanged, so the module stays plain Python you can
import and test::

    from particledb import scalar

    @scalar
    def domain_of(email: str) -> str:
        return email.split("@")[-1]

Check a module locally, then deploy it::

    python -m particledb particles verify ./particles
    particledb particles deploy ./particles --target 127.0.0.1:5432

and call it from SQL: ``SELECT domain_of(email) FROM users;``

What runs (the server checks the same rules; this module runs inside every
server worker, so the validation below is the server's validation too):

``@scalar``
    One row in, one value out. A plain function.
``@aggregate``
    Many rows in, one value out. Either a function taking the group as a
    ``list`` (collect mode), or a class with ``initial()``,
    ``accumulate(state, value)``, ``merge(a, b)`` and ``finalize(state)``
    (streaming: the group is never materialized, and partial states are merged
    for parallel plans).
``@window``
    A function taking the partition's values as a ``list`` (in the window's
    ORDER BY order) and returning a list of the same length.
``@cast``
    A one-argument conversion ``(source) -> target`` that the server uses for
    ``CAST(x AS target)`` and for implicit conversions between the two types.
``@type``
    Reserved: decorating works, the server refuses to register it (``0A000``).

Options (every one is enforced by the server; an unknown option raises here):

``name``           SQL name, when it differs from the Python name.
``volatility``     ``"immutable"`` (default), ``"stable"`` or ``"volatile"``.
``called_on_null`` ``False`` (default: a NULL argument returns NULL without
                   calling you; an aggregate skips NULLs) or ``True`` (you
                   receive ``None``).
``parallel_safe``  ``True`` (default) or ``False`` (calls are serialized
                   server-wide and never split across workers).
``backend``        ``"auto"`` (default) or ``"subprocess"``; ``"pyo3"`` and
                   ``"firecracker"`` are refused by this server version.
``capabilities``   Must be empty: capabilities are deny-by-default and none
                   can be granted by this server version.
``timeout_ms``     Per-call time budget (default: the server's,
                   ``--particles-call-timeout-ms``, 10000).
``source`` / ``target`` (``@cast`` only) the Python types converted between;
                   they must agree with the signature.
"""
from __future__ import annotations

import inspect
from typing import Any, Callable

__all__ = [
    "scalar",
    "aggregate",
    "window",
    "type",
    "cast",
    "PARTICLE_ATTR",
    "ParticleDefinitionError",
]

#: The attribute the decorators attach to a Particle. The collector keys on it.
PARTICLE_ATTR = "__pdb_particle__"

KINDS = ("scalar", "aggregate", "window", "cast", "type")
VOLATILITIES = ("immutable", "stable", "volatile")
BACKENDS = ("auto", "subprocess", "pyo3", "firecracker")
COMMON_OPTIONS = (
    "name",
    "volatility",
    "called_on_null",
    "parallel_safe",
    "backend",
    "capabilities",
    "timeout_ms",
)
KIND_OPTIONS = {"cast": ("source", "target")}
STREAM_METHODS = ("initial", "accumulate", "merge", "finalize")


class ParticleDefinitionError(TypeError):
    """A decorator was used with an option or on an object it cannot accept."""


def _check_options(kind: str, options: dict) -> None:
    allowed = COMMON_OPTIONS + KIND_OPTIONS.get(kind, ())
    unknown = sorted(set(options) - set(allowed))
    if unknown:
        raise ParticleDefinitionError(
            "@%s: unknown option(s) %s; allowed: %s"
            % (kind, ", ".join(unknown), ", ".join(allowed))
        )
    v = options.get("volatility")
    if v is not None and v not in VOLATILITIES:
        raise ParticleDefinitionError(
            "@%s: volatility must be one of %s, got %r" % (kind, ", ".join(VOLATILITIES), v)
        )
    for flag in ("called_on_null", "parallel_safe"):
        if flag in options and not isinstance(options[flag], bool):
            raise ParticleDefinitionError("@%s: %s must be True or False" % (kind, flag))
    b = options.get("backend")
    if b is not None and b not in BACKENDS:
        raise ParticleDefinitionError(
            "@%s: backend must be one of %s, got %r" % (kind, ", ".join(BACKENDS), b)
        )
    caps = options.get("capabilities")
    if caps is not None and (
        isinstance(caps, str) or not all(isinstance(c, str) for c in caps)
    ):
        raise ParticleDefinitionError("@%s: capabilities must be a list of strings" % kind)
    t = options.get("timeout_ms")
    if t is not None and (isinstance(t, bool) or not isinstance(t, int) or t <= 0):
        raise ParticleDefinitionError("@%s: timeout_ms must be a positive integer" % kind)
    n = options.get("name")
    if n is not None and (not isinstance(n, str) or not n.isidentifier()):
        raise ParticleDefinitionError("@%s: name must be a SQL identifier, got %r" % (kind, n))
    if kind == "cast" and options.get("called_on_null"):
        raise ParticleDefinitionError("@cast: a conversion is always strict (NULL stays NULL)")


def _check_target(kind: str, obj: Any) -> None:
    if inspect.isclass(obj):
        if kind == "aggregate":
            missing = [m for m in STREAM_METHODS if not callable(getattr(obj, m, None))]
            if missing:
                raise ParticleDefinitionError(
                    "@aggregate class %s must define %s (missing %s)"
                    % (obj.__name__, ", ".join(STREAM_METHODS), ", ".join(missing))
                )
            return
        if kind == "type":
            return
        raise ParticleDefinitionError(
            "@%s decorates a function; only @aggregate (streaming) and @type take a class" % kind
        )
    if not callable(obj):
        raise ParticleDefinitionError("@%s can only decorate a function or class" % kind)


def _mark(obj: Any, kind: str, options: dict) -> Any:
    """Attach the Particle metadata to ``obj`` and return it unchanged.

    Only options the author WROTE carry a value; the rest are ``None``, so the
    server can tell "declared volatile" from "left at the default" when it
    reconciles the decorator with SQL clauses such as ``IMMUTABLE``.
    """
    _check_options(kind, options)
    _check_target(kind, obj)
    called_on_null = options.get("called_on_null")
    meta = {
        "kind": kind,
        "name": options.get("name"),
        "volatility": options.get("volatility"),
        "null_policy": None
        if called_on_null is None
        else ("called_on_null" if called_on_null else "strict"),
        "parallel_safe": options.get("parallel_safe"),
        "runtime_backend": options.get("backend"),
        "capabilities": list(options.get("capabilities") or []),
        "timeout_ms": options.get("timeout_ms"),
    }
    if kind == "cast":
        for k in ("source", "target"):
            t = options.get(k)
            meta[k] = getattr(t, "__name__", None) if t is not None else None
    try:
        setattr(obj, PARTICLE_ATTR, meta)
    except (AttributeError, TypeError):
        raise ParticleDefinitionError(
            "@%s can only decorate a user-defined function or class" % kind
        )
    return obj


def _decorator(kind: str) -> Callable:
    """Build a decorator usable bare (``@scalar``) or called
    (``@scalar(volatility="stable")`` / ``@scalar("sql_name")``)."""

    def decorator(*args: Any, **kwargs: Any) -> Any:
        if len(args) == 1 and callable(args[0]) and not kwargs:
            return _mark(args[0], kind, {})
        if len(args) > 1:
            raise ParticleDefinitionError(
                "@%s takes at most one positional argument (the SQL name)" % kind
            )
        options = dict(kwargs)
        if args:
            if "name" in options:
                raise ParticleDefinitionError("@%s: name given twice" % kind)
            options["name"] = args[0]

        def apply(obj: Any) -> Any:
            return _mark(obj, kind, options)

        return apply

    decorator.__name__ = kind
    decorator.__qualname__ = kind
    decorator.__doc__ = "Mark a %s Particle. See the particledb.particles module docs." % kind
    return decorator


scalar = _decorator("scalar")
aggregate = _decorator("aggregate")
window = _decorator("window")
cast = _decorator("cast")
#: ``@type`` is reserved (refused by the server). Named ``type`` here; import
#: it ``as udt`` to avoid shadowing the builtin.
type = _decorator("type")  # noqa: A001 - deliberate SQL-vocabulary name
