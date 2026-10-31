"""Particles — the deploy collector (DISCOVER + MANIFEST stages).

This is the single source of the deploy manifest for BOTH front doors:
  - the ``pdb particles deploy`` CLI (client-side, on the developer's machine), and
  - the ``--python-functions`` server startup loader (in-process at boot).

Both run this script under ``python3``; the Rust side never imports Python
itself, it drives the interpreter (exactly like the subprocess ParticleRuntime
backend). The script imports the target module(s), walks ``@``-decorated
Particles (the ``__pdb_particle__`` metadata attached by
:mod:`particledb.particles`), reads each signature from the Python type hints,
maps hints to SQL types, and emits the manifest JSON on stdout.

Usage::

    python3 -m particledb._collect ./particles
    python3 -m particledb._collect geo.distance          # dotted module name

Output: the manifest JSON on stdout, or ``{"error": "..."}`` + a non-zero exit
on a discovery/typing failure — with a plain, source-located message (§6.1) so
the developer fixes their module, not the DB.

NOTE (skeleton): PACKAGE / SIGN / STORE (artifact digest, dependency lock,
signature) are done by the Rust CLI around this script — this script only
produces stages 1–2 (discover + the per-particle manifest entries). The Rust
side fills in ``package`` (sha256, lock, signature).
"""
from __future__ import annotations

import importlib
import importlib.util
import inspect
import json
import os
import sys
import types
import typing
from typing import Any

SDK_VERSION = "0.2.1"
ABI = "particles-1"
PARTICLE_ATTR = "__pdb_particle__"

# Python type-hint → SQL type. The single mapping table (must match the doc's
# PARTICLES_ANNOTATIONS.md table and the Rust ScalarValue marshaling contract).
_SCALAR_HINT_TO_SQL = {
    "bool": "boolean",
    "int": "bigint",
    "float": "double precision",
    "str": "text",
    "bytes": "bytea",
    "Decimal": "numeric",
    "datetime": "timestamp",
    "date": "date",
    "NoneType": "void",
}


class CollectError(Exception):
    """A discovery/typing failure with a plain, actionable message."""


def _sql_type_for_hint(hint: Any, where: str) -> str:
    """Map one Python type hint to its SQL type, or raise CollectError."""
    if hint is inspect.Parameter.empty:
        raise CollectError(
            "%s has no type hint. Add one, e.g. 'x: str'. "
            "Your type hints ARE the SQL signature." % where
        )
    origin = typing.get_origin(hint)
    if origin is typing.Union or (
        getattr(types, "UnionType", None) is not None and origin is getattr(types, "UnionType")
    ):
        # Optional[X] / X | None: NULL is always allowed in SQL, so the SQL type is X's.
        args = [a for a in typing.get_args(hint) if a is not type(None)]
        if len(args) == 1:
            return _sql_type_for_hint(args[0], where)
        raise CollectError("%s: a union of several types has no single SQL type" % where)
    if origin in (dict, typing.Dict) or hint is dict:
        return "jsonb"
    if origin in (list, typing.List):  # list[...] → array
        (inner,) = typing.get_args(hint) or (str,)
        return _sql_type_for_hint(inner, where) + "[]"
    name = getattr(hint, "__name__", str(hint))
    if name in _SCALAR_HINT_TO_SQL:
        return _SCALAR_HINT_TO_SQL[name]
    raise CollectError(
        "%s — no SQL type maps to Python '%s'. Supported: "
        "str, int, float, Decimal, bytes, datetime, date, bool, dict (jsonb), list[...]."
        % (where, name)
    )


DEFAULTS = {
    "volatility": "immutable",
    "null_policy": "strict",
    "parallel_safe": True,
    "runtime_backend": "auto",
}


def _signature_of(obj: Any, kind: str):
    """The callable whose hints ARE the SQL signature, and how many leading
    parameters to skip: a function itself; a streaming @aggregate class's
    ``accumulate(self, state, value)`` (skip 2) with ``finalize``'s return."""
    if inspect.isclass(obj):
        if kind != "aggregate":
            raise CollectError(
                "%s: only a streaming @aggregate may be a class" % obj.__name__
            )
        return obj.accumulate, 2, obj.finalize
    return obj, 0, obj


def _entry_for(obj: Any, module_name: str) -> dict:
    """Build one manifest particle entry from a decorated function/class."""
    meta = getattr(obj, PARTICLE_ATTR)
    kind = meta["kind"]
    name = meta.get("name") or obj.__name__
    handler = "%s:%s" % (module_name, obj.__name__)
    if kind == "type":
        raise CollectError(
            "%s: @type is reserved and not available in this release; remove the "
            "decorator or keep the class undecorated" % handler
        )

    target, skip, ret_target = _signature_of(obj, kind)
    sig = inspect.signature(target)
    try:
        hints = typing.get_type_hints(target)
        ret_hints = typing.get_type_hints(ret_target)
    except Exception as e:  # noqa: BLE001
        raise CollectError("while reading type hints for %s: %s" % (handler, e))

    params = []
    for i, (pname, p) in enumerate(sig.parameters.items()):
        if i < skip:
            continue
        if p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD):
            raise CollectError("%s(%s): *args / **kwargs cannot map to SQL parameters" % (name, pname))
        hint = hints.get(pname, p.annotation)
        sql = _sql_type_for_hint(hint, "%s(%s)" % (name, pname))
        params.append(
            {"name": pname, "sql_type": sql, "py_hint": getattr(hint, "__name__", str(hint))}
        )

    ret_hint = ret_hints.get("return", inspect.signature(ret_target).return_annotation)
    ret_sql = _sql_type_for_hint(ret_hint, "%s return" % name)
    if ret_sql == "void":
        raise CollectError("%s must return a value (a `-> None` hint has no SQL type)" % name)

    # Shape rules (the server enforces the same ones at registration).
    if kind == "cast" and len(params) != 1:
        raise CollectError(
            "@cast %s must take EXACTLY ONE parameter (the source value); got %d"
            % (name, len(params))
        )
    if kind in ("aggregate", "window") and len(params) != 1:
        raise CollectError(
            "@%s %s must take exactly one value parameter (the %s); got %d"
            % (kind, name, "list of values" if not inspect.isclass(obj) else "value", len(params))
        )
    if kind == "window" and not ret_sql.endswith("[]"):
        raise CollectError(
            "@window %s must return a list (one value per row), e.g. `-> list[float]`" % name
        )
    if kind == "window":
        # A window returns one element per row: the SQL result is the element type.
        ret_sql = ret_sql[:-2]
    if kind == "cast":
        for k, val in (("source", params[0]["py_hint"]), ("target", getattr(ret_hint, "__name__", ""))):
            want = meta.get(k)
            if want is not None and want != val:
                raise CollectError(
                    "@cast %s: %s=%s disagrees with the signature (%s)" % (name, k, want, val)
                )

    def opt(key):
        v = meta.get(key)
        return DEFAULTS[key] if v is None else v

    return {
        "name": name,
        "kind": kind,
        "handler": handler,
        "params": params,
        "returns": {"sql_type": ret_sql, "py_hint": getattr(ret_hint, "__name__", str(ret_hint))},
        "volatility": opt("volatility"),
        "null_policy": opt("null_policy"),
        "parallel_safe": opt("parallel_safe"),
        "runtime_backend": opt("runtime_backend"),
        "capabilities": meta.get("capabilities") or [],
        "streaming": inspect.isclass(obj),
        "timeout_ms": meta.get("timeout_ms"),
        "cost": {"fixed": 1, "per_row": 1, "max_output_ratio": 1},
    }


def _iter_modules(target: str):
    """Yield (module_name, module) for a file, a package dir, or a dotted name."""
    if os.path.isdir(target):
        sys.path.insert(0, target)
        for fname in sorted(os.listdir(target)):
            if fname.endswith(".py") and not fname.startswith("_"):
                mod_name = fname[:-3]
                yield mod_name, importlib.import_module(mod_name)
    elif os.path.isfile(target):
        mod_name = os.path.splitext(os.path.basename(target))[0]
        spec = importlib.util.spec_from_file_location(mod_name, target)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[mod_name] = mod
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        yield mod_name, mod
    else:  # dotted module name
        yield target, importlib.import_module(target)


def collect(target: str) -> dict:
    """Discover Particles under ``target`` and return the (partial) manifest.

    The ``package`` block is left for the Rust CLI to fill (sha256, dependency
    lock, signature). ``particles`` is fully populated here.
    """
    particles = []
    for mod_name, mod in _iter_modules(target):
        for _, obj in inspect.getmembers(mod):
            if (inspect.isfunction(obj) or inspect.isclass(obj)) and hasattr(
                obj, PARTICLE_ATTR
            ):
                # Only collect objects DEFINED in this module (not imported
                # decorated names re-exported from elsewhere).
                if getattr(obj, "__module__", None) not in (mod_name, None):
                    continue
                particles.append(_entry_for(obj, mod_name))

    return {
        "manifest_version": 1,
        "sdk": {"language": "python", "sdk_version": SDK_VERSION, "abi": ABI},
        # Rust fills package.{artifact,sha256,dependency_lock,signature}.
        "package": {"artifact": "", "sha256": "", "dependency_lock": [], "signature": None},
        "particles": particles,
    }


def verify(target: str) -> list:
    """Check a module the way the server will, without a server: discovery,
    type mapping, shapes and options. Returns a list of problems (empty = OK).
    Capabilities and non-subprocess backends are reported because this server
    release refuses them."""
    problems = []
    manifest = collect(target)
    if not manifest["particles"]:
        problems.append("no decorated Particles found in %s" % target)
    for p in manifest["particles"]:
        where = "%s (%s)" % (p["name"], p["handler"])
        if p["capabilities"]:
            problems.append("%s: capabilities %s are refused (deny-by-default)" % (where, p["capabilities"]))
        if p["runtime_backend"] not in ("auto", "subprocess"):
            problems.append("%s: backend %r is not available in this release" % (where, p["runtime_backend"]))
        if p["kind"] == "cast" and (
            p["volatility"] != "immutable" or not p["parallel_safe"] or p["null_policy"] != "strict"
        ):
            problems.append("%s: a @cast must be immutable, strict and parallel-safe" % where)
    return problems


def main(argv: list) -> int:
    if len(argv) != 2:
        json.dump({"error": "usage: python3 -m particledb._collect <path-or-module>"}, sys.stdout)
        return 2
    try:
        manifest = collect(argv[1])
    except CollectError as e:
        json.dump({"error": str(e)}, sys.stdout)
        return 1
    except Exception as e:  # noqa: BLE001 - frame import-time failures for the dev
        json.dump({"error": "while importing %s: %s: %s" % (argv[1], type(e).__name__, e)}, sys.stdout)
        return 1
    json.dump(manifest, sys.stdout)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
