"""Particles SDK: the decorators, the collector and `python -m particledb particles verify`.

Server-free. Run from the repository root: `python -m pytest tests/test_particles.py -v`.
"""
from __future__ import annotations

import datetime
import decimal
import json
import os
import subprocess
import sys
import textwrap
from typing import List, Optional

import pytest

from particledb import _collect
from particledb.particles import (
    PARTICLE_ATTR,
    ParticleDefinitionError,
    aggregate,
    cast,
    scalar,
    window,
)
from particledb.particles import type as udt

SDK_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ---------------------------------------------------------------- decorators


def test_bare_and_called_forms_return_the_object_unchanged():
    def f(x: int) -> int:
        return x + 1

    assert scalar(f) is f and f(1) == 2
    g = scalar(volatility="stable")(lambda x: x)
    assert getattr(g, PARTICLE_ATTR)["volatility"] == "stable"


def test_only_written_options_carry_values():
    @scalar
    def f(x: int) -> int:
        return x

    meta = getattr(f, PARTICLE_ATTR)
    assert meta["kind"] == "scalar"
    for k in ("volatility", "null_policy", "parallel_safe", "runtime_backend", "timeout_ms", "name"):
        assert meta[k] is None, k
    assert meta["capabilities"] == []


def test_options_map_to_metadata():
    @scalar(name="sql_name", volatility="volatile", called_on_null=True, parallel_safe=False,
            backend="subprocess", timeout_ms=500)
    def f(x: int) -> int:
        return x

    meta = getattr(f, PARTICLE_ATTR)
    assert meta == {
        "kind": "scalar", "name": "sql_name", "volatility": "volatile",
        "null_policy": "called_on_null", "parallel_safe": False,
        "runtime_backend": "subprocess", "capabilities": [], "timeout_ms": 500,
    }
    assert getattr(scalar(called_on_null=False)(lambda x: x), PARTICLE_ATTR)["null_policy"] == "strict"


def test_positional_name():
    @scalar("other")
    def f(x: int) -> int:
        return x

    assert getattr(f, PARTICLE_ATTR)["name"] == "other"
    with pytest.raises(ParticleDefinitionError):
        scalar("a", name="b")


@pytest.mark.parametrize("opts", [
    {"colour": "red"},
    {"volatility": "sometimes"},
    {"called_on_null": "yes"},
    {"parallel_safe": 1},
    {"backend": "gpu"},
    {"capabilities": "network"},
    {"timeout_ms": 0},
    {"timeout_ms": True},
    {"name": "not an identifier"},
])
def test_invalid_options_raise(opts):
    with pytest.raises(ParticleDefinitionError):
        scalar(**opts)(lambda x: x)


def test_cast_options():
    @cast(source=str, target=dict)
    def parse(s: str) -> dict:
        return {}

    meta = getattr(parse, PARTICLE_ATTR)
    assert meta["source"] == "str" and meta["target"] == "dict"
    with pytest.raises(ParticleDefinitionError):
        cast(called_on_null=True)(lambda s: s)
    with pytest.raises(ParticleDefinitionError):
        scalar(source=str)(lambda s: s)


def test_streaming_aggregate_class_must_define_all_four_methods():
    @aggregate
    class Sum:
        def initial(self):
            return 0

        def accumulate(self, st, x: int):
            return st + x

        def merge(self, a, b):
            return a + b

        def finalize(self, st) -> int:
            return st

    assert getattr(Sum, PARTICLE_ATTR)["kind"] == "aggregate"
    with pytest.raises(ParticleDefinitionError, match="merge"):
        @aggregate
        class Broken:
            def initial(self):
                return 0

            def accumulate(self, st, x):
                return st

            def finalize(self, st):
                return st


def test_a_class_is_refused_outside_aggregate_and_type():
    with pytest.raises(ParticleDefinitionError):
        @scalar
        class C:
            pass

    @udt
    class Point:
        x: float

    assert getattr(Point, PARTICLE_ATTR)["kind"] == "type"


# ---------------------------------------------------------------- collector


def write_module(tmp_path, src, name="mod"):
    p = tmp_path / ("%s.py" % name)
    p.write_text(textwrap.dedent(src))
    return str(p)


def collect(tmp_path, src):
    return _collect.collect(write_module(tmp_path, src))["particles"]


def test_manifest_records_the_installed_sdk_version(tmp_path):
    from particledb import __version__

    result = _collect.collect(write_module(tmp_path, """
        from particledb import scalar
        @scalar
        def increment(value: int) -> int:
            return value + 1
    """))
    assert result["sdk"]["sdk_version"] == __version__


def test_hints_map_to_sql_types(tmp_path):
    (p,) = collect(tmp_path, """
        import datetime, decimal
        from typing import List, Optional
        from particledb import scalar
        @scalar
        def f(a: int, b: float, c: decimal.Decimal, d: str, e: bytes, f2: datetime.datetime,
              g: datetime.date, h: bool, i: List[int], j: dict, k: Optional[str]) -> str:
            return d
    """)
    assert [x["sql_type"] for x in p["params"]] == [
        "bigint", "double precision", "numeric", "text", "bytea", "timestamp",
        "date", "boolean", "bigint[]", "jsonb", "text",
    ]
    assert p["returns"]["sql_type"] == "text"
    assert p["volatility"] == "immutable" and p["null_policy"] == "strict"
    assert p["parallel_safe"] is True and p["runtime_backend"] == "auto"


def test_unhinted_or_unmappable_parameters_are_errors(tmp_path):
    with pytest.raises(_collect.CollectError, match="no type hint"):
        collect(tmp_path, """
            from particledb import scalar
            @scalar
            def f(a) -> int:
                return 1
        """)
    with pytest.raises(_collect.CollectError, match="no SQL type"):
        collect(tmp_path, """
            from particledb import scalar
            @scalar
            def f(a: set) -> int:
                return 1
        """)


def test_streaming_aggregate_signature_comes_from_accumulate_and_finalize(tmp_path):
    (p,) = collect(tmp_path, """
        from particledb import aggregate
        @aggregate
        class GeoMean:
            def initial(self): return (0.0, 0)
            def accumulate(self, st, x: float): return st
            def merge(self, a, b): return a
            def finalize(self, st) -> float: return 1.0
    """)
    assert p["kind"] == "aggregate" and p["streaming"] is True
    assert [x["sql_type"] for x in p["params"]] == ["double precision"]
    assert p["returns"]["sql_type"] == "double precision"


def test_window_returns_its_element_type(tmp_path):
    (p,) = collect(tmp_path, """
        from typing import List
        from particledb import window
        @window
        def ranks(xs: List[float]) -> List[int]:
            return [1] * len(xs)
    """)
    assert p["params"][0]["sql_type"] == "double precision[]"
    assert p["returns"]["sql_type"] == "bigint"
    with pytest.raises(_collect.CollectError, match="must return a list"):
        collect(tmp_path, """
            from typing import List
            from particledb import window
            @window
            def bad(xs: List[float]) -> float:
                return 1.0
        """)


def test_shapes_and_reserved_kinds(tmp_path):
    with pytest.raises(_collect.CollectError, match="EXACTLY ONE"):
        collect(tmp_path, """
            from particledb import cast
            @cast
            def c(a: str, b: str) -> str:
                return a
        """)
    with pytest.raises(_collect.CollectError, match="reserved"):
        collect(tmp_path, """
            from particledb import type as udt
            @udt
            class P:
                x: float
        """)
    with pytest.raises(_collect.CollectError, match="disagrees"):
        collect(tmp_path, """
            from particledb import cast
            @cast(source=int, target=str)
            def c(a: str) -> str:
                return a
        """)


def test_verify_reports_refused_options(tmp_path):
    path = write_module(tmp_path, """
        from particledb import scalar
        @scalar(capabilities=["network"], backend="pyo3")
        def f(a: int) -> int:
            return a
    """)
    problems = _collect.verify(path)
    assert any("capabilities" in p for p in problems)
    assert any("pyo3" in p for p in problems)


def run_cli(*args):
    env = dict(os.environ, PYTHONPATH=SDK_ROOT)
    return subprocess.run([sys.executable, "-m", "particledb", *args], capture_output=True, text=True, env=env)


def test_verify_cli_passes_a_good_module_and_fails_a_bad_one(tmp_path):
    good = write_module(tmp_path, """
        from particledb import scalar
        @scalar
        def double(x: int) -> int:
            return x * 2
    """, name="good")
    r = run_cli("particles", "verify", good)
    assert r.returncode == 0, r.stderr
    assert "double(bigint) -> bigint" in r.stdout
    r = run_cli("particles", "verify", good, "--json")
    assert json.loads(r.stdout)["particles"][0]["name"] == "double"
    bad = write_module(tmp_path, """
        from particledb import scalar
        @scalar
        def nohint(x):
            return x
    """, name="bad")
    r = run_cli("particles", "verify", bad)
    assert r.returncode == 1 and "no type hint" in r.stderr
    assert run_cli("particles").returncode == 2


def test_examples_directory_verifies():
    examples = os.path.join(os.path.dirname(os.path.dirname(SDK_ROOT)), "examples", "particles")
    if not os.path.isdir(examples):
        pytest.skip("examples/particles not present")
    r = run_cli("particles", "verify", examples)
    assert r.returncode == 0, r.stdout + r.stderr
