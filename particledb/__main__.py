"""``python -m particledb particles verify <path>`` — check a Particles module
locally, with no server: discovery, type mapping, shapes and options, the same
rules the server applies when the module is deployed.

Exit status 0 when every Particle passes, 1 otherwise. ``--json`` prints the
manifest the deploy would send.
"""
from __future__ import annotations

import json
import sys

from particledb import _collect

USAGE = "usage: python -m particledb particles verify <module-file|package-dir|dotted.module> [--json]"


def main(argv: list) -> int:
    args = [a for a in argv[1:] if a != "--json"]
    as_json = "--json" in argv
    if len(args) != 3 or args[0] != "particles" or args[1] != "verify":
        print(USAGE, file=sys.stderr)
        return 2
    target = args[2]
    try:
        manifest = _collect.collect(target)
        problems = _collect.verify(target)
    except _collect.CollectError as e:
        print("error: %s" % e, file=sys.stderr)
        return 1
    except Exception as e:  # noqa: BLE001 - surface import-time failures plainly
        print("error: while importing %s: %s: %s" % (target, type(e).__name__, e), file=sys.stderr)
        return 1
    if as_json:
        print(json.dumps(manifest, indent=2))
    else:
        for p in manifest["particles"]:
            args_sql = ", ".join(x["sql_type"] for x in p["params"])
            print("ok  @%-9s %s(%s) -> %s  [%s%s%s]" % (
                p["kind"], p["name"], args_sql, p["returns"]["sql_type"], p["volatility"],
                (", streaming" if p.get("streaming") else "")
                + (", called_on_null" if p["null_policy"] == "called_on_null" else ""),
                ", parallel unsafe" if not p["parallel_safe"] else ""))
    for msg in problems:
        print("FAIL %s" % msg, file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
