#!/usr/bin/env bash
# run_conformance.sh -- run the ParticleDB SDK conformance suite against THIS
# SDK (package `particledb`) from source.
#
#   PDB_BIN=/path/to/particledb ./run_conformance.sh [--mode default|strict] [--results FILE]
#
# One command, self-contained: cases/ and lib/ are this bundle's copies of
# the shared SDK conformance suite (identical in every SDK repository). It
# starts a fresh server from PDB_BIN (unless PDB_CONF_PORT names a running one,
# in which case PDB_CONF_CONTROL must be set too), runs every case for the mode
# with the SDK imported from THIS checkout (PYTHONPATH; the adapter refuses to
# run if another installed `particledb` shadows it), and validates the results
# file (every case reported; every skip justified). Exit 0 only if no case
# failed and the results file is valid. PDB_PY=... picks the interpreter.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE="$(cd "$HERE/.." && pwd)"
MODE=default
RESULTS=""
while [ $# -gt 0 ]; do
  case "$1" in
    --mode) MODE=$2; shift 2 ;;
    --results) RESULTS=$2; shift 2 ;;
    -h|--help) sed -n '2,15p' "$0"; exit 0 ;;
    *) echo "unknown argument $1" >&2; exit 2 ;;
  esac
done
PY=${PDB_PY:-python3}

WORK=$(mktemp -d "${TMPDIR:-/tmp}/pdb-conf-python.XXXXXX")
[ -n "$RESULTS" ] || RESULTS="$WORK/results.json"
STARTED=0
cleanup() {
  if [ $STARTED -eq 1 ]; then "$HERE/lib/server.sh" stop "$WORK/server" || true; fi
}
trap cleanup EXIT INT TERM

if [ -z "${PDB_CONF_PORT:-}" ]; then
  : "${PDB_BIN:?set PDB_BIN to the particledb server binary (or PDB_CONF_PORT + PDB_CONF_CONTROL for a running one)}"
  "$HERE/lib/server.sh" start "$WORK/server" "$PDB_BIN" "$MODE" || exit 1
  STARTED=1
  eval "$("$HERE/lib/server.sh" env "$WORK/server")"
  printf '#!/bin/sh\nexec "%s" "$1" "%s"\n' "$HERE/lib/server.sh" "$WORK/server" > "$WORK/control"
  chmod +x "$WORK/control"
  export PDB_CONF_CONTROL="$WORK/control"
  echo "server: $PDB_BIN on native port $PDB_CONF_PORT (default port $PDB_CONF_DEFAULT_PORT), mode $MODE, log $WORK/server/server.log"
fi
: "${PDB_CONF_CONTROL:?PDB_CONF_CONTROL must be set when PDB_CONF_PORT names a running server}"
export PDB_CONF_MODE=$MODE PDB_CONF_CASES_DIR="$HERE/cases" PDB_CONF_RESULTS="$RESULTS" PDB_CONF_IDS="$WORK/ids"
export PDB_CONF_SDK_ROOT="$MODULE/particledb"
export PYTHONPATH="$MODULE${PYTHONPATH:+:$PYTHONPATH}"

"$PY" "$HERE/lib/conformance.py" ids "$HERE/cases" "$MODE" > "$WORK/ids" || exit 2
"$PY" -c 'import sys, particledb; print("sdk:", particledb.__file__, "python", sys.version.split()[0])'

"$PY" "$HERE/src/adapter.py"
rc=$?
"$PY" "$HERE/lib/conformance.py" validate "$HERE/cases" "$MODE" "$RESULTS" || rc=1
"$PY" "$HERE/lib/conformance.py" table "$HERE/cases" "$RESULTS" > /dev/null || true
echo "results: $RESULTS"
exit $rc
