#!/usr/bin/env bash
# server.sh -- the conformance server launcher AND the control hook adapters call.
#
#   server.sh start   STATE_DIR BIN MODE [NATIVE_PORT]
#   server.sh kill9   STATE_DIR          SIGKILL the server, wait for it to exit
#   server.sh restart STATE_DIR          start again: same binary, mode, ports, data dir
#   server.sh stop    STATE_DIR          SIGTERM, then SIGKILL after 10 s
#   server.sh env     STATE_DIR          print the PDB_CONF_* exports for this server
#
# STATE_DIR holds everything: data/, server.log, pid, and env (the ports).
# Only the PID this script started is ever signalled -- never a name match.
#
# MODE:  default = --concurrency-control optimistic --durability groupsync   (the shipped defaults;
#                  sessions default to READ COMMITTED)
#        strict  = --concurrency-control strict-optimistic --durability sync
#
# Ports: the native wire is NATIVE_PORT, defaulting to the SERVER's own default
# native port (PDB_CONF_DEFAULT_PORT, else read from `BIN start --help`'s
# "--pdb-port ... (default N)" -- never a number hard-coded here), when that
# port is free, else the first free block from 25440 in steps of 10. Other
# listeners use a separate free block when the native port is available but
# an adjacent port is busy. Restart reuses every recorded listener port.
set -euo pipefail

cmd=${1:?usage: server.sh start|kill9|restart|stop|env STATE_DIR ...}
STATE=${2:?STATE_DIR required}

port_free() { ! nc -z 127.0.0.1 "$1" 2>/dev/null; }
native_free() { port_free "$1" && [ ! -e "/tmp/.s.PDB.$1" ]; }

all_free() {
  local b=$1 o
  native_free "$b" || return 1
  for o in 1 2 3 4 5 6 7; do port_free $((b + o)) || return 1; done
  return 0
}

# The server's compiled-in default native-wire port, from its own --help text.
default_native_port() {
  "$1" start --help 2>/dev/null | awk '/--pdb-port/{f=1;next} f&&/default [0-9]+/{match($0,/default [0-9]+/); print substr($0,RSTART+8,RLENGTH-8); exit}'
}

mode_args() {
  case "$1" in
    default) echo "--concurrency-control optimistic --durability groupsync" ;;
    strict)  echo "--concurrency-control strict-optimistic --durability sync" ;;
    *) echo "unknown mode '$1' (default|strict)" >&2; return 2 ;;
  esac
}

launch() {
  # shellcheck disable=SC1091
  . "$STATE/env"
  local margs
  margs=$(mode_args "$PDB_CONF_MODE")
  mkdir -p "$STATE/data"
  local P=$PDB_CONF_PORT
  # The native listener's Unix socket stays at its default (/tmp/.s.PDB.<port>):
  # a zero-option client derives exactly that path, so the default connect is
  # the one under test. Port uniqueness keeps concurrent runs apart.
  # shellcheck disable=SC2086
  "$PDB_CONF_BIN" start --no-auth --no-console --pg-unix-socket "" $margs --data-dir "$STATE/data" \
    --pdb-addr 127.0.0.1:$P --pg-addr 127.0.0.1:$PDB_CONF_PG_PORT --redis-addr 127.0.0.1:$PDB_CONF_RESP_PORT \
    --grpc-addr 127.0.0.1:$PDB_CONF_GRPC_PORT --http-addr 127.0.0.1:$PDB_CONF_HTTP_PORT --metrics-port $PDB_CONF_METRICS_PORT \
    --health-port $PDB_CONF_HEALTH_PORT --mysql-addr 127.0.0.1:$PDB_CONF_MYSQL_PORT >> "$STATE/server.log" 2>&1 &
  local pid=$!
  echo "$pid" > "$STATE/pid"
  local i
  for i in $(seq 1 600); do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "server died during startup (pid $pid); tail of $STATE/server.log:" >&2
      tail -30 "$STATE/server.log" >&2
      return 1
    fi
    if nc -z 127.0.0.1 "$P" 2>/dev/null && nc -z 127.0.0.1 "$PDB_CONF_RESP_PORT" 2>/dev/null; then
      return 0
    fi
    sleep 0.1
  done
  echo "server did not open port $P within 60 s" >&2
  kill -9 "$pid" 2>/dev/null || true
  return 1
}

wait_gone() {
  local pid=$1 i
  for i in $(seq 1 300); do
    kill -0 "$pid" 2>/dev/null || return 0
    sleep 0.1
  done
  return 1
}

case "$cmd" in
  start)
    BIN=${3:?BIN required}; MODE=${4:?MODE required}
    mode_args "$MODE" > /dev/null
    [ -x "$BIN" ] || { echo "server binary not executable: $BIN" >&2; exit 2; }
    DEFPORT=${PDB_CONF_DEFAULT_PORT:-$(default_native_port "$BIN")}
    [ -n "$DEFPORT" ] || { echo "cannot read the server's default native port from '$BIN start --help'; set PDB_CONF_DEFAULT_PORT" >&2; exit 2; }
    WANT=${5:-$DEFPORT}
    mkdir -p "$STATE"
    base=""
    native=""
    if native_free "$WANT"; then native=$WANT; fi
    if all_free "$WANT"; then base=$WANT; else
      for b in $(seq 25440 10 25990); do
        if all_free "$b" && { [ -z "$native" ] || [ "$native" -lt "$b" ] || [ "$native" -gt $((b + 7)) ]; }; then
          base=$b
          break
        fi
      done
    fi
    [ -n "$base" ] || { echo "no free port block found" >&2; exit 1; }
    native=${native:-$base}
    cat > "$STATE/env" <<EOF
PDB_CONF_BIN=$BIN
PDB_CONF_MODE=$MODE
PDB_CONF_DEFAULT_PORT=$DEFPORT
PDB_CONF_HOST=127.0.0.1
PDB_CONF_PORT=$native
PDB_CONF_PG_PORT=$((base + 1))
PDB_CONF_RESP_PORT=$((base + 2))
PDB_CONF_GRPC_PORT=$((base + 3))
PDB_CONF_HTTP_PORT=$((base + 4))
PDB_CONF_METRICS_PORT=$((base + 5))
PDB_CONF_HEALTH_PORT=$((base + 6))
PDB_CONF_MYSQL_PORT=$((base + 7))
EOF
    launch
    ;;
  kill9)
    pid=$(cat "$STATE/pid")
    kill -9 "$pid" 2>/dev/null || true
    wait_gone "$pid" || { echo "pid $pid did not exit after SIGKILL" >&2; exit 1; }
    ;;
  restart)
    pid=$(cat "$STATE/pid")
    if kill -0 "$pid" 2>/dev/null; then echo "server pid $pid still running; kill9 first" >&2; exit 1; fi
    # The kernel may hold the listening port briefly after the kill.
    . "$STATE/env"
    for _ in $(seq 1 100); do port_free "$PDB_CONF_PORT" && break; sleep 0.1; done
    launch
    ;;
  stop)
    pid=$(cat "$STATE/pid" 2>/dev/null || echo "")
    [ -n "$pid" ] || exit 0
    kill "$pid" 2>/dev/null || exit 0
    wait_gone "$pid" || { kill -9 "$pid" 2>/dev/null || true; wait_gone "$pid" || true; }
    ;;
  env)
    sed 's/^/export /' "$STATE/env"
    echo "export PDB_CONF_CONTROL_STATE=$STATE"
    ;;
  *) echo "unknown command $cmd" >&2; exit 2 ;;
esac
