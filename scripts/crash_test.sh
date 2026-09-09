#!/usr/bin/env bash
# Crash-friendliness test against a real server process.
#
# Writes a stream of keys one at a time, records exactly which writes the
# server acknowledged, then kills the process with SIGKILL mid-stream. After a
# restart every acknowledged write must still be readable.
#
#   scripts/crash_test.sh [sync-mode] [keys]
#
# With -sync always the guarantee is absolute: nothing acknowledged is lost.
# With -sync interval a crash may lose up to one fsync interval of writes, so
# the script reports how many were lost instead of failing.
set -uo pipefail

SYNC=${1:-always}
KEYS=${2:-3000}
ADDR=127.0.0.1:7099
ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d)
BIN=$WORK/bin
GO=${GO:-go}

cleanup() { [[ -n "${PID:-}" ]] && kill -9 "$PID" 2>/dev/null; rm -rf "$WORK"; }
trap cleanup EXIT

echo "building..."
"$GO" build -o "$BIN/kvd" "$ROOT/cmd/kvd" || exit 1
"$GO" build -o "$BIN/kvcli" "$ROOT/cmd/kvcli" || exit 1

start_server() {
  "$BIN/kvd" -addr $ADDR -dir "$WORK/data" -sync "$SYNC" >>"$WORK/server.log" 2>&1 &
  PID=$!
  for _ in $(seq 50); do
    "$BIN/kvcli" -addrs $ADDR ping >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  echo "server did not come up"; cat "$WORK/server.log"; exit 1
}

echo "starting server with -sync $SYNC"
start_server

# Feed sequential writes through one connection and record the acknowledgements.
seq 0 $((KEYS - 1)) | awk '{printf "put crash:%06d value-%06d\n", $1, $1}' > "$WORK/cmds"
( "$BIN/kvcli" -addrs $ADDR < "$WORK/cmds" > "$WORK/acks" 2>/dev/null ) &
WRITER=$!

sleep 0.7
echo "SIGKILLing the server mid-stream"
kill -9 "$PID"; wait "$PID" 2>/dev/null
kill "$WRITER" 2>/dev/null; wait "$WRITER" 2>/dev/null

ACKED=$(grep -c '^OK$' "$WORK/acks")
echo "server acknowledged $ACKED writes before the kill"
if [[ "$ACKED" -eq 0 ]]; then echo "nothing was acknowledged; test inconclusive"; exit 1; fi

echo "restarting"
start_server
grep recovered "$WORK/server.log" | tail -1

LOST=0
for ((i = 0; i < ACKED; i++)); do
  k=$(printf "crash:%06d" "$i")
  got=$("$BIN/kvcli" -addrs $ADDR get "$k" 2>/dev/null)
  if [[ "$got" != "$(printf 'value-%06d' "$i")" ]]; then
    LOST=$((LOST + 1))
    [[ $LOST -le 5 ]] && echo "  missing $k (got '$got')"
  fi
done

echo
if [[ "$SYNC" == "always" ]]; then
  if [[ "$LOST" -eq 0 ]]; then
    echo "PASS: all $ACKED acknowledged writes survived SIGKILL"
    exit 0
  fi
  echo "FAIL: lost $LOST of $ACKED acknowledged writes with -sync always"
  exit 1
fi
echo "result: lost $LOST of $ACKED acknowledged writes with -sync $SYNC"
echo "(that is the documented trade-off: up to one fsync interval may be lost)"
exit 0
