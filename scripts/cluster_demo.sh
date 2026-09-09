#!/usr/bin/env bash
# Brings up a three-node cluster, writes through the leader, and shows the
# writes arriving on the followers.
#
#   scripts/cluster_demo.sh          # runs the demo and leaves the cluster up
#   scripts/cluster_demo.sh --stop   # tears it down
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
RUN=${RUN_DIR:-$ROOT/.run}
BIN=$RUN/bin
GO=${GO:-go}
PEERS=127.0.0.1:7070,127.0.0.1:7071,127.0.0.1:7072

stop_all() {
  for f in "$RUN"/*.pid; do
    [[ -e "$f" ]] || continue
    kill "$(cat "$f")" 2>/dev/null
    rm -f "$f"
  done
  echo "cluster stopped"
}

if [[ "${1:-}" == "--stop" ]]; then stop_all; exit 0; fi

stop_all 2>/dev/null
rm -rf "$RUN"; mkdir -p "$BIN"
"$GO" build -o "$BIN/kvd" "$ROOT/cmd/kvd" || exit 1
"$GO" build -o "$BIN/kvcli" "$ROOT/cmd/kvcli" || exit 1

start() { # name port extra...
  local name=$1 port=$2; shift 2
  "$BIN/kvd" -addr "127.0.0.1:$port" -dir "$RUN/$name" -id "$name" \
    -peers "$PEERS" -heartbeat 500ms -election-timeout 2s "$@" \
    >"$RUN/$name.log" 2>&1 &
  echo $! > "$RUN/$name.pid"
  echo "started $name on 127.0.0.1:$port (pid $(cat "$RUN/$name.pid"))"
}

start n1 7070 -bootstrap
start n2 7071
start n3 7072
sleep 2

echo
echo "--- writing 5000 pairs through the leader ---"
"$BIN/kvcli" -addrs 127.0.0.1:7070 fill demo: 5000

echo
echo "--- reading one of them back from each node ---"
for p in 7070 7071 7072; do
  printf "  127.0.0.1:%s -> %s\n" "$p" "$("$BIN/kvcli" -addrs 127.0.0.1:$p get demo:00004999)"
done

echo
echo "--- a range query served by a follower ---"
"$BIN/kvcli" -addrs 127.0.0.1:7072 range demo:00000100 demo:00000104

echo
echo "--- a write sent to a follower is redirected to the leader ---"
"$BIN/kvcli" -addrs 127.0.0.1:7072 put written:via-follower ok
"$BIN/kvcli" -addrs 127.0.0.1:7071 get written:via-follower

echo
echo "--- cluster state ---"
for p in 7070 7071 7072; do
  echo "  == 127.0.0.1:$p"
  "$BIN/kvcli" -addrs 127.0.0.1:$p info | grep -E "id|role|leader|epoch|seq|keys"
done
echo
echo "cluster is still running; stop it with: scripts/cluster_demo.sh --stop"
