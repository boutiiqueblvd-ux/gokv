#!/usr/bin/env bash
# Kills the leader of a running cluster and shows a follower taking over
# without the client being reconfigured. Run scripts/cluster_demo.sh first.
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
RUN=${RUN_DIR:-$ROOT/.run}
BIN=$RUN/bin
ALL=127.0.0.1:7070,127.0.0.1:7071,127.0.0.1:7072

if [[ ! -x "$BIN/kvcli" ]]; then
  echo "run scripts/cluster_demo.sh first"; exit 1
fi

leader_of() { "$BIN/kvcli" -addrs "$1" info 2>/dev/null | awk '$1=="leader"{print $2}'; }

echo "current leader: $(leader_of 127.0.0.1:7071)"
echo
echo "--- SIGKILLing n1 (the leader) ---"
kill -9 "$(cat "$RUN/n1.pid")" 2>/dev/null
rm -f "$RUN/n1.pid"

echo "waiting for the survivors to elect a new leader"
for i in $(seq 60); do
  for p in 7071 7072; do
    role=$("$BIN/kvcli" -addrs 127.0.0.1:$p info 2>/dev/null | awk '$1=="role"{print $2}')
    if [[ "$role" == "leader" ]]; then
      echo "  127.0.0.1:$p is now the leader (after ${i}00ms)"
      NEW=127.0.0.1:$p
      break 2
    fi
  done
  sleep 0.1
done

echo
echo "--- the same client config still accepts writes ---"
"$BIN/kvcli" -addrs "$ALL" put after:failover survived
"$BIN/kvcli" -addrs "$ALL" get after:failover
echo "--- and nothing written before the failover was lost ---"
"$BIN/kvcli" -addrs "$ALL" get demo:00004999

echo
echo "--- restarting n1; it rejoins as a follower ---"
"$BIN/kvd" -addr 127.0.0.1:7070 -dir "$RUN/n1" -id n1 -peers "$ALL" \
  -leader "${NEW:-127.0.0.1:7071}" -heartbeat 500ms -election-timeout 2s \
  >>"$RUN/n1.log" 2>&1 &
echo $! > "$RUN/n1.pid"
sleep 3
"$BIN/kvcli" -addrs 127.0.0.1:7070 info | grep -E "id|role|leader|epoch|keys"
echo
echo "n1 serves the write it never saw as leader:"
"$BIN/kvcli" -addrs 127.0.0.1:7070 get after:failover
