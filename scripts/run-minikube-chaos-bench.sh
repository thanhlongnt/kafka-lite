#!/usr/bin/env bash
# run-minikube-chaos-bench.sh — build kafka-lite image into Minikube, deploy
# 5 coordinators + 20 brokers, run a throughput test while randomly killing and
# restarting broker and coordinator pods.
#
# Unlike run-minikube-degradation-bench.sh, killed pods are RESTARTED (StatefulSet
# controller recreates them automatically after --force --grace-period=0 deletion),
# simulating transient crash-and-recover failures rather than permanent node loss.
#
# Requires: minikube (running), kubectl, docker, python3 (optional, for plots)
#
# Usage:
#   bash scripts/run-minikube-chaos-bench.sh
#   RATE=10000 DURATION=120s CHAOS_MIN_INTERVAL=10 CHAOS_MAX_INTERVAL=20 \
#       bash scripts/run-minikube-chaos-bench.sh

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$ROOT/results/chaos"
mkdir -p "$LOG_DIR"

# Throughput params
TOPIC="${TOPIC:-bench}"
PARTITIONS="${PARTITIONS:-20}"
CONCURRENCY="${CONCURRENCY:-20}"
RATE="${RATE:-0}"         # 0 = unlimited closed-loop
DURATION="${DURATION:-120s}"
MSG_SIZE="${MSG_SIZE:-256}"

# Chaos params
CHAOS_MIN_INTERVAL="${CHAOS_MIN_INTERVAL:-10}"  # min seconds between kill events
CHAOS_MAX_INTERVAL="${CHAOS_MAX_INTERVAL:-30}"  # max seconds between kill events
CHAOS_DOWN_TIME="${CHAOS_DOWN_TIME:-5}"         # seconds to wait before checking pod is Ready again

NUM_BROKERS=20
NUM_COORDS=5

CSV_OUT="${CSV_OUT:-$LOG_DIR/bench-minikube-chaos.csv}"
EVENTS_FILE="${EVENTS_FILE:-$LOG_DIR/chaos-events-minikube.csv}"

# ── preflight ──────────────────────────────────────────────────────────────────
if ! minikube status --format='{{.Host}}' 2>/dev/null | grep -q "Running"; then
  echo "error: minikube is not running.  Start it with:"
  echo "  minikube start --cpus=6 --memory=8g"
  exit 1
fi

# ── cleanup ────────────────────────────────────────────────────────────────────
JOB_TMP=""
LOG_TMP=""
CHAOS_PID=""

cleanup() {
  echo ""
  echo "==> tearing down cluster..."
  [ -n "$CHAOS_PID" ] && kill "$CHAOS_PID" 2>/dev/null || true
  kubectl delete job/throughput --ignore-not-found 2>/dev/null || true
  kubectl delete -f "$ROOT/k8s/configmap.yaml"   --ignore-not-found 2>/dev/null || true
  kubectl delete -f "$ROOT/k8s/coordinator.yaml" --ignore-not-found 2>/dev/null || true
  kubectl delete -f "$ROOT/k8s/broker.yaml"      --ignore-not-found 2>/dev/null || true
  [ -n "$JOB_TMP" ] && rm -f "$JOB_TMP"
  [ -n "$LOG_TMP" ] && rm -f "$LOG_TMP"
  echo "==> logs left in $LOG_DIR"
  echo "    bench CSV   : $CSV_OUT"
  [ -f "$EVENTS_FILE" ] && echo "    events CSV  : $EVENTS_FILE"
}
trap cleanup EXIT INT TERM

# ── build image inside Minikube's Docker daemon ────────────────────────────────
echo "==> building kafka-lite image in Minikube..."
eval "$(minikube docker-env)"
docker build -t kafka-lite:latest "$ROOT"
echo "    done"

# ── deploy cluster ─────────────────────────────────────────────────────────────
echo "==> deploying cluster..."
kubectl delete job/throughput --ignore-not-found 2>/dev/null || true
kubectl apply -f "$ROOT/k8s/configmap.yaml"
kubectl apply -f "$ROOT/k8s/coordinator.yaml"
kubectl apply -f "$ROOT/k8s/broker.yaml"

echo "==> waiting for coordinators (5 replicas)..."
kubectl rollout status statefulset/coordinator --timeout=90s

echo "==> waiting for brokers ($NUM_BROKERS replicas)..."
kubectl rollout status statefulset/broker --timeout=180s

# ── generate Job manifest ──────────────────────────────────────────────────────
JOB_TMP="$(mktemp /tmp/throughput-job-XXXX.yaml)"
cat >"$JOB_TMP" <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: throughput
spec:
  ttlSecondsAfterFinished: 600
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: throughput
          image: kafka-lite:latest
          imagePullPolicy: Never
          command: ["/bin/sh", "/scripts/start-throughput.sh"]
          env:
            - name: NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
            - name: TOPIC
              value: "$TOPIC"
            - name: PARTITIONS
              value: "$PARTITIONS"
            - name: CONCURRENCY
              value: "$CONCURRENCY"
            - name: DURATION
              value: "$DURATION"
            - name: MSG_SIZE
              value: "$MSG_SIZE"
            - name: RATE
              value: "$RATE"
          volumeMounts:
            - name: scripts
              mountPath: /scripts
      volumes:
        - name: scripts
          configMap:
            name: kafka-lite-scripts
            defaultMode: 0755
EOF

echo ""
echo "==> cluster ready"
echo "    coordinators : coordinator-{0..4}.coordinator.svc.cluster.local:9093"
printf  "    brokers      : "
for i in $(seq 0 $((NUM_BROKERS-1))); do printf "broker-%d " "$i"; done
echo ""
echo ""
echo "==> chaos throughput test"
echo "    topic=$TOPIC  partitions=$PARTITIONS  concurrency=$CONCURRENCY  rate=${RATE:-unlimited}"
echo "    duration=$DURATION  msg-size=${MSG_SIZE}B  mode=route  chaos=on"
echo "    kill interval=${CHAOS_MIN_INTERVAL}-${CHAOS_MAX_INTERVAL}s  down-time=${CHAOS_DOWN_TIME}s"
echo "    CSV output  : $CSV_OUT"
echo "    events CSV  : $EVENTS_FILE"
echo ""

echo "elapsed_s,event,type,num" >"$EVENTS_FILE"

# ── start Job ──────────────────────────────────────────────────────────────────
kubectl apply -f "$JOB_TMP"

echo "==> waiting for throughput pod..."
until kubectl get pods -l job-name=throughput -o name 2>/dev/null | grep -q pod; do
  sleep 1
done
POD=$(kubectl get pods -l job-name=throughput -o jsonpath='{.items[0].metadata.name}')
until kubectl get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null | grep -qv Pending; do
  sleep 1
done

# ── chaos loop (background) ────────────────────────────────────────────────────
# Randomly kills broker or coordinator pods (2:1 bias toward brokers).
# Pods are part of StatefulSets, so Kubernetes recreates them automatically after
# the force-delete.  After CHAOS_DOWN_TIME seconds we wait for the pod to be Ready
# before logging the "restart" event.

chaos_loop() {
  local dead_coords=0
  local interval_range=$(( CHAOS_MAX_INTERVAL - CHAOS_MIN_INTERVAL + 1 ))

  while true; do
    sleep $(( RANDOM % interval_range + CHAOS_MIN_INTERVAL ))

    if [ $(( RANDOM % 3 )) -lt 2 ]; then
      # Kill a random broker pod.
      local idx=$(( RANDOM % NUM_BROKERS ))
      local pod="broker-$idx"
      local elapsed=$(( $(date +%s) - BENCH_EPOCH ))
      echo "[CHAOS] killing $pod  elapsed=${elapsed}s"
      echo "$elapsed,kill,broker,$((idx+1))" >>"$EVENTS_FILE"
      kubectl delete pod "$pod" --force --grace-period=0 2>/dev/null || true
      sleep "$CHAOS_DOWN_TIME"
      local elapsed=$(( $(date +%s) - BENCH_EPOCH ))
      echo "[CHAOS] $pod restarting...  elapsed=${elapsed}s"
      echo "$elapsed,restart,broker,$((idx+1))" >>"$EVENTS_FILE"
      # Wait for StatefulSet controller to recreate and bring Ready; don't block long.
      kubectl wait pod/"$pod" --for=condition=Ready --timeout=60s 2>/dev/null || true
    else
      # Kill a random coordinator pod (never more than 2 simultaneously — quorum needs 3/5).
      if [ "$dead_coords" -ge 2 ]; then
        continue
      fi
      local idx=$(( RANDOM % NUM_COORDS ))
      local pod="coordinator-$idx"
      local elapsed=$(( $(date +%s) - BENCH_EPOCH ))
      echo "[CHAOS] killing $pod  elapsed=${elapsed}s"
      echo "$elapsed,kill,coordinator,$((idx+1))" >>"$EVENTS_FILE"
      kubectl delete pod "$pod" --force --grace-period=0 2>/dev/null || true
      dead_coords=$(( dead_coords + 1 ))
      sleep "$CHAOS_DOWN_TIME"
      local elapsed=$(( $(date +%s) - BENCH_EPOCH ))
      echo "[CHAOS] $pod restarting...  elapsed=${elapsed}s"
      echo "$elapsed,restart,coordinator,$((idx+1))" >>"$EVENTS_FILE"
      kubectl wait pod/"$pod" --for=condition=Ready --timeout=90s 2>/dev/null || true
      dead_coords=$(( dead_coords - 1 ))
    fi
  done
}

# ── stream logs, aligned to throughput tool's clock ───────────────────────────
# kubectl logs is backgrounded so we can detect when the tool starts its
# per-second loop and align chaos event timestamps to elapsed_s.
LOG_TMP="$(mktemp /tmp/pod-logs-XXXX.txt)"
echo "==> streaming output from $POD"
echo ""
kubectl logs -f "$POD" 2>/dev/null | tee "$LOG_TMP" &
LOGS_PID=$!

until grep -qm1 '\[  *1s\]' "$LOG_TMP" 2>/dev/null; do sleep 0.1; done
export BENCH_EPOCH=$(( $(date +%s) - 1 ))

chaos_loop &
CHAOS_PID=$!

wait "$LOGS_PID" 2>/dev/null || true

# Job finished — stop the chaos loop (it's an infinite loop so we kill it).
kill "$CHAOS_PID" 2>/dev/null || true
wait "$CHAOS_PID" 2>/dev/null || true
CHAOS_PID=""

# ── extract CSV from log markers ───────────────────────────────────────────────
if awk '/^---CSV_START---/{found=1;next} /^---CSV_END---/{exit} found && /^(elapsed_s|[0-9])/{print}' \
       "$LOG_TMP" >"$CSV_OUT" && [ -s "$CSV_OUT" ]; then
  echo ""
  echo "==> CSV extracted to $CSV_OUT"
else
  echo ""
  echo "==> warning: CSV markers not found in pod logs — no CSV written"
fi

# ── auto-plot ──────────────────────────────────────────────────────────────────
PLOT_OUT="$LOG_DIR/bench-minikube-chaos.png"
if [ "${PLOT:-1}" != "0" ] && command -v python3 >/dev/null 2>&1 \
   && [ -f "$ROOT/scripts/plot-bench.py" ] && [ -s "$CSV_OUT" ]; then
  echo ""
  echo "==> plotting results..."
  python3 "$ROOT/scripts/plot-bench.py" \
    --data    "$CSV_OUT" \
    --events  "$EVENTS_FILE" \
    --brokers "$NUM_BROKERS" \
    --out     "$PLOT_OUT" \
    --title   "kafka-lite chaos (minikube): ${PARTITIONS}p rate=${RATE:-unlimited} ${MSG_SIZE}B kill every ${CHAOS_MIN_INTERVAL}-${CHAOS_MAX_INTERVAL}s" \
    && echo "    saved to $PLOT_OUT" \
    || echo "    plot failed (run manually: python3 scripts/plot-bench.py --data $CSV_OUT --events $EVENTS_FILE)"
fi
