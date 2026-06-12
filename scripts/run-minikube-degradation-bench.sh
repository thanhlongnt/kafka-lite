#!/usr/bin/env bash
# run-minikube-degradation-bench.sh — build kafka-lite image into Minikube, deploy
# 5 coordinators + 20 brokers, run a throughput test while permanently killing brokers
# one by one by scaling down the StatefulSet.
#
# Matches run-degradation-bench.sh behaviour but targets Minikube instead of local
# processes. Broker kills are permanent: scaling the StatefulSet down removes the
# highest-ordinal pod and the controller never recreates it.
#
# Requires: minikube (running), kubectl, docker, python3 (optional, for plots)
#
# Usage:
#   bash scripts/run-minikube-degradation-bench.sh
#   RATE=10000 DURATION=300s DEGRADE_INTERVAL=20 DEGRADE_MAX_KILLS=10 \
#       bash scripts/run-minikube-degradation-bench.sh

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$ROOT/results/degradation"
mkdir -p "$LOG_DIR"

# Throughput params
TOPIC="${TOPIC:-bench}"
PARTITIONS="${PARTITIONS:-20}"
CONCURRENCY="${CONCURRENCY:-20}"   # 20 = one goroutine per partition
RATE="${RATE:-0}"                  # 0 = unlimited closed-loop
DURATION="${DURATION:-300s}"
MSG_SIZE="${MSG_SIZE:-256}"

# Degradation params
DEGRADE_INTERVAL="${DEGRADE_INTERVAL:-20}"   # seconds between permanent kills
DEGRADE_MAX_KILLS="${DEGRADE_MAX_KILLS:-10}" # how many brokers to kill
NUM_BROKERS=20

CSV_OUT="${CSV_OUT:-$LOG_DIR/bench-minikube-degradation.csv}"
EVENTS_FILE="${EVENTS_FILE:-$LOG_DIR/degrade-events-minikube.csv}"

# ── preflight ──────────────────────────────────────────────────────────────────
if ! minikube status --format='{{.Host}}' 2>/dev/null | grep -q "Running"; then
  echo "error: minikube is not running.  Start it with:"
  echo "  minikube start --cpus=6 --memory=8g"
  exit 1
fi

# ── cleanup ────────────────────────────────────────────────────────────────────
JOB_TMP=""
LOG_TMP=""
DEGRADE_PID=""

cleanup() {
  echo ""
  echo "==> tearing down cluster..."
  [ -n "$DEGRADE_PID" ] && kill "$DEGRADE_PID" 2>/dev/null || true
  kubectl delete job/throughput --ignore-not-found 2>/dev/null || true
  kubectl delete -f "$ROOT/k8s/configmap.yaml"  --ignore-not-found 2>/dev/null || true
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
echo "==> degradation test"
echo "    topic=$TOPIC  partitions=$PARTITIONS  concurrency=$CONCURRENCY  rate=${RATE:-unlimited}"
echo "    duration=$DURATION  msg-size=${MSG_SIZE}B  mode=route"
echo "    killing 1 broker every ${DEGRADE_INTERVAL}s, max ${DEGRADE_MAX_KILLS} kills"
echo "    CSV output  : $CSV_OUT"
echo ""

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

# ── degradation loop (background) ─────────────────────────────────────────────
# Permanently removes random broker pods.  kubectl scale always removes the
# highest ordinal, so we use force-delete instead and run a suppressor subshell
# that re-deletes any pod the StatefulSet controller tries to recreate.

echo "elapsed_s,event,type,num" >"$EVENTS_FILE"

degradation_loop() {
  local dead_file
  dead_file="$(mktemp /tmp/dead-brokers-XXXX.txt)"

  # Suppressor: re-kill any dead broker that the StatefulSet controller restarts.
  (while true; do
    if [ -s "$dead_file" ]; then
      while IFS= read -r ord; do
        kubectl delete pod "broker-$ord" --force --grace-period=0 2>/dev/null || true
      done < "$dead_file"
    fi
    sleep 2
  done) &
  local suppressor=$!

  # Build list of available ordinals (0-based) and select randomly.
  local available=()
  for i in $(seq 0 $((NUM_BROKERS-1))); do available+=($i); done

  local killed=0
  while [ $killed -lt "$DEGRADE_MAX_KILLS" ]; do
    sleep "$DEGRADE_INTERVAL"
    local rand_pos=$(( RANDOM % ${#available[@]} ))
    local target=${available[$rand_pos]}
    available=( "${available[@]:0:$rand_pos}" "${available[@]:$((rand_pos+1))}" )

    local elapsed=$(( $(date +%s) - BENCH_EPOCH ))
    local num=$((killed + 1))
    echo "[DEGRADE] killing broker-$target permanently  elapsed=${elapsed}s  killed=$num/$DEGRADE_MAX_KILLS"
    echo "$target" >> "$dead_file"
    kubectl delete pod "broker-$target" --force --grace-period=0 2>/dev/null || true
    echo "$elapsed,kill_permanent,broker,$target" >>"$EVENTS_FILE"
    killed=$((killed + 1))
  done

  echo "[DEGRADE] done — killed $killed brokers, $((NUM_BROKERS - killed)) remaining"
  kill "$suppressor" 2>/dev/null || true
  wait "$suppressor" 2>/dev/null || true
  rm -f "$dead_file"
}

# ── stream logs, aligned to throughput tool's clock ───────────────────────────
# kubectl logs is backgrounded so we can detect when the tool starts its
# per-second loop and align the degradation event timestamps to elapsed_s.
LOG_TMP="$(mktemp /tmp/pod-logs-XXXX.txt)"
echo "==> streaming output from $POD"
echo ""
kubectl logs -f "$POD" 2>/dev/null | tee "$LOG_TMP" &
LOGS_PID=$!

until grep -qm1 '\[  *1s\]' "$LOG_TMP" 2>/dev/null; do sleep 0.1; done
export BENCH_EPOCH=$(( $(date +%s) - 1 ))

degradation_loop &
DEGRADE_PID=$!

wait "$LOGS_PID" 2>/dev/null || true

wait "$DEGRADE_PID" 2>/dev/null || true
DEGRADE_PID=""

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
PLOT_OUT="$LOG_DIR/bench-minikube-degradation.png"
if [ "${PLOT:-1}" != "0" ] && command -v python3 >/dev/null 2>&1 \
   && [ -f "$ROOT/scripts/plot-bench.py" ] && [ -s "$CSV_OUT" ]; then
  echo ""
  echo "==> plotting results..."
  python3 "$ROOT/scripts/plot-bench.py" \
    --data    "$CSV_OUT" \
    --events  "$EVENTS_FILE" \
    --brokers "$NUM_BROKERS" \
    --out     "$PLOT_OUT" \
    --title   "kafka-lite degradation (minikube): ${PARTITIONS}p rate=${RATE:-unlimited} ${MSG_SIZE}B (kill every ${DEGRADE_INTERVAL}s, max ${DEGRADE_MAX_KILLS})" \
    && echo "    saved to $PLOT_OUT" \
    || echo "    plot failed (run manually: python3 scripts/plot-bench.py --data $CSV_OUT --events $EVENTS_FILE --brokers $NUM_BROKERS)"
fi
