#!/usr/bin/env bash
# run-minikube-bench.sh — build kafka-lite image into Minikube, deploy
# 5 coordinators + 20 brokers, run a throughput test, extract CSV, plot.
#
# Requires: minikube (running), kubectl, docker, python3 (optional, for plots)
#
# Usage:
#   bash scripts/run-minikube-bench.sh
#   RATE=10000 DURATION=60s CONCURRENCY=20 bash scripts/run-minikube-bench.sh

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$ROOT/results/throughput"
mkdir -p "$LOG_DIR"

# Throughput params
TOPIC="${TOPIC:-bench}"
PARTITIONS="${PARTITIONS:-20}"
CONCURRENCY="${CONCURRENCY:-20}"
RATE="${RATE:-0}"         # 0 = unlimited closed-loop
DURATION="${DURATION:-60s}"
MSG_SIZE="${MSG_SIZE:-256}"

NUM_BROKERS=20

CSV_OUT="${CSV_OUT:-$LOG_DIR/bench-minikube.csv}"
EVENTS_FILE="${EVENTS_FILE:-$LOG_DIR/bench-minikube-events.csv}"

# ── preflight ──────────────────────────────────────────────────────────────────
if ! minikube status --format='{{.Host}}' 2>/dev/null | grep -q "Running"; then
  echo "error: minikube is not running.  Start it with:"
  echo "  minikube start --cpus=6 --memory=8g"
  exit 1
fi

# ── cleanup ────────────────────────────────────────────────────────────────────
JOB_TMP=""
LOG_TMP=""

cleanup() {
  echo ""
  echo "==> tearing down cluster..."
  kubectl delete job/throughput --ignore-not-found 2>/dev/null || true
  kubectl delete -f "$ROOT/k8s/configmap.yaml"   --ignore-not-found 2>/dev/null || true
  kubectl delete -f "$ROOT/k8s/coordinator.yaml" --ignore-not-found 2>/dev/null || true
  kubectl delete -f "$ROOT/k8s/broker.yaml"      --ignore-not-found 2>/dev/null || true
  [ -n "$JOB_TMP" ] && rm -f "$JOB_TMP"
  [ -n "$LOG_TMP" ] && rm -f "$LOG_TMP"
  echo "==> logs left in $LOG_DIR"
  echo "    bench CSV : $CSV_OUT"
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
echo "==> throughput test"
echo "    topic=$TOPIC  partitions=$PARTITIONS  concurrency=$CONCURRENCY  rate=${RATE:-unlimited}"
echo "    duration=$DURATION  msg-size=${MSG_SIZE}B  mode=route"
echo "    CSV output  : $CSV_OUT"
echo ""

# Empty events file (plotter expects it to exist).
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

# ── stream logs, capturing to file for CSV extraction ─────────────────────────
LOG_TMP="$(mktemp /tmp/pod-logs-XXXX.txt)"
echo "==> streaming output from $POD"
echo ""
kubectl logs -f "$POD" | tee "$LOG_TMP" || true

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
PLOT_OUT="$LOG_DIR/bench-minikube.png"
if [ "${PLOT:-1}" != "0" ] && command -v python3 >/dev/null 2>&1 \
   && [ -f "$ROOT/scripts/plot-bench.py" ] && [ -s "$CSV_OUT" ]; then
  echo ""
  echo "==> plotting results..."
  python3 "$ROOT/scripts/plot-bench.py" \
    --data    "$CSV_OUT" \
    --events  "$EVENTS_FILE" \
    --brokers "$NUM_BROKERS" \
    --out     "$PLOT_OUT" \
    --title   "kafka-lite (minikube): ${PARTITIONS}p concurrency=${CONCURRENCY} rate=${RATE:-unlimited} ${MSG_SIZE}B ${DURATION}" \
    && echo "    saved to $PLOT_OUT" \
    || echo "    plot failed (run manually: python3 scripts/plot-bench.py --data $CSV_OUT)"
fi
