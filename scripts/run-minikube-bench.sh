#!/usr/bin/env bash
# run-minikube-bench.sh — build kafka-lite image into Minikube, deploy 5 coordinators +
# 20 brokers, run the throughput job, stream its output, then tear everything down.
#
# Requires: minikube, kubectl, docker
#
# Override benchmark params via env, e.g.:
#   CONCURRENCY=40 DURATION=120s bash scripts/run-minikube-bench.sh

TOPIC="${TOPIC:-bench}"
PARTITIONS="${PARTITIONS:-20}"
CONCURRENCY="${CONCURRENCY:-20}"
DURATION="${DURATION:-60s}"
MSG_SIZE="${MSG_SIZE:-256}"

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# ── preflight ─────────────────────────────────────────────────────────────────
if ! minikube status --format='{{.Host}}' 2>/dev/null | grep -q "Running"; then
  echo "error: minikube is not running. Start it with:"
  echo "  minikube start --cpus=6 --memory=8g"
  exit 1
fi

cleanup() {
  echo ""
  echo "==> tearing down cluster..."
  kubectl delete -f "$ROOT/k8s/" --ignore-not-found 2>/dev/null || true
  echo "==> done."
}
trap cleanup EXIT INT TERM

# ── build image inside Minikube's Docker daemon ───────────────────────────────
echo "==> building kafka-lite image in Minikube..."
eval "$(minikube docker-env)"
docker build -t kafka-lite:latest "$ROOT"
echo "    done"

# ── patch Job manifest with benchmark params ──────────────────────────────────
JOB_TMP="$(mktemp /tmp/throughput-job-XXXX.yaml)"
trap 'rm -f "$JOB_TMP"; cleanup' EXIT INT TERM

sed \
  -e "s/value: \"bench\"/value: \"$TOPIC\"/" \
  -e "s/value: \"20\"$/value: \"$PARTITIONS\"/" \
  -e "s/value: \"20\"$/value: \"$CONCURRENCY\"/" \
  -e "s/value: \"60s\"/value: \"$DURATION\"/" \
  -e "s/value: \"256\"/value: \"$MSG_SIZE\"/" \
  "$ROOT/k8s/throughput-job.yaml" >"$JOB_TMP"

# ── deploy cluster (configmap + coordinators + brokers) ──────────────────────
echo "==> deploying cluster..."
kubectl apply -f "$ROOT/k8s/configmap.yaml"
kubectl apply -f "$ROOT/k8s/coordinator.yaml"
kubectl apply -f "$ROOT/k8s/broker.yaml"

echo "==> waiting for coordinators (5 replicas)..."
kubectl rollout status statefulset/coordinator --timeout=90s

echo "==> waiting for brokers (20 replicas)..."
kubectl rollout status statefulset/broker --timeout=180s

# ── run throughput job ────────────────────────────────────────────────────────
echo ""
echo "==> cluster ready"
echo "    coordinators : coordinator-{0..4}.coordinator.svc.cluster.local:9093"
echo "    brokers      : broker-{0..19}.broker.svc.cluster.local:9092"
echo ""
echo "==> starting throughput job"
echo "    topic=$TOPIC  partitions=$PARTITIONS  concurrency=$CONCURRENCY"
echo "    duration=$DURATION  msg-size=${MSG_SIZE}B  mode=route"
echo ""

kubectl apply -f "$JOB_TMP"

# Wait for the pod to be created then stream its logs.
echo "    waiting for throughput pod..."
until kubectl get pods -l job-name=throughput --field-selector=status.phase!=Pending \
      -o name 2>/dev/null | grep -q pod; do sleep 1; done

POD=$(kubectl get pods -l job-name=throughput -o jsonpath='{.items[0].metadata.name}')
echo "    streaming logs from $POD"
echo ""
kubectl logs -f "$POD"

# Surface job failure after log stream ends.
kubectl wait --for=condition=complete job/throughput --timeout=30s || \
  { echo "error: throughput job did not complete successfully"; exit 1; }
