#!/usr/bin/env bash
# sweep-chaos.sh — rate sweep with node kill/restart failures.
# Wrapper around sweep.sh that pre-enables chaos mode and writes results
# to separate output files so they don't overwrite the clean-run sweep.
#
# Usage:
#   bash scripts/sweep-chaos.sh
#   DURATION=120s RATES="2000 5000 10000 20000 40000 80000 160000" bash scripts/sweep-chaos.sh

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

export CHAOS=1
export CHAOS_MIN_INTERVAL="${CHAOS_MIN_INTERVAL:-15}"
export CHAOS_MAX_INTERVAL="${CHAOS_MAX_INTERVAL:-30}"
export CHAOS_DOWN_TIME="${CHAOS_DOWN_TIME:-5}"
export RUN_PREFIX="${RUN_PREFIX:-bench-chaos}"
# SWEEP_CSV / SWEEP_PNG / LOG_DIR are derived by sweep.sh from CHAOS=1

exec bash "$ROOT/scripts/sweep.sh" "$@"
