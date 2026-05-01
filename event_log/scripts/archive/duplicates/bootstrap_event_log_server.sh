#!/usr/bin/env bash
set -euo pipefail

CODE_ROOT="${CODE_ROOT:-/root/code/event_log}"
DATA_ROOT="${DATA_ROOT:-/data/event_log}"
DB_PATH="${DB_PATH:-$DATA_ROOT/db/event_log.db}"
RUNS_ROOT="${RUNS_ROOT:-$DATA_ROOT/runs}"
LOG_ROOT="${LOG_ROOT:-$DATA_ROOT/logs}"
ARTIFACT_ROOT="${ARTIFACT_ROOT:-$DATA_ROOT/artifacts}"
TMP_ROOT="${TMP_ROOT:-$DATA_ROOT/tmp}"
VENV_ROOT="${VENV_ROOT:-$CODE_ROOT/.venv}"

mkdir -p \
  "$CODE_ROOT" \
  "$DATA_ROOT/db" \
  "$RUNS_ROOT" \
  "$LOG_ROOT" \
  "$ARTIFACT_ROOT" \
  "$TMP_ROOT"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required" >&2
  exit 1
fi

if ! command -v git >/dev/null 2>&1; then
  echo "git is required" >&2
  exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is not installed. Install it before running sync." >&2
  exit 2
fi

if [ ! -d "$VENV_ROOT" ]; then
  uv venv "$VENV_ROOT"
fi

cat <<EOF
bootstrap_ok=pass
code_root=$CODE_ROOT
data_root=$DATA_ROOT
db_path=$DB_PATH
runs_root=$RUNS_ROOT
log_root=$LOG_ROOT
artifact_root=$ARTIFACT_ROOT
tmp_root=$TMP_ROOT
venv_root=$VENV_ROOT
EOF
