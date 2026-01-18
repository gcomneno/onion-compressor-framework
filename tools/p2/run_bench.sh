#!/usr/bin/env bash
set -euo pipefail

TIME_BIN="/usr/bin/time"
if [[ ! -x "$TIME_BIN" ]]; then
  echo "ERROR: missing $TIME_BIN (install package 'time')" >&2
  exit 2
fi

need() { command -v "$1" >/dev/null 2>&1 || { echo "ERROR: manca '$1' nel PATH" >&2; exit 2; }; }
need du
need awk
need date

HAS_RG=0
if command -v rg >/dev/null 2>&1; then
  HAS_RG=1
fi

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <dataset_dir_or_input_dir> [--buckets N] [--modes classic,single,mixed] [--skip-verify] [--skip-unpack] [--timeout SEC]" >&2
  exit 2
fi

DATASET="$1"
shift || true

BUCKETS="8"
MODES="classic,single,mixed"
SKIP_VERIFY=0
SKIP_UNPACK=0
TIMEOUT_SEC=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --buckets) BUCKETS="$2"; shift 2;;
    --modes) MODES="$2"; shift 2;;
    --skip-verify) SKIP_VERIFY=1; shift 1;;
    --skip-unpack) SKIP_UNPACK=1; shift 1;;
    --timeout) TIMEOUT_SEC="$2"; shift 2;;
    *) echo "ERROR: arg sconosciuto: $1" >&2; exit 2;;
  esac
done

if [[ -d "$DATASET/in" ]]; then
  IN_DIR="$(cd "$DATASET/in" && pwd)"
  DS_NAME="$(basename "$(cd "$DATASET" && pwd)")"
else
  IN_DIR="$(cd "$DATASET" && pwd)"
  DS_NAME="$(basename "$IN_DIR")"
fi

OUT_ROOT="${OCF_P2_OUT:-bench_out/ocf_p2}"
mkdir -p "$OUT_ROOT"

TS="$(date +%Y%m%d-%H%M%S)"
RUN_DIR="$OUT_ROOT/${TS}_${DS_NAME}"
LOG_DIR="$RUN_DIR/logs"
mkdir -p "$LOG_DIR"

CSV="$RUN_DIR/bench.csv"
MD="$RUN_DIR/bench.md"

echo "dataset,mode,step,rc,elapsed_sec,max_rss_kb,in_bytes,out_bytes,ratio,note" > "$CSV"

in_bytes="$(du -sb "$IN_DIR" | awk '{print $1}')"

parse_metric() {
  local key="$1"
  local file="$2"
  if [[ ! -f "$file" ]]; then
    echo "NA"
    return
  fi
  if [[ "$HAS_RG" == "1" ]]; then
    rg -N "^${key}=" "$file" | head -n 1 | cut -d= -f2 || echo "NA"
    return
  fi
  awk -F= -v k="$key" '$1==k {print $2; exit}' "$file" 2>/dev/null || echo "NA"
}

run_timed() {
  local mode="$1"; shift
  local step="$1"; shift
  local log="$LOG_DIR/${mode}_${step}.log"
  local met="$LOG_DIR/${mode}_${step}.metrics"

  local -a cmd=( "$@" )

  if [[ -n "$TIMEOUT_SEC" ]]; then
    if command -v timeout >/dev/null 2>&1; then
      cmd=( timeout "$TIMEOUT_SEC" "${cmd[@]}" )
    else
      echo "WARN: --timeout richiesto ma 'timeout' non presente; ignoro." >&2
    fi
  fi

  set +e
  "$TIME_BIN" -f "ELAPSED_SEC=%e\nMAX_RSS_KB=%M" -o "$met" "${cmd[@]}" >"$log" 2>&1
  local rc="$?"
  set -e

  local elapsed rss
  elapsed="$(parse_metric "ELAPSED_SEC" "$met")"
  rss="$(parse_metric "MAX_RSS_KB" "$met")"

  echo "$rc|$elapsed|$rss|$log"
}

ratio_of() {
  local out_b="$1"
  python3 - <<PY
in_b = int("$in_bytes")
out_b = int("$out_b")
print("NA" if in_b <= 0 else f"{out_b / in_b:.4f}")
PY
}

is_single_not_applicable() {
  local log="$1"
  if [[ ! -f "$log" ]]; then
    return 1
  fi
  if rg -N -i "text-only|non.?utf|utf-8|nul|binary|non.*testo|not.*text" "$log" >/dev/null 2>&1; then
    return 0
  fi
  return 1
}

bench_mode() {
  local mode="$1"
  local out_dir="$RUN_DIR/out_${mode}"
  local restored="$RUN_DIR/restored_${mode}"
  rm -rf "$out_dir" "$restored"
  mkdir -p "$out_dir" "$restored"

  local out_bytes="NA"
  local ratio="NA"
  local rc elapsed rss log note

  if [[ "$mode" == "classic" ]]; then
    IFS='|' read -r rc elapsed rss log < <(run_timed "$mode" "pack" gcc-ocf dir pack "$IN_DIR" "$out_dir" --buckets "$BUCKETS")
  elif [[ "$mode" == "single" ]]; then
    IFS='|' read -r rc elapsed rss log < <(run_timed "$mode" "pack" gcc-ocf dir pack "$IN_DIR" "$out_dir" --single-container)
  elif [[ "$mode" == "mixed" ]]; then
    IFS='|' read -r rc elapsed rss log < <(run_timed "$mode" "pack" gcc-ocf dir pack "$IN_DIR" "$out_dir" --single-container-mixed)
  else
    echo "ERROR: unknown mode: $mode" >&2
    exit 3
  fi

  note=""
  if [[ "$rc" == "0" ]]; then
    out_bytes="$(du -sb "$out_dir" | awk '{print $1}')"
    ratio="$(ratio_of "$out_bytes")"
  else
    if [[ "$mode" == "single" ]] && is_single_not_applicable "$log"; then
      note="NA: single-container is text-only (dataset contains non-text)"
      rc="NA"
      elapsed="NA"
      rss="NA"
    else
      note="FAIL: pack"
    fi
    out_bytes="NA"
    ratio="NA"
  fi

  echo "$DS_NAME,$mode,pack,$rc,$elapsed,$rss,$in_bytes,$out_bytes,$ratio,$note" >> "$CSV"

  if [[ "$rc" != "0" ]]; then
    echo "$DS_NAME,$mode,verify_full,SKIP,NA,NA,$in_bytes,$out_bytes,$ratio,SKIP: pack not ok" >> "$CSV"
    echo "$DS_NAME,$mode,unpack,SKIP,NA,NA,$in_bytes,$out_bytes,$ratio,SKIP: pack not ok" >> "$CSV"
    return
  fi

  if [[ "$SKIP_VERIFY" == "0" ]]; then
    IFS='|' read -r rc elapsed rss log < <(run_timed "$mode" "verify_full" gcc-ocf dir verify "$out_dir" --full)
    note=""
    if [[ "$rc" != "0" ]]; then note="FAIL: verify_full"; fi
    echo "$DS_NAME,$mode,verify_full,$rc,$elapsed,$rss,$in_bytes,$out_bytes,$ratio,$note" >> "$CSV"
  else
    echo "$DS_NAME,$mode,verify_full,SKIP,NA,NA,$in_bytes,$out_bytes,$ratio,SKIP: requested" >> "$CSV"
  fi

  if [[ "$SKIP_UNPACK" == "0" ]]; then
    IFS='|' read -r rc elapsed rss log < <(run_timed "$mode" "unpack" gcc-ocf dir unpack "$out_dir" "$restored")
    note=""
    if [[ "$rc" != "0" ]]; then note="FAIL: unpack"; fi
    echo "$DS_NAME,$mode,unpack,$rc,$elapsed,$rss,$in_bytes,$out_bytes,$ratio,$note" >> "$CSV"
  else
    echo "$DS_NAME,$mode,unpack,SKIP,NA,NA,$in_bytes,$out_bytes,$ratio,SKIP: requested" >> "$CSV"
  fi
}

echo "INFO: dataset=$DS_NAME input=$IN_DIR"
echo "INFO: run_dir=$RUN_DIR"
echo "INFO: buckets=$BUCKETS"
echo "INFO: modes=$MODES"
echo "INFO: skip_verify=$SKIP_VERIFY skip_unpack=$SKIP_UNPACK timeout=${TIMEOUT_SEC:-none}"

IFS=',' read -r -a mode_list <<< "$MODES"
for m in "${mode_list[@]}"; do
  bench_mode "$m"
done

{
  echo "# OCF P2 Bench"
  echo
  echo "- dataset: \`$DS_NAME\`"
  echo "- input: \`$IN_DIR\`"
  echo "- run: \`$RUN_DIR\`"
  echo "- modes: \`$MODES\`"
  echo "- buckets: \`$BUCKETS\`"
  echo "- skip_verify: \`$SKIP_VERIFY\`"
  echo "- skip_unpack: \`$SKIP_UNPACK\`"
  echo "- timeout: \`${TIMEOUT_SEC:-none}\`"
  echo
  echo "## Results (CSV)"
  echo
  echo "\`\`\`"
  if command -v column >/dev/null 2>&1; then
    column -t -s, "$CSV"
  else
    cat "$CSV"
  fi
  echo "\`\`\`"
} > "$MD"

echo "OK: CSV -> $CSV"
echo "OK: MD  -> $MD"
echo "OK: logs -> $LOG_DIR"
