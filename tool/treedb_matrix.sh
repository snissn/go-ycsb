#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

GO_YCSB=${GO_YCSB:-"$ROOT/bin/go-ycsb"}
RESULTS_DIR=${RESULTS_DIR:-"$ROOT/bench-results/treedb-matrix-$(date +%Y%m%d-%H%M%S)"}
BASE_DIR=${BASE_DIR:-"./bench-data"}
RECORDCOUNT=${RECORDCOUNT:-1000000}
OPERATIONCOUNT=${OPERATIONCOUNT:-1000000}
SCAN_LONG_RECORDCOUNT=${SCAN_LONG_RECORDCOUNT:-}
SCAN_LONG_OPERATIONCOUNT=${SCAN_LONG_OPERATIONCOUNT:-}
ENGINES=${ENGINES:-"treedb badger"}
SCENARIOS=${SCENARIOS:-"baseline read_heavy_skew update_heavy_skew read_only read_latest scan_short scan_long large_values small_values"}

THREADS=${THREADS:-}
TARGET=${TARGET:-}

mkdir -p "$RESULTS_DIR"

if [[ ! -x "$GO_YCSB" ]]; then
  mkdir -p "$(dirname "$GO_YCSB")"
  (cd "$ROOT" && go build -o "$GO_YCSB" ./cmd/go-ycsb)
fi

common_props=()
if [[ -n "$THREADS" ]]; then
  common_props+=("-p" "threadcount=$THREADS")
fi
if [[ -n "$TARGET" ]]; then
  common_props+=("-p" "target=$TARGET")
fi

scenario_load_args() {
  local scenario="$1"
  case "$scenario" in
    baseline)
      echo "-P" "$ROOT/workloads/workloada" "-p" "requestdistribution=uniform"
      ;;
    read_heavy_skew)
      echo "-P" "$ROOT/workloads/workloadb" "-p" "requestdistribution=zipfian"
      ;;
    update_heavy_skew)
      echo "-P" "$ROOT/workloads/workloada" "-p" "readproportion=0.1" "-p" "updateproportion=0.9" "-p" "requestdistribution=zipfian"
      ;;
    read_only)
      echo "-P" "$ROOT/workloads/workloadc" "-p" "requestdistribution=uniform"
      ;;
    read_latest)
      echo "-P" "$ROOT/workloads/workloadd" "-p" "requestdistribution=latest"
      ;;
    scan_short)
      echo "-P" "$ROOT/workloads/workloade" "-p" "minscanlength=10" "-p" "maxscanlength=100" "-p" "scanlengthdistribution=uniform"
      ;;
    scan_long)
      echo "-P" "$ROOT/workloads/workloade" "-p" "minscanlength=1000" "-p" "maxscanlength=10000" "-p" "scanlengthdistribution=uniform"
      ;;
    large_values)
      echo "-P" "$ROOT/workloads/workloada" "-p" "fieldlength=1024"
      ;;
    small_values)
      echo "-P" "$ROOT/workloads/workloada" "-p" "fieldlength=32"
      ;;
    *)
      echo "unknown scenario: $scenario" >&2
      exit 1
      ;;
  esac
}

scenario_run_args() {
  # For now, run props match load props (workload + overrides).
  scenario_load_args "$1"
}

engine_config() {
  local engine="$1"
  case "$engine" in
    treedb)
      echo "treedb" "treedb.dir" ""
      ;;
    treedb-backend)
      echo "treedb" "treedb.dir" "-p treedb.mode=backend"
      ;;
    badger)
      echo "badger" "badger.dir" ""
      ;;
    *)
      echo "unknown engine: $engine" >&2
      exit 1
      ;;
  esac
}

parse_ops() {
  local file="$1" scenario="$2" engine="$3" phase="$4" out="$5"
  awk -v scenario="$scenario" -v engine="$engine" -v phase="$phase" '
    BEGIN {
      n = split("INSERT READ UPDATE SCAN TOTAL", order, " ")
    }
    $1 ~ /^(INSERT|READ|UPDATE|SCAN|TOTAL)$/ {
      op = $1
      for (i = 1; i <= NF; i++) {
        if ($i == "OPS:") {
          val = $(i+1)
          sub(/,/, "", val)
          ops[op] = val
          break
        }
      }
    }
    END {
      for (i = 1; i <= n; i++) {
        op = order[i]
        if (op in ops) {
          printf "%s,%s,%s,%s,%s\n", scenario, engine, phase, op, ops[op]
        }
      }
    }
  ' "$file" >> "$out"
}

results_csv="$RESULTS_DIR/results.csv"
printf "scenario,engine,phase,op,ops\n" > "$results_csv"

write_markdown() {
  local out="$1"
  declare -A ops_map
  while IFS=, read -r scenario engine phase op ops; do
    if [[ "$scenario" == "scenario" ]]; then
      continue
    fi
    ops_map["$phase|$scenario|$engine|$op"]="$ops"
  done < "$results_csv"

  {
    printf "# Results\n\n"
      for phase in load run; do
      printf "## %s\n\n" "$phase"
      printf "|scenario|engine|INSERT OPS|READ OPS|UPDATE OPS|SCAN OPS|TOTAL OPS|\n"
      printf "|-|-|-|-|-|-|-|\n"
      for scenario in $SCENARIOS; do
        for engine in $ENGINES; do
          insert="${ops_map[$phase|$scenario|$engine|INSERT]:--}"
          read_ops="${ops_map[$phase|$scenario|$engine|READ]:--}"
          update="${ops_map[$phase|$scenario|$engine|UPDATE]:--}"
          scan_ops="${ops_map[$phase|$scenario|$engine|SCAN]:--}"
          total="${ops_map[$phase|$scenario|$engine|TOTAL]:--}"
          printf "|%s|%s|%s|%s|%s|%s|%s|\n" "$scenario" "$engine" "$insert" "$read_ops" "$update" "$scan_ops" "$total"
        done
      done
      printf "\n"
    done
  } > "$out"
}

echo "Results dir: $RESULTS_DIR"

for scenario in $SCENARIOS; do
  for engine in $ENGINES; do
    read -r db db_prop db_extra <<<"$(engine_config "$engine")"
    data_dir="$BASE_DIR/$engine/$scenario"
    mkdir -p "$data_dir"

    load_log="$RESULTS_DIR/${scenario}_${engine}_load.log"
    run_log="$RESULTS_DIR/${scenario}_${engine}_run.log"
    read -r -a load_args <<<"$(scenario_load_args "$scenario")"
    read -r -a run_args <<<"$(scenario_run_args "$scenario")"
    db_extra_args=()
    if [[ -n "$db_extra" ]]; then
      read -r -a db_extra_args <<<"$db_extra"
    fi
    scenario_recordcount="$RECORDCOUNT"
    scenario_operationcount="$OPERATIONCOUNT"
    if [[ "$scenario" == "scan_long" ]]; then
      if [[ -n "$SCAN_LONG_RECORDCOUNT" ]]; then
        scenario_recordcount="$SCAN_LONG_RECORDCOUNT"
      fi
      if [[ -n "$SCAN_LONG_OPERATIONCOUNT" ]]; then
        scenario_operationcount="$SCAN_LONG_OPERATIONCOUNT"
      fi
    fi

    echo "=== $engine / $scenario : load ==="
    set -x
    "$GO_YCSB" load "$db" \
      "${load_args[@]}" \
      -p "recordcount=$scenario_recordcount" \
      -p "operationcount=$scenario_recordcount" \
      -p "dropdata=true" \
      -p "$db_prop=$data_dir" \
      "${db_extra_args[@]}" \
      "${common_props[@]}" \
      2>&1 | tee "$load_log"
    set +x

    parse_ops "$load_log" "$scenario" "$engine" "load" "$results_csv"

    echo "=== $engine / $scenario : run ==="
    set -x
    "$GO_YCSB" run "$db" \
      "${run_args[@]}" \
      -p "recordcount=$scenario_recordcount" \
      -p "operationcount=$scenario_operationcount" \
      -p "$db_prop=$data_dir" \
      "${db_extra_args[@]}" \
      "${common_props[@]}" \
      2>&1 | tee "$run_log"
    set +x

    parse_ops "$run_log" "$scenario" "$engine" "run" "$results_csv"
  done
 done

results_md="$RESULTS_DIR/results.md"
write_markdown "$results_md"

echo "Summary CSV: $results_csv"
echo "Summary Markdown: $results_md"
