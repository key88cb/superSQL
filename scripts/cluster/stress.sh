#!/usr/bin/env bash
# cluster.stress — 集群级别吞吐/容量压测
#
# 两个阶段：
#   phase1_concurrent_insert  N 个并发 client 各插入 1000 行
#   phase2_bulk_import        单 client 插入 10 万行，每 10s 校验行数单调增长

set -u
set -o pipefail

CLIENT_CONTAINER="${CLIENT_CONTAINER:-client}"
CLIENT_JAR_PATH="${CLIENT_JAR_PATH:-/app/app.jar}"
CONCURRENCY="${STRESS_CONCURRENCY:-4}"
ROWS_PER_WORKER="${STRESS_ROWS_PER_WORKER:-1000}"
BULK_ROWS="${STRESS_BULK_ROWS:-100000}"
BULK_CHECK_INTERVAL="${STRESS_BULK_CHECK_INTERVAL:-10}"

fail() { echo "[FAIL] $*" >&2; exit 1; }
ok()   { echo "[PASS] $*"; }

run_sql() {
    printf "%s\nexit;\n" "$1" | docker exec -i "$CLIENT_CONTAINER" java -jar "$CLIENT_JAR_PATH" 2>&1
}

run_sql_script() {
    # $1 script file path (on host)
    docker exec -i "$CLIENT_CONTAINER" java -jar "$CLIENT_JAR_PATH" < "$1" 2>&1
}

count_rows() {
    # returns integer row count in $1. SqlClient prints a trailing
    # "(N rows)" footer after SELECT; extract N from there.
    local out
    out=$(run_sql "SELECT * FROM $1;")
    local n
    n=$(echo "$out" | grep -oE '\([0-9]+ rows?\)' | tail -n 1 | grep -oE '[0-9]+')
    echo "${n:-0}"
}

phase1_concurrent_insert() {
    local table="stress_conc_$(date +%s)"
    run_sql "CREATE TABLE $table(id int, worker int, primary key(id));" >/dev/null \
        || fail "create $table failed"

    local pids=() tmpdir
    tmpdir=$(mktemp -d)
    for w in $(seq 1 "$CONCURRENCY"); do
        (
            base=$((w * 1000000))
            for i in $(seq 1 "$ROWS_PER_WORKER"); do
                id=$((base + i))
                printf "INSERT INTO %s VALUES (%d, %d);\n" "$table" "$id" "$w"
            done
            echo "exit;"
        ) > "$tmpdir/worker_$w.sql"
    done

    local started finished
    started=$(date +%s)
    for w in $(seq 1 "$CONCURRENCY"); do
        ( run_sql_script "$tmpdir/worker_$w.sql" > "$tmpdir/worker_$w.log" 2>&1 ) &
        pids+=($!)
    done
    for p in "${pids[@]}"; do
        wait "$p" || fail "worker pid=$p failed"
    done
    finished=$(date +%s)
    local elapsed=$(( finished - started ))
    local total=$(( CONCURRENCY * ROWS_PER_WORKER ))
    [[ $elapsed -gt 0 ]] || elapsed=1
    local tps=$(( total / elapsed ))
    echo "[metric] phase1 table=$table total=$total elapsed=${elapsed}s tps=$tps"

    local observed
    observed=$(count_rows "$table")
    [[ "$observed" -eq "$total" ]] || fail "row count mismatch: expected=$total observed=$observed"
    ok "phase1 concurrent insert"

    run_sql "DROP TABLE $table;" >/dev/null || true
    rm -rf "$tmpdir"
}

phase2_bulk_import() {
    local table="stress_bulk_$(date +%s)"
    run_sql "CREATE TABLE $table(id int, payload char(32), primary key(id));" >/dev/null \
        || fail "create $table failed"

    local tmpfile
    tmpfile=$(mktemp)
    {
        for i in $(seq 1 "$BULK_ROWS"); do
            printf "INSERT INTO %s VALUES (%d, 'p_%d');\n" "$table" "$i" "$i"
        done
        echo "exit;"
    } > "$tmpfile"

    ( run_sql_script "$tmpfile" > /tmp/bulk_import.log 2>&1 ) &
    local import_pid=$!

    local last=-1 samples=0 started
    started=$(date +%s)
    while kill -0 "$import_pid" 2>/dev/null; do
        sleep "$BULK_CHECK_INTERVAL"
        local current
        current=$(count_rows "$table")
        echo "[progress] phase2 rows=$current (last=$last)"
        if [[ "$current" -lt "$last" ]]; then
            kill "$import_pid" 2>/dev/null || true
            fail "row count went backwards: last=$last current=$current"
        fi
        last="$current"
        samples=$((samples + 1))
        if [[ "$samples" -gt 120 ]]; then
            kill "$import_pid" 2>/dev/null || true
            fail "phase2 timeout (> ${samples} * ${BULK_CHECK_INTERVAL}s)"
        fi
    done
    wait "$import_pid" || fail "bulk import process failed"

    local finished observed
    finished=$(date +%s)
    observed=$(count_rows "$table")
    echo "[metric] phase2 expected=$BULK_ROWS observed=$observed elapsed=$((finished - started))s"
    [[ "$observed" -eq "$BULK_ROWS" ]] || fail "row count mismatch: expected=$BULK_ROWS observed=$observed"
    ok "phase2 bulk import"

    run_sql "DROP TABLE $table;" >/dev/null || true
    rm -f "$tmpfile"
}

phase1_concurrent_insert
phase2_bulk_import

echo "[SUMMARY] cluster.stress PASS"
