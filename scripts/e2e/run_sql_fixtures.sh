#!/usr/bin/env bash
# e2e.client — 以 docker client 执行 fixture SQL 并对照期望输出
#
# 约定：
#   scripts/e2e/fixtures/<name>.sql        输入脚本
#   scripts/e2e/fixtures/<name>.expected   期望关键输出（逐行正则匹配即可通过）

set -u
set -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURE_DIR="$ROOT/e2e/fixtures"
CLIENT_CONTAINER="${CLIENT_CONTAINER:-client}"
CLIENT_JAR_PATH="${CLIENT_JAR_PATH:-/app/app.jar}"

fail_count=0
pass_count=0

if [[ ! -d "$FIXTURE_DIR" ]]; then
    echo "[INFO] 无 fixtures 目录 ($FIXTURE_DIR)，跳过"
    echo "[SUMMARY] e2e.client SKIPPED (no fixtures)"
    exit 0
fi

# Copy the whole fixtures dir into the client container so that any fixture
# using `execfile <path>` can resolve a real on-container file. /app is owned
# by root in the client image and the unprivileged supersql user can't mkdir,
# so the bootstrap step needs `-u root`.
docker exec -u root "$CLIENT_CONTAINER" sh -lc 'mkdir -p /app/fixtures && chmod 755 /app/fixtures' >/dev/null 2>&1 || true
docker cp "$FIXTURE_DIR/." "$CLIENT_CONTAINER":/app/fixtures/ >/dev/null 2>&1 || true
docker exec -u root "$CLIENT_CONTAINER" sh -lc 'chmod -R a+rX /app/fixtures' >/dev/null 2>&1 || true

shopt -s nullglob

# Pre-cleanup: distributed DROP TABLE doesn't always delete the underlying
# data files on each RS (cluster bug — see TEST_STATUS BUG-17), so a stale
# fixture run can leave rows that cause the next run's INSERTs to fail with
# "Primary key conflict". Force-clean any e2e_* znodes from ZK and any matching
# files from each RS so fixtures see a clean slate. ZK_CLI_CONTAINER defaults
# to zk1; RS_CONTAINERS defaults to "rs-1 rs-2 rs-3".
ZK_CLI_CONTAINER="${ZK_CLI_CONTAINER:-zk1}"
RS_CONTAINERS="${RS_CONTAINERS:-rs-1 rs-2 rs-3}"
zk_tables=$(docker exec "$ZK_CLI_CONTAINER" zkCli.sh -server "$ZK_CLI_CONTAINER":2181 ls /supersql/meta/tables 2>/dev/null | grep "^\[" | tr -d "[]" | tr "," "\n" | tr -d " ")
for t in $zk_tables; do
    case "$t" in
        e2e_*)
            docker exec "$ZK_CLI_CONTAINER" zkCli.sh -server "$ZK_CLI_CONTAINER":2181 deleteall /supersql/meta/tables/$t >/dev/null 2>&1 || true
            docker exec "$ZK_CLI_CONTAINER" zkCli.sh -server "$ZK_CLI_CONTAINER":2181 deleteall /supersql/assignments/$t >/dev/null 2>&1 || true
            ;;
    esac
done
for rs in $RS_CONTAINERS; do
    docker exec -u root "$rs" sh -lc 'rm -f /data/db/database/data/e2e_* /data/db/database/index/idx_e2e_* 2>/dev/null' >/dev/null 2>&1 || true
done

first=1
for sql in "$FIXTURE_DIR"/*.sql; do
    name=$(basename "$sql" .sql)
    expected="$FIXTURE_DIR/$name.expected"
    # Each fixture spawns a fresh SqlClient JVM, but the master/RS state from
    # the previous fixture (DROP TABLE → ZK delete event → route cache
    # invalidation watcher) propagates asynchronously. Without a short pause
    # the next fixture's CREATE TABLE can race with stale RouteCache and the
    # first SELECT may transiently see TABLE_NOT_FOUND or stale rows.
    [[ $first -eq 0 ]] && sleep 2
    first=0
    echo "[RUN] $name"
    out=$(docker exec -i "$CLIENT_CONTAINER" java -jar "$CLIENT_JAR_PATH" < "$sql" 2>&1 || true)
    if [[ ! -f "$expected" ]]; then
        echo "[WARN] $name 无 .expected，视为通过（仅捕获输出）"
        pass_count=$((pass_count+1))
        continue
    fi
    miss=0
    while IFS= read -r pat; do
        [[ -z "$pat" ]] && continue
        [[ "$pat" =~ ^# ]] && continue
        if ! echo "$out" | grep -Eq "$pat"; then
            echo "[FAIL] $name missing pattern: $pat"
            miss=$((miss+1))
        fi
    done < "$expected"
    if [[ "$miss" -eq 0 ]]; then
        echo "[PASS] $name"
        pass_count=$((pass_count+1))
    else
        fail_count=$((fail_count+1))
    fi
done

echo "[SUMMARY] e2e.client pass=$pass_count fail=$fail_count"
[[ "$fail_count" -eq 0 ]]
