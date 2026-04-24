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

shopt -s nullglob
for sql in "$FIXTURE_DIR"/*.sql; do
    name=$(basename "$sql" .sql)
    expected="$FIXTURE_DIR/$name.expected"
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
