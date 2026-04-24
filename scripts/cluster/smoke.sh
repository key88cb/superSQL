#!/usr/bin/env bash
# cluster.smoke — 集群最小可用闭环
#
# 步骤：
#   1. 确认 docker compose 全 healthy
#   2. 任选 master，GET /status 校验存在 ACTIVE 与 STANDBY
#   3. RS /status 含 suspectedReplicaCount 字段
#   4. 通过 client 容器完成 CREATE → INSERT → SELECT → DROP 闭环

set -u
set -o pipefail

CLIENT_CONTAINER="${CLIENT_CONTAINER:-client}"
CLIENT_JAR_PATH="${CLIENT_JAR_PATH:-/app/app.jar}"
TABLE="smoke_$(date +%s)"

fail() { echo "[FAIL] $*" >&2; exit 1; }
ok()   { echo "[PASS] $*"; }

# 1. docker 健康
running=$(docker compose ps --status running -q | wc -l | tr -d ' ')
[[ "$running" -ge 10 ]] || fail "cluster containers running=$running (expected >=10)"
ok "cluster containers up: $running"

# 2. master 角色分布（管理 HTTP 端口 8880/8881/8882；808X 是 Thrift）
active=0; standby=0
for port in 8880 8881 8882; do
    role=$(curl -sf "http://localhost:$port/status" | grep -oE '"role":"[A-Z_]+"' | head -1 || true)
    case "$role" in
        *ACTIVE*)  active=$((active+1)) ;;
        *STANDBY*) standby=$((standby+1)) ;;
        *)         echo "[warn] master :$port role='$role'" ;;
    esac
done
[[ "$active" -eq 1 ]] || fail "active master count=$active (expected 1)"
[[ "$standby" -ge 1 ]] || fail "standby master count=$standby (expected >=1)"
ok "master roles: active=$active standby=$standby"

# 3. RS /status 字段（RS 管理端口默认与 Thrift 同端口暴露，见 chaos_test.sh 中 docker exec 容器内 :9190）
# 从主机侧可通过 docker exec 访问每个 RS 容器的管理端口
for svc in rs-1 rs-2 rs-3; do
    payload=$(docker exec "$svc" sh -lc "curl -sf http://localhost:9190/status" 2>/dev/null || true)
    echo "$payload" | grep -q 'suspectedReplicaCount' \
        || fail "rs $svc /status missing suspectedReplicaCount"
done
ok "rs /status contains suspectedReplicaCount"

# 4. SQL 闭环
run_sql() {
    printf "%s\nexit;\n" "$1" | docker exec -i "$CLIENT_CONTAINER" java -jar "$CLIENT_JAR_PATH" 2>&1
}

run_sql "CREATE TABLE $TABLE(id int, name char(20), primary key(id));" >/tmp/smoke_create.log \
    || { cat /tmp/smoke_create.log; fail "create table failed"; }
run_sql "INSERT INTO $TABLE VALUES (1,'alpha');" >/dev/null || fail "insert failed"
run_sql "INSERT INTO $TABLE VALUES (2,'beta');"  >/dev/null || fail "insert failed"
out=$(run_sql "SELECT * FROM $TABLE;")
echo "$out" | grep -q 'alpha' || { echo "$out"; fail "select missing alpha"; }
echo "$out" | grep -q 'beta'  || { echo "$out"; fail "select missing beta"; }
run_sql "DROP TABLE $TABLE;"  >/dev/null || fail "drop failed"
ok "sql ddl+dml+select loop OK"

echo "[SUMMARY] cluster.smoke PASS"
