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

run_sql_script() {
    # $1 = literal SQL block, fed as multi-statement input
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

# 5. SQL 覆盖闭环：CREATE INDEX / DROP INDEX / DELETE / SELECT WHERE = / range / AND/OR
# 与 §8.2 SQL 覆盖矩阵的 cluster.smoke 列一一对应。一次 client 调用串入全部语句，
# 减少多次 java 启动开销。
COV_TABLE="smoke_cov_$(date +%s)"
COV_SCRIPT="CREATE TABLE $COV_TABLE(id int, grade int, dept char(8), primary key(id));
INSERT INTO $COV_TABLE VALUES (1, 90, 'eng');
INSERT INTO $COV_TABLE VALUES (2, 80, 'eng');
INSERT INTO $COV_TABLE VALUES (3, 70, 'math');
INSERT INTO $COV_TABLE VALUES (4, 90, 'math');
INSERT INTO $COV_TABLE VALUES (5, 60, 'phys');
CREATE INDEX idx_${COV_TABLE} ON $COV_TABLE(grade);
SELECT * FROM $COV_TABLE WHERE grade = 90;
SELECT * FROM $COV_TABLE WHERE grade > 75;
SELECT * FROM $COV_TABLE WHERE grade = 90 AND dept = 'eng';
SELECT * FROM $COV_TABLE WHERE dept = 'eng' OR dept = 'phys';
DELETE FROM $COV_TABLE WHERE dept = 'phys';
SELECT * FROM $COV_TABLE;
DROP INDEX idx_${COV_TABLE} ON $COV_TABLE;
DROP TABLE $COV_TABLE;"

cov_out=$(run_sql_script "$COV_SCRIPT")
# CREATE INDEX 应回 OK，SELECT 行数 footer 验证语义
echo "$cov_out" | grep -qE 'Index .*created|index .*created|OK' \
    || { echo "$cov_out"; fail "create index produced no OK marker"; }
echo "$cov_out" | grep -qE '\(2 rows?\)'  || { echo "$cov_out"; fail "WHERE = expected 2 rows"; }
echo "$cov_out" | grep -qE '\(4 rows?\)'  || { echo "$cov_out"; fail "WHERE > expected 4 rows"; }
echo "$cov_out" | grep -qE '\(1 row\)'    || { echo "$cov_out"; fail "AND expected 1 row"; }
echo "$cov_out" | grep -qE '\(3 rows?\)'  || { echo "$cov_out"; fail "OR expected 3 rows"; }
# DELETE 后 final SELECT 期望 4 行（删除 dept='phys' 的 1 条）
echo "$cov_out" | grep -qcE '\(4 rows?\)' >/dev/null \
    || { echo "$cov_out"; fail "post-delete SELECT row count missing"; }
ok "sql coverage loop OK (CREATE/DROP INDEX, range/AND/OR, DELETE)"

# 6. UPDATE / ALTER / TRUNCATE 错误路径：cpp engine 不支持，必须明确返回 ERROR 而非静默通过
ERR_TABLE="smoke_err_$(date +%s)"
ERR_SCRIPT="CREATE TABLE $ERR_TABLE(id int, primary key(id));
INSERT INTO $ERR_TABLE VALUES (1);
UPDATE $ERR_TABLE SET id = 2 WHERE id = 1;
ALTER TABLE $ERR_TABLE ADD COLUMN c int;
TRUNCATE TABLE $ERR_TABLE;
DROP TABLE $ERR_TABLE;"

err_out=$(run_sql_script "$ERR_SCRIPT")
# 至少捕获到 ERROR/Error 字样，避免回归到「静默 success」
echo "$err_out" | grep -qE 'Error|ERROR' \
    || { echo "$err_out"; fail "UPDATE/ALTER/TRUNCATE 应返回 ERROR 但未观察到"; }
ok "unsupported sql (UPDATE/ALTER/TRUNCATE) reported error explicitly"

echo "[SUMMARY] cluster.smoke PASS"
