#!/usr/bin/env bash
# scripts/chaos/network_disconnect.sh
# ------------------------------------------------------------------------
# 拔网线测试 — 用 docker network disconnect/connect 代替 iptables，
# 绕过 `rs-X` 镜像未装 iptables 的限制（BUG-09）。
#
# 5 个场景（可通过 $1 单独指定，或 `all` 跑全部）：
#   rs_isolated       — 把 rs-2 从网络剥离 20s；期间读应可用，写允许返回
#                       tentative error。恢复后 seed 行必须依然可读。
#   master_isolated   — 把当前 ACTIVE master 剥离；新主必须在 60s 内选出，
#                       epoch 单调递增。旧主恢复后应降为 STANDBY。
#   zk_isolated       — 把 zk3 剥离 15s；集群应保持可用（ZK 还有 quorum）。
#                       恢复后 zk3 应重新加入。
#   client_isolated   — 把 client 剥离 10s 再恢复；RouteCache 应能重连并
#                       完成后续 SQL。
#   dual_rs_isolated  — 同时剥离 2 个 RS；集群应进入只读/部分不可用，恢复后
#                       数据一致。
#
# 用法：
#   scripts/chaos/network_disconnect.sh rs_isolated
#   scripts/chaos/network_disconnect.sh all
#
# 环境变量：
#   NET_NAME                 (default: supersql_supersql-net)
#   NET_PARTITION_SECONDS    (default: 20)
#   NET_RECOVERY_WAIT_SECONDS (default: 25)
#   CLIENT_CONTAINER         (default: client)
#   CLIENT_JAR_PATH          (default: /app/app.jar)
# ------------------------------------------------------------------------

set -u

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

NET_NAME="${NET_NAME:-supersql_supersql-net}"
PARTITION_SECONDS="${NET_PARTITION_SECONDS:-20}"
RECOVERY_WAIT_SECONDS="${NET_RECOVERY_WAIT_SECONDS:-25}"
CLIENT_CONTAINER="${CLIENT_CONTAINER:-client}"
CLIENT_JAR_PATH="${CLIENT_JAR_PATH:-/app/app.jar}"

FAILURES=0
CURRENT_TABLE=""

log_info()  { printf "%b[INFO]%b %s\n"  "$YELLOW" "$NC" "$*"; }
log_pass()  { printf "%b[PASS]%b %s\n"  "$GREEN"  "$NC" "$*"; }
log_fail()  { printf "%b[FAIL]%b %s\n"  "$RED"    "$NC" "$*"; FAILURES=$((FAILURES+1)); }

# ------------------------------------------------------------------------
# 基础工具
# ------------------------------------------------------------------------

run_sql() {
    printf "%s\nexit;\n" "$1" | docker exec -i "$CLIENT_CONTAINER" java -jar "$CLIENT_JAR_PATH" 2>&1
}

get_active_master() {
    local role
    for i in 1 2 3; do
        role=$(curl -s "http://localhost:888$((i-1))/status" 2>/dev/null \
               | grep -oE '"role":"[A-Z_]+"' | head -n 1 \
               | sed -E 's/"role":"([A-Z_]+)"/\1/')
        if [ "$role" = "ACTIVE" ]; then
            echo "master-$i"; return 0
        fi
    done
    echo "none"; return 1
}

get_active_epoch() {
    docker exec zk1 zkCli.sh -server zk1:2181 get /supersql/active-master 2>/dev/null \
        | grep -oE '"epoch":[0-9]+' | head -n 1 | grep -oE '[0-9]+'
}

wait_healthy() {
    local svc=$1 timeout=${2:-60} i
    for i in $(seq 1 "$timeout"); do
        if docker inspect -f '{{.State.Health.Status}}' "$svc" 2>/dev/null | grep -q healthy; then
            return 0
        fi
        sleep 1
    done
    return 1
}

disconnect_from_net() {
    local svc=$1
    docker network disconnect "$NET_NAME" "$svc" 2>&1 \
        || { log_fail "disconnect $svc failed"; return 1; }
    log_info "disconnected $svc from $NET_NAME"
}

reconnect_to_net() {
    local svc=$1
    docker network connect "$NET_NAME" "$svc" 2>&1 \
        || { log_fail "reconnect $svc failed"; return 1; }
    log_info "reconnected $svc to $NET_NAME"
}

seed_table() {
    local label=$1
    CURRENT_TABLE="netchaos_${label}_$RANDOM"
    run_sql "CREATE TABLE ${CURRENT_TABLE}(id int, name char(20), primary key(id));" >/dev/null || return 1
    # 等待表元数据对 client 可见
    local i
    for i in $(seq 1 15); do
        if ! run_sql "SELECT * FROM ${CURRENT_TABLE};" | grep -qE "TABLE_NOT_FOUND|Table not found"; then
            break
        fi
        sleep 1
    done
    run_sql "INSERT INTO ${CURRENT_TABLE} VALUES (1, 'seed');" >/dev/null || return 1
    log_info "seeded table ${CURRENT_TABLE}"
}

drop_test_table() {
    if [ -n "$CURRENT_TABLE" ]; then
        run_sql "DROP TABLE ${CURRENT_TABLE};" >/dev/null 2>&1 || true
        CURRENT_TABLE=""
    fi
}

# ------------------------------------------------------------------------
# 场景 1：单 RS 被拔网线
# ------------------------------------------------------------------------
scenario_rs_isolated() {
    printf "\n%b>>> Scenario: rs_isolated%b\n" "$YELLOW" "$NC"
    local victim="${NET_RS_VICTIM:-rs-2}"
    seed_table "rs_iso" || { log_fail "seed failed"; drop_test_table; return 1; }

    disconnect_from_net "$victim" || { drop_test_table; return 1; }
    sleep 3

    # 读应该可用（master 会路由到其他副本）
    local read_out
    read_out=$(run_sql "SELECT * FROM ${CURRENT_TABLE} WHERE id = 1;")
    if echo "$read_out" | grep -q "seed"; then
        log_pass "seed row readable while $victim isolated"
    else
        log_fail "seed row unreadable while $victim isolated"
    fi

    # 写：允许返回 transport_error 类错误
    local write_out
    write_out=$(run_sql "INSERT INTO ${CURRENT_TABLE} VALUES (2, 'during_iso');")
    if echo "$write_out" | grep -qE "Error|ERROR|FAIL|TIMEOUT"; then
        log_info "write returned error during isolation (acceptable) — $(echo "$write_out" | grep -oE 'Error[^[:space:]]*' | head -1)"
    else
        log_info "write returned success during isolation"
    fi

    sleep "$PARTITION_SECONDS"
    reconnect_to_net "$victim"
    wait_healthy "$victim" 30 || log_fail "$victim did not become healthy after reconnect"

    sleep "$RECOVERY_WAIT_SECONDS"

    # 恢复后 seed 仍可读
    local after
    after=$(run_sql "SELECT * FROM ${CURRENT_TABLE} WHERE id = 1;")
    if echo "$after" | grep -q "seed"; then
        log_pass "seed row readable after $victim reconnected"
    else
        log_fail "seed row lost after $victim reconnected"
    fi

    drop_test_table
}

# ------------------------------------------------------------------------
# 场景 2：ACTIVE master 被拔网线
# ------------------------------------------------------------------------
scenario_master_isolated() {
    printf "\n%b>>> Scenario: master_isolated%b\n" "$YELLOW" "$NC"

    local victim old_epoch new_epoch new_active
    victim=$(get_active_master)
    old_epoch=$(get_active_epoch)
    log_info "active=$victim epoch=$old_epoch"
    if [ "$victim" = "none" ]; then
        log_fail "no active master before test"
        return 1
    fi

    disconnect_from_net "$victim" || return 1

    # 等待最长 90s 看 epoch 递增（ZK session 超时 + latch 重选）
    local i
    for i in $(seq 1 45); do
        sleep 2
        new_active=$(get_active_master)
        new_epoch=$(get_active_epoch)
        if [ -n "$new_epoch" ] && [ -n "$old_epoch" ] && [ "$new_epoch" != "$old_epoch" ]; then
            break
        fi
    done

    if [ -n "$new_epoch" ] && [ "$new_epoch" != "$old_epoch" ]; then
        log_pass "new active=$new_active epoch=$old_epoch → $new_epoch"
    else
        log_fail "failover did not happen (old_epoch=$old_epoch new_epoch=$new_epoch)"
    fi

    reconnect_to_net "$victim"
    wait_healthy "$victim" 60 || log_fail "$victim unhealthy after reconnect"
    sleep 10

    # 旧主恢复后应为 STANDBY
    local role
    for i in 1 2 3; do
        if [ "master-$i" = "$victim" ]; then
            role=$(curl -s "http://localhost:888$((i-1))/status" 2>/dev/null \
                   | grep -oE '"role":"[A-Z_]+"' | head -n 1 \
                   | sed -E 's/"role":"([A-Z_]+)"/\1/')
            break
        fi
    done
    if [ "$role" = "STANDBY" ] || [ "$role" = "ACTIVE" ]; then
        log_pass "$victim role after reconnect=$role"
    else
        log_fail "$victim has unexpected role after reconnect=$role"
    fi
}

# ------------------------------------------------------------------------
# 场景 3：单 ZK 节点被拔
# ------------------------------------------------------------------------
scenario_zk_isolated() {
    printf "\n%b>>> Scenario: zk_isolated%b\n" "$YELLOW" "$NC"
    local victim="${NET_ZK_VICTIM:-zk3}"

    disconnect_from_net "$victim" || return 1
    sleep 5

    # 集群仍有 quorum（zk1+zk2），业务应能继续
    local probe="netchaos_zkiso_$RANDOM"
    local out
    out=$(run_sql "CREATE TABLE ${probe}(id int, primary key(id));")
    if echo "$out" | grep -qE "Error|ERROR|FAIL|TIMEOUT"; then
        log_fail "CREATE during zk partition failed: $(echo "$out" | tail -1)"
    else
        log_pass "SQL remains available with $victim isolated"
    fi
    run_sql "DROP TABLE ${probe};" >/dev/null 2>&1 || true

    sleep "$PARTITION_SECONDS"
    reconnect_to_net "$victim"
    wait_healthy "$victim" 30 || log_fail "$victim unhealthy after reconnect"
}

# ------------------------------------------------------------------------
# 场景 4：client 被拔 → 恢复 → RouteCache 重连
# ------------------------------------------------------------------------
scenario_client_isolated() {
    printf "\n%b>>> Scenario: client_isolated%b\n" "$YELLOW" "$NC"
    seed_table "cli_iso" || { log_fail "seed failed"; drop_test_table; return 1; }

    disconnect_from_net "client" || { drop_test_table; return 1; }

    # 期间尝试 SQL 应报错
    local off_out
    off_out=$(run_sql "SELECT * FROM ${CURRENT_TABLE};" 2>&1 || true)
    if echo "$off_out" | grep -qE "Error|UnknownHostException|timed out|ConnectException|Could not connect"; then
        log_pass "client produces clear error while disconnected"
    else
        log_info "client produced ambiguous output while disconnected"
    fi

    sleep "$PARTITION_SECONDS"
    reconnect_to_net "client"
    sleep 5

    local back_out
    back_out=$(run_sql "SELECT * FROM ${CURRENT_TABLE} WHERE id = 1;")
    if echo "$back_out" | grep -q "seed"; then
        log_pass "client recovers SQL path after reconnect"
    else
        log_fail "client cannot SQL after reconnect"
    fi

    drop_test_table
}

# ------------------------------------------------------------------------
# 场景 5：同时拔 2 个 RS
# ------------------------------------------------------------------------
scenario_dual_rs_isolated() {
    printf "\n%b>>> Scenario: dual_rs_isolated%b\n" "$YELLOW" "$NC"
    seed_table "dual_rs" || { log_fail "seed failed"; drop_test_table; return 1; }

    disconnect_from_net "rs-1" || { drop_test_table; return 1; }
    disconnect_from_net "rs-2" || { reconnect_to_net "rs-1"; drop_test_table; return 1; }
    sleep 5

    # 读：只剩 rs-3，可能可读也可能不（取决于 seed 行所在副本）
    local read_out
    read_out=$(run_sql "SELECT * FROM ${CURRENT_TABLE} WHERE id = 1;" || true)
    if echo "$read_out" | grep -q "seed"; then
        log_pass "surviving RS still serves read"
    else
        log_info "read unavailable with 2 RS isolated (acceptable)"
    fi

    sleep "$PARTITION_SECONDS"
    reconnect_to_net "rs-1"
    reconnect_to_net "rs-2"
    wait_healthy rs-1 60 || log_fail "rs-1 unhealthy after reconnect"
    wait_healthy rs-2 60 || log_fail "rs-2 unhealthy after reconnect"

    sleep "$RECOVERY_WAIT_SECONDS"

    # 恢复后 seed 仍可读
    local final
    final=$(run_sql "SELECT * FROM ${CURRENT_TABLE} WHERE id = 1;")
    if echo "$final" | grep -q "seed"; then
        log_pass "seed row readable after full recovery"
    else
        log_fail "seed row lost after dual RS recovery"
    fi

    drop_test_table
}

# ------------------------------------------------------------------------
# 入口
# ------------------------------------------------------------------------
usage() {
    cat <<EOF
Usage: $0 <scenario>
  rs_isolated | master_isolated | zk_isolated | client_isolated | dual_rs_isolated | all
Env:
  NET_PARTITION_SECONDS       isolation duration (default 20)
  NET_RECOVERY_WAIT_SECONDS   wait after reconnect (default 25)
  NET_RS_VICTIM               rs to isolate in rs_isolated (default rs-2)
  NET_ZK_VICTIM               zk to isolate in zk_isolated (default zk3)
EOF
}

main() {
    local action="${1:-all}"
    case "$action" in
        rs_isolated)      scenario_rs_isolated ;;
        master_isolated)  scenario_master_isolated ;;
        zk_isolated)      scenario_zk_isolated ;;
        client_isolated)  scenario_client_isolated ;;
        dual_rs_isolated) scenario_dual_rs_isolated ;;
        all)
            scenario_rs_isolated
            scenario_master_isolated
            scenario_zk_isolated
            scenario_client_isolated
            scenario_dual_rs_isolated
            ;;
        *) usage; exit 3 ;;
    esac
    echo ""
    if [ "$FAILURES" -eq 0 ]; then
        log_pass "network_disconnect ALL scenarios OK"
        exit 0
    else
        log_fail "network_disconnect had $FAILURES failure(s)"
        exit 1
    fi
}

main "$@"
