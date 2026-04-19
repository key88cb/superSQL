#!/bin/bash

# ==============================================================================
#  SuperSQL Distributed Chaos Testing Script
#
#  Usage: ./scripts/chaos_test.sh [scenario]
#  Scenarios:
#    master_failover   - Stop active master, verify leader failover
#    rs_crash          - Kill one fixed RS, verify availability and recovery
#    random_rs_crash   - Randomly stop RS nodes for multiple rounds and verify recovery
#    network_partition - Inject a basic RS-to-RS partition and collect observations
#    all               - Run all scenarios above
#
#  Environment overrides:
#    CHAOS_RANDOM_ROUNDS=3
#    CHAOS_STOP_WAIT_SECONDS=8
#    CHAOS_RECOVERY_WAIT_SECONDS=25
#    CHAOS_PARTITION_DURATION_SECONDS=12
#    CHAOS_LOG_DIR=artifacts/chaos
# ==============================================================================

set -u

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

RANDOM_ROUNDS="${CHAOS_RANDOM_ROUNDS:-3}"
STOP_WAIT_SECONDS="${CHAOS_STOP_WAIT_SECONDS:-8}"
RECOVERY_WAIT_SECONDS="${CHAOS_RECOVERY_WAIT_SECONDS:-25}"
PARTITION_DURATION_SECONDS="${CHAOS_PARTITION_DURATION_SECONDS:-12}"
CLIENT_CONTAINER="${CLIENT_CONTAINER:-client}"
CLIENT_JAR_PATH="${CLIENT_JAR_PATH:-/app/app.jar}"
CHAOS_TABLE_PREFIX="${CHAOS_TABLE_PREFIX:-chaos_test}"
CHAOS_LOG_DIR="${CHAOS_LOG_DIR:-artifacts/chaos}"
CHAOS_LOG_FILE="${CHAOS_LOG_DIR}/chaos_test_$(date +%Y%m%d_%H%M%S).log"

echo -e "${YELLOW}Starting SuperSQL Chaos Testing...${NC}"

docker compose ps > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo -e "${RED}Error: Docker Compose is not running or not found in current directory.${NC}"
    exit 1
fi

FAILURES=0
CURRENT_TABLE=""
mkdir -p "$CHAOS_LOG_DIR"
printf "SuperSQL chaos test started at %s\n" "$(date '+%F %T')" > "$CHAOS_LOG_FILE"

function log_line() {
    printf "%s %s\n" "$(date '+%F %T')" "$1" >> "$CHAOS_LOG_FILE"
}

function print_section() {
    echo -e "\n${YELLOW}>>> $1${NC}"
    log_line "SECTION $1"
}

function print_info() {
    echo -e "${YELLOW}$1${NC}"
    log_line "INFO $1"
}

function print_success() {
    echo -e "${GREEN}$1${NC}"
    log_line "PASS $1"
}

function print_error() {
    echo -e "${RED}$1${NC}"
    log_line "FAIL $1"
}

function record_failure() {
    print_error "$1"
    FAILURES=$((FAILURES + 1))
}

function check_command() {
    local command_name=$1
    if ! command -v "$command_name" >/dev/null 2>&1; then
        print_error "Missing required command: $command_name"
        exit 1
    fi
}

function get_active_master() {
    local role
    for i in 1 2 3; do
        role=$(curl -s "http://localhost:888$((i-1))/status" | grep -oP '"role":"\K[^"]+' || true)
        if [ "$role" = "ACTIVE" ]; then
            echo "master-$i"
            return 0
        fi
    done
    echo "none"
    return 1
}

function run_sql() {
    local sql=$1
    local output
    log_line "SQL $sql"
    output=$(printf "%s\nexit;\n" "$sql" | docker exec -i "$CLIENT_CONTAINER" java -jar "$CLIENT_JAR_PATH" 2>&1)
    local status=$?
    if [ $status -ne 0 ]; then
        print_error "SQL execution failed: $sql"
        echo "$output"
        return $status
    fi
    echo "$output"
}

function wait_for_status_url() {
    local url=$1
    local timeout=$2
    local label=$3
    local i
    echo -n "Waiting for $label to respond"
    for i in $(seq 1 "$timeout"); do
        if curl -sf "$url" >/dev/null 2>&1; then
            echo -e " ${GREEN}OK${NC}"
            return 0
        fi
        echo -n "."
        sleep 1
    done
    echo -e " ${RED}TIMEOUT${NC}"
    return 1
}

function wait_for_health() {
    local service=$1
    local timeout=${2:-30}
    local i
    echo -n "Waiting for $service to be healthy"
    for i in $(seq 1 "$timeout"); do
        if docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$service" 2>/dev/null | grep -q "healthy"; then
            echo -e " ${GREEN}OK${NC}"
            return 0
        fi
        echo -n "."
        sleep 1
    done
    echo -e " ${RED}TIMEOUT${NC}"
    return 1
}

function ensure_cluster_ready() {
    print_info "Checking cluster health before chaos scenarios..."
    local service
    for service in master-1 master-2 master-3 rs-1 rs-2 rs-3; do
        if ! wait_for_health "$service" 5; then
            print_info "Service $service is not currently healthy. Continuing may fail."
        fi
    done
}

function create_test_table() {
    local scenario_key=$1
    CURRENT_TABLE="${CHAOS_TABLE_PREFIX}_${scenario_key}_$RANDOM"
    print_info "Preparing table: $CURRENT_TABLE"
    run_sql "CREATE TABLE ${CURRENT_TABLE}(id int, name char(20), primary key(id));" >/dev/null || return 1
    run_sql "INSERT INTO ${CURRENT_TABLE} VALUES (1, 'seed');" >/dev/null || return 1
}

function verify_row_present() {
    local table_name=$1
    local expected=$2
    local output
    output=$(run_sql "SELECT * FROM ${table_name} WHERE id = ${expected};")
    if echo "$output" | grep -q "$expected"; then
        return 0
    fi
    echo "$output"
    return 1
}

function verify_name_present() {
    local table_name=$1
    local expected=$2
    local output
    output=$(run_sql "SELECT * FROM ${table_name};")
    if echo "$output" | grep -q "$expected"; then
        return 0
    fi
    echo "$output"
    return 1
}

function cleanup_test_table() {
    if [ -n "$CURRENT_TABLE" ]; then
        run_sql "DROP TABLE ${CURRENT_TABLE};" >/dev/null 2>&1 || true
        CURRENT_TABLE=""
    fi
}

function command_exists_in_container() {
    local container=$1
    local binary=$2
    docker exec "$container" sh -lc "command -v $binary >/dev/null 2>&1"
}

function block_container_pair() {
    local source=$1
    local target=$2
    local target_ip
    target_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$target" 2>/dev/null)
    if [ -z "$target_ip" ]; then
        return 1
    fi
    docker exec "$source" sh -lc "iptables -C OUTPUT -d $target_ip -j DROP >/dev/null 2>&1 || iptables -A OUTPUT -d $target_ip -j DROP"
    docker exec "$source" sh -lc "iptables -C INPUT -s $target_ip -j DROP >/dev/null 2>&1 || iptables -A INPUT -s $target_ip -j DROP"
}

function unblock_container_pair() {
    local source=$1
    local target=$2
    local target_ip
    target_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$target" 2>/dev/null)
    if [ -z "$target_ip" ]; then
        return 0
    fi
    docker exec "$source" sh -lc "while iptables -C OUTPUT -d $target_ip -j DROP >/dev/null 2>&1; do iptables -D OUTPUT -d $target_ip -j DROP; done" >/dev/null 2>&1 || true
    docker exec "$source" sh -lc "while iptables -C INPUT -s $target_ip -j DROP >/dev/null 2>&1; do iptables -D INPUT -s $target_ip -j DROP; done" >/dev/null 2>&1 || true
}

function apply_partition() {
    local left=$1
    local right=$2
    if ! command_exists_in_container "$left" iptables; then
        record_failure "Container $left does not have iptables; cannot inject network partition."
        return 1
    fi
    if ! command_exists_in_container "$right" iptables; then
        record_failure "Container $right does not have iptables; cannot inject network partition."
        return 1
    fi
    block_container_pair "$left" "$right" || return 1
    block_container_pair "$right" "$left" || return 1
    print_info "Injected network partition between $left and $right"
    return 0
}

function clear_partition() {
    local left=$1
    local right=$2
    unblock_container_pair "$left" "$right"
    unblock_container_pair "$right" "$left"
    print_info "Cleared network partition between $left and $right"
}

function capture_rs_status() {
    local service=$1
    docker exec "$service" sh -lc "curl -sf http://localhost:9190/status" 2>/dev/null
}

function choose_random_rs() {
    local nodes=("rs-1" "rs-2" "rs-3")
    local index=$((RANDOM % ${#nodes[@]}))
    echo "${nodes[$index]}"
}

function test_master_failover() {
    print_section "Scenario 1: Active Master Failover"

    local active
    active=$(get_active_master)
    if [ "$active" = "none" ]; then
        record_failure "No active master found before failover test."
        return 1
    fi

    print_info "Current active master: $active"
    docker stop "$active" >/dev/null
    print_info "Waiting for failover..."
    sleep 20

    local new_active
    new_active=$(get_active_master)
    if [ "$new_active" != "none" ] && [ "$new_active" != "$active" ]; then
        print_success "Failover succeeded. New active master: $new_active"
    else
        record_failure "Failover failed. Expected a new active master after stopping $active."
    fi

    print_info "Restarting $active"
    docker start "$active" >/dev/null
    wait_for_health "$active" 30 || record_failure "Restarted master $active did not become healthy in time."
}

function test_rs_crash_and_recovery() {
    print_section "Scenario 2: Fixed RegionServer Crash & Recovery"
    cleanup_test_table

    if ! create_test_table "fixed_rs"; then
        record_failure "Failed to prepare test table for fixed RS crash scenario."
        return 1
    fi

    print_info "Hard killing rs-1"
    docker kill rs-1 >/dev/null
    sleep "$STOP_WAIT_SECONDS"

    local select_output
    select_output=$(run_sql "SELECT * FROM ${CURRENT_TABLE} WHERE id = 1;")
    if echo "$select_output" | grep -q "seed"; then
        print_success "Read availability preserved while rs-1 is down."
    else
        record_failure "Read availability check failed while rs-1 was down."
    fi

    if run_sql "INSERT INTO ${CURRENT_TABLE} VALUES (2, 'after_crash');" >/dev/null; then
        print_success "Write succeeded while rs-1 was down."
    else
        record_failure "Write failed while rs-1 was down."
    fi

    print_info "Restarting rs-1"
    docker start rs-1 >/dev/null
    wait_for_health rs-1 "$RECOVERY_WAIT_SECONDS" || record_failure "rs-1 did not become healthy after restart."
    sleep 5

    local verify_output
    verify_output=$(run_sql "SELECT * FROM ${CURRENT_TABLE};")
    if echo "$verify_output" | grep -q "after_crash"; then
        print_success "Recovered cluster can still read post-crash writes."
    else
        record_failure "Recovered cluster could not observe data written during rs-1 outage."
    fi

    cleanup_test_table
}

function test_random_rs_crash() {
    print_section "Scenario 3: Random RegionServer Crash Chaos"
    cleanup_test_table

    if ! create_test_table "random_rs"; then
        record_failure "Failed to prepare test table for random RS chaos scenario."
        return 1
    fi

    local round
    for round in $(seq 1 "$RANDOM_ROUNDS"); do
        local victim
        local id
        local payload
        local select_output

        victim=$(choose_random_rs)
        id=$((round + 1))
        payload="round_${round}_${victim}"

        print_info "Round ${round}/${RANDOM_ROUNDS}: stopping ${victim}"
        docker stop "$victim" >/dev/null
        sleep "$STOP_WAIT_SECONDS"

        select_output=$(run_sql "SELECT * FROM ${CURRENT_TABLE} WHERE id = 1;")
        if echo "$select_output" | grep -q "seed"; then
            print_success "Round ${round}: seed row is still readable during ${victim} outage."
        else
            record_failure "Round ${round}: seed row became unavailable during ${victim} outage."
        fi

        if run_sql "INSERT INTO ${CURRENT_TABLE} VALUES (${id}, '${payload}');" >/dev/null; then
            print_success "Round ${round}: write succeeded while ${victim} was down."
        else
            record_failure "Round ${round}: write failed while ${victim} was down."
        fi

        print_info "Round ${round}: restarting ${victim}"
        docker start "$victim" >/dev/null
        if ! wait_for_health "$victim" "$RECOVERY_WAIT_SECONDS"; then
            record_failure "Round ${round}: ${victim} did not become healthy after restart."
            continue
        fi
        sleep 5

        if verify_name_present "$CURRENT_TABLE" "$payload"; then
            print_success "Round ${round}: data written during outage remains queryable after ${victim} recovery."
        else
            record_failure "Round ${round}: missing row '${payload}' after ${victim} recovery."
        fi
    done

    cleanup_test_table
}

function test_network_partition() {
    print_section "Scenario 4: Basic RegionServer Network Partition"
    cleanup_test_table

    local left="rs-1"
    local right="rs-2"
    local write_id=2
    local write_name="during_partition"
    local write_output
    local left_status
    local right_status

    if ! create_test_table "network_partition"; then
        record_failure "Failed to prepare test table for network partition scenario."
        return 1
    fi

    if ! apply_partition "$left" "$right"; then
        cleanup_test_table
        return 1
    fi

    sleep 2
    write_output=$(run_sql "INSERT INTO ${CURRENT_TABLE} VALUES (${write_id}, '${write_name}');" || true)
    echo "$write_output" >> "$CHAOS_LOG_FILE"

    if echo "$write_output" | grep -Eq "Error|TIMEOUT|ABORT|FAILED"; then
        print_info "Partitioned write returned a failure/timeout signal, which is acceptable in the basic scenario."
    elif echo "$write_output" | grep -Eq "affected|OK|SUCCESS"; then
        print_info "Partitioned write returned success; recovery will verify whether the row converges."
    else
        print_info "Partitioned write returned an ambiguous response; result archived for later inspection."
    fi

    sleep "$PARTITION_DURATION_SECONDS"

    left_status=$(capture_rs_status "$left" || true)
    right_status=$(capture_rs_status "$right" || true)
    printf "%s\n" "$left_status" >> "$CHAOS_LOG_FILE"
    printf "%s\n" "$right_status" >> "$CHAOS_LOG_FILE"

    if echo "$left_status$right_status" | grep -q "suspectedReplicaCount"; then
        print_success "Partition status capture includes suspected replica metrics."
    else
        record_failure "Partition status capture did not include suspected replica metrics."
    fi

    if echo "$left_status$right_status" | grep -Eq "transport_error|suspectedReplicaCount\"[:][1-9]"; then
        print_success "Partition status shows transport/suspected signals during split."
    else
        print_info "No transport/suspected signal observed in status payload during this run."
    fi

    clear_partition "$left" "$right"
    sleep "$RECOVERY_WAIT_SECONDS"

    if verify_name_present "$CURRENT_TABLE" "seed"; then
        print_success "Seed row remains readable after partition recovery."
    else
        record_failure "Seed row is missing after partition recovery."
    fi

    local final_output
    final_output=$(run_sql "SELECT * FROM ${CURRENT_TABLE};")
    echo "$final_output" >> "$CHAOS_LOG_FILE"
    if echo "$write_output" | grep -Eq "affected|OK|SUCCESS"; then
        if echo "$final_output" | grep -q "$write_name"; then
            print_success "Write acknowledged during partition is readable after recovery."
        else
            record_failure "Write was acknowledged during partition but is not readable after recovery."
        fi
    else
        print_info "Partitioned write was not acknowledged; final data snapshot archived for manual inspection."
    fi

    cleanup_test_table
}

function run_all() {
    test_master_failover
    test_rs_crash_and_recovery
    test_random_rs_crash
    test_network_partition
}

check_command docker
check_command curl

ensure_cluster_ready

case "${1:-all}" in
    master_failover)
        test_master_failover
        ;;
    rs_crash)
        test_rs_crash_and_recovery
        ;;
    random_rs_crash)
        test_random_rs_crash
        ;;
    network_partition)
        test_network_partition
        ;;
    all)
        run_all
        ;;
    *)
        print_error "Unknown scenario: ${1}"
        print_info "Supported scenarios: master_failover | rs_crash | random_rs_crash | network_partition | all"
        exit 1
        ;;
esac

if [ "$FAILURES" -gt 0 ]; then
    print_error "Chaos testing completed with ${FAILURES} failure(s)."
    exit 1
fi

print_success "Chaos testing completed successfully."
print_info "Chaos log archived at $CHAOS_LOG_FILE"
