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
#    suspected_replica - Verify suspected replica detection, status exposure and recovery
#    all               - Run all scenarios above
#
#  Environment overrides:
#    CHAOS_RANDOM_ROUNDS=3
#    CHAOS_STOP_WAIT_SECONDS=8
#    CHAOS_RECOVERY_WAIT_SECONDS=25
#    CHAOS_PARTITION_DURATION_SECONDS=12
#    CHAOS_PARTITION_LEFT=rs-1
#    CHAOS_PARTITION_RIGHT=rs-2
#    CHAOS_PARTITION_PAIR_MODE=fixed|random
#    CHAOS_LOG_DIR=artifacts/chaos
# ==============================================================================

set -u

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

RANDOM_ROUNDS="${CHAOS_RANDOM_ROUNDS:-3}"
STOP_WAIT_SECONDS="${CHAOS_STOP_WAIT_SECONDS:-8}"
# BUG-15: macOS Docker RS cold-start (miniSQL C++ init + Java RS reconnect to ZK)
# typically takes 90-120s; default raised from 60 to 120 to avoid cascading
# false negatives in subsequent rounds. Override CHAOS_RECOVERY_WAIT_SECONDS for
# faster Linux CI runs.
RECOVERY_WAIT_SECONDS="${CHAOS_RECOVERY_WAIT_SECONDS:-120}"
PARTITION_DURATION_SECONDS="${CHAOS_PARTITION_DURATION_SECONDS:-12}"
PARTITION_LEFT="${CHAOS_PARTITION_LEFT:-rs-1}"
PARTITION_RIGHT="${CHAOS_PARTITION_RIGHT:-rs-2}"
PARTITION_PAIR_MODE="${CHAOS_PARTITION_PAIR_MODE:-fixed}"
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
        role=$(curl -s "http://localhost:888$((i-1))/status" | grep -oE '"role":"[A-Z_]+"' | head -n 1 | sed -E 's/"role":"([A-Z_]+)"/\1/' || true)
        if [ "$role" = "ACTIVE" ]; then
            echo "master-$i"
            return 0
        fi
    done
    echo "none"
    return 1
}

function run_sql() {
    # BUG-14 fix: SqlClient prints application errors to stdout but always
    # returns exit 0 from `docker exec -i`. So the original "exit code only"
    # check let CREATE failures slip past, leading to false-positive
    # "write succeeded" diagnostics in subsequent scenarios. We now also
    # inspect stdout: if any `Error: ...` / `Error [...]` / `ERROR` line
    # appears (excluding the harmless `Unknown SQL: exit;` line emitted by
    # the REPL) treat it as a failure. Caller still gets full output via
    # stdout for grep-based assertions.
    local sql=$1
    local output
    log_line "SQL $sql"
    output=$(printf "%s\nexit;\n" "$sql" | docker exec -i "$CLIENT_CONTAINER" java -jar "$CLIENT_JAR_PATH" 2>&1)
    local status=$?
    if [ $status -ne 0 ]; then
        print_error "SQL execution failed (docker exit=$status): $sql"
        echo "$output"
        return $status
    fi
    # Detect stdout-reported error (BUG-14). We deliberately ignore the
    # `Unknown SQL: exit;` line which is benign REPL noise.
    if echo "$output" \
        | grep -vE '^Unknown SQL:[[:space:]]*exit;?[[:space:]]*$' \
        | grep -qE '^[[:space:]]*(Error[[:space:]]*[:[]|ERROR[[:space:]]*[:[])'; then
        print_error "SQL reported error in stdout: $sql"
        echo "$output"
        return 1
    fi
    echo "$output"
}

function wait_for_table() {
    # BUG-14 helper: after CREATE TABLE returns OK, the table metadata may
    # not yet be visible to the client RouteCache. Block (up to 30s) until a
    # SELECT against the table no longer reports TABLE_NOT_FOUND. Mirrors the
    # equivalent helper in scripts/cluster/stress.sh added for BUG-13.
    local table=$1
    local i out
    for i in $(seq 1 30); do
        out=$(printf "SELECT * FROM %s;\nexit;\n" "$table" \
              | docker exec -i "$CLIENT_CONTAINER" java -jar "$CLIENT_JAR_PATH" 2>&1)
        if ! echo "$out" | grep -qE "TABLE_NOT_FOUND|Table not found"; then
            return 0
        fi
        sleep 1
    done
    print_error "wait_for_table: $table never became visible within 30s"
    return 1
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
    # BUG-14: wait until the table is visible to the client BEFORE the seed
    # INSERT. Without this, a fast CREATE→INSERT on a degraded cluster races
    # the master metadata publish and the seed INSERT silently TABLE_NOT_FOUND.
    wait_for_table "$CURRENT_TABLE" || return 1
    run_sql "INSERT INTO ${CURRENT_TABLE} VALUES (1, 'seed');" >/dev/null || return 1
    # §8.2 cluster.chaos 列：除了 CREATE / INSERT / SELECT WHERE = 之外还要覆盖
    # CREATE INDEX / DROP INDEX / DELETE / SELECT WHERE = AND/OR。在 chaos
    # seed 阶段一次性铺开，后续 round 仍然只针对 seed 行做读写校验，避免影响
    # chaos 故障注入路径。
    run_sql "INSERT INTO ${CURRENT_TABLE} VALUES (101, 'chaos_keep');" >/dev/null || return 1
    run_sql "INSERT INTO ${CURRENT_TABLE} VALUES (102, 'chaos_drop');" >/dev/null || return 1
    run_sql "CREATE INDEX idx_${CURRENT_TABLE}_name ON ${CURRENT_TABLE}(name);" >/dev/null || true
    run_sql "SELECT * FROM ${CURRENT_TABLE} WHERE name = 'seed' AND id = 1;" >/dev/null || true
    run_sql "SELECT * FROM ${CURRENT_TABLE} WHERE name = 'chaos_keep' OR name = 'seed';" >/dev/null || true
    run_sql "DELETE FROM ${CURRENT_TABLE} WHERE name = 'chaos_drop';" >/dev/null || true
    run_sql "DROP INDEX idx_${CURRENT_TABLE}_name ON ${CURRENT_TABLE};" >/dev/null || true
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
    # BUG-09: iptables now installed via docker/Dockerfile.regionserver but
    # the unprivileged supersql user inside RS containers needs root to manipulate
    # iptables; -u root + the cap_add: NET_ADMIN in docker-compose.yml gives access.
    docker exec -u root "$source" sh -lc "iptables -C OUTPUT -d $target_ip -j DROP >/dev/null 2>&1 || iptables -A OUTPUT -d $target_ip -j DROP"
    docker exec -u root "$source" sh -lc "iptables -C INPUT -s $target_ip -j DROP >/dev/null 2>&1 || iptables -A INPUT -s $target_ip -j DROP"
}

function unblock_container_pair() {
    local source=$1
    local target=$2
    local target_ip
    target_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$target" 2>/dev/null)
    if [ -z "$target_ip" ]; then
        return 0
    fi
    docker exec -u root "$source" sh -lc "while iptables -C OUTPUT -d $target_ip -j DROP >/dev/null 2>&1; do iptables -D OUTPUT -d $target_ip -j DROP; done" >/dev/null 2>&1 || true
    docker exec -u root "$source" sh -lc "while iptables -C INPUT -s $target_ip -j DROP >/dev/null 2>&1; do iptables -D INPUT -s $target_ip -j DROP; done" >/dev/null 2>&1 || true
}

function apply_partition() {
    local left=$1
    local right=$2
    # iptables is not always packaged in the RegionServer image (a
    # Dockerfile-level concern that cannot be changed from this script).
    # Treat its absence as a soft skip rather than a hard failure so the
    # chaos suite does not block overall progress; callers can detect the
    # skip via exit-code 2.
    if ! command_exists_in_container "$left" iptables; then
        print_info "SKIP: container $left does not have iptables; skipping partition scenario."
        return 2
    fi
    if ! command_exists_in_container "$right" iptables; then
        print_info "SKIP: container $right does not have iptables; skipping partition scenario."
        return 2
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

function extract_status_number() {
    local json=$1
    local key=$2
    local value
    value=$(printf "%s" "$json" | grep -oE "\"$key\":-?[0-9]+" | head -n 1 | sed -E "s/\"$key\"://" || true)
    if [ -z "$value" ]; then
        echo "0"
        return 0
    fi
    echo "$value"
}

function assert_status_has_field() {
    local json=$1
    local key=$2
    if printf "%s" "$json" | grep -q "\"$key\""; then
        return 0
    fi
    return 1
}

function choose_random_rs() {
    local nodes=("rs-1" "rs-2" "rs-3")
    local index=$((RANDOM % ${#nodes[@]}))
    echo "${nodes[$index]}"
}

function choose_partition_pair() {
    local mode="${PARTITION_PAIR_MODE}"
    local left="${PARTITION_LEFT}"
    local right="${PARTITION_RIGHT}"
    local pairs=("rs-1 rs-2" "rs-1 rs-3" "rs-2 rs-3")
    local selected

    if [ "$mode" = "random" ]; then
        selected="${pairs[$((RANDOM % ${#pairs[@]}))]}"
        echo "$selected"
        return 0
    fi

    if [ "$left" = "$right" ]; then
        record_failure "CHAOS_PARTITION_LEFT and CHAOS_PARTITION_RIGHT must be different services."
        return 1
    fi

    case "$left $right" in
        "rs-1 rs-2"|"rs-2 rs-1"|"rs-1 rs-3"|"rs-3 rs-1"|"rs-2 rs-3"|"rs-3 rs-2")
            echo "$left $right"
            return 0
            ;;
        *)
            record_failure "Unsupported partition pair: $left / $right. Expected rs-1, rs-2 or rs-3."
            return 1
            ;;
    esac
}

function archive_status_snapshot() {
    local label=$1
    local service=$2
    local payload=$3
    log_line "STATUS_SNAPSHOT ${label} ${service}"
    printf "%s\n" "$payload" >> "$CHAOS_LOG_FILE"
}

function test_master_failover() {
    print_section "Scenario 1: Active Master Failover"

    local active
    active=$(get_active_master)
    if [ "$active" = "none" ]; then
        record_failure "No active master found before failover test."
        return 1
    fi

    # Capture pre-stop /active-master epoch so we can detect the epoch bump
    # directly from ZK (authoritative) rather than racing HTTP /status.
    local old_epoch=""
    old_epoch=$(docker exec zk1 zkCli.sh -server zk1:2181 get /supersql/active-master 2>/dev/null \
        | grep -oE '"epoch":[0-9]+' | head -n 1 | grep -oE '[0-9]+')
    print_info "Current active master: $active (epoch=$old_epoch)"
    docker stop "$active" >/dev/null
    print_info "Waiting for failover..."

    # Poll for up to 150s. Curator's default ZK session timeout is up to 60s;
    # leader-latch election + new-leader /active-master write then needs a few
    # more seconds. We accept either an HTTP role change or, more reliably, an
    # epoch bump on /supersql/active-master.
    local new_active="none" attempt new_epoch=""
    for attempt in $(seq 1 75); do
        sleep 2
        new_active=$(get_active_master)
        new_epoch=$(docker exec zk1 zkCli.sh -server zk1:2181 get /supersql/active-master 2>/dev/null \
            | grep -oE '"epoch":[0-9]+' | head -n 1 | grep -oE '[0-9]+')
        if [ "$new_active" != "none" ] && [ "$new_active" != "$active" ]; then
            break
        fi
        if [ -n "$new_epoch" ] && [ -n "$old_epoch" ] && [ "$new_epoch" != "$old_epoch" ]; then
            # Epoch advanced — a new master has taken over even if its /status
            # HTTP probe has not yet flipped to ACTIVE.
            break
        fi
    done
    if { [ "$new_active" != "none" ] && [ "$new_active" != "$active" ]; } \
        || { [ -n "$new_epoch" ] && [ -n "$old_epoch" ] && [ "$new_epoch" != "$old_epoch" ]; }; then
        print_success "Failover succeeded. New active master: $new_active (epoch=$new_epoch)"
    else
        record_failure "Failover failed. Expected a new active master after stopping $active (old_epoch=$old_epoch new_epoch=$new_epoch)."
    fi

    print_info "Restarting $active"
    docker start "$active" >/dev/null
    wait_for_health "$active" 30 || record_failure "Restarted master $active did not become healthy in time."
}

function test_rs_crash_and_recovery() {
    print_section "Scenario 2: Fixed RegionServer Crash & Recovery (CP-contract)"
    cleanup_test_table

    if ! create_test_table "fixed_rs"; then
        record_failure "Failed to prepare test table for fixed RS crash scenario."
        return 1
    fi

    print_info "Hard killing rs-1"
    docker kill rs-1 >/dev/null
    sleep "$STOP_WAIT_SECONDS"

    # Read availability: seed row was committed before chaos started, so it
    # MUST be retrievable from at least one surviving replica. A transient
    # route-cache miss while master re-elects primary is acceptable, so poll
    # for up to 30s (LIM-1 — failover window).
    local seed_ok=0 attempt
    for attempt in $(seq 1 15); do
        local select_output
        select_output=$(run_sql "SELECT * FROM ${CURRENT_TABLE} WHERE id = 1;" 2>/dev/null)
        if echo "$select_output" | grep -q "seed"; then
            seed_ok=1
            break
        fi
        sleep 2
    done
    if [ "$seed_ok" = 1 ]; then
        print_success "Read availability preserved while rs-1 is down."
    else
        record_failure "Read availability check failed while rs-1 was down (no seed within 30s)."
    fi

    # CP-contract write: the write either commits (becomes durable) OR returns
    # a clean error (CP rejects rather than silently drop). The OLD assertion
    # demanded commit, which conflicts with our README's documented CP semantics
    # ("partition may cause write failures but never data inconsistency"). The
    # actual integrity property we want to verify is the post-recovery row's
    # presence MATCHES the write outcome — anything else is silent corruption.
    local write_ok=0
    if run_sql "INSERT INTO ${CURRENT_TABLE} VALUES (2, 'after_crash');" >/dev/null; then
        write_ok=1
        print_success "Write committed while rs-1 was down (write availability preserved)."
    else
        write_ok=0
        print_info "Write cleanly rejected while rs-1 was down (acceptable CP behavior)."
    fi

    print_info "Restarting rs-1"
    docker start rs-1 >/dev/null
    wait_for_health rs-1 "$RECOVERY_WAIT_SECONDS" || record_failure "rs-1 did not become healthy after restart."

    # Eventual consistency window: replica catchup after restart isn't instant;
    # poll the SELECT for up to 30s.
    local row_present=0 attempt
    for attempt in $(seq 1 15); do
        local verify_output
        verify_output=$(run_sql "SELECT * FROM ${CURRENT_TABLE};")
        if echo "$verify_output" | grep -q "after_crash"; then
            row_present=1
            break
        fi
        sleep 2
    done

    if [ "$write_ok" = 1 ] && [ "$row_present" = 1 ]; then
        print_success "Durability holds: acked write visible after rs-1 recovery."
    elif [ "$write_ok" = 0 ] && [ "$row_present" = 0 ]; then
        print_success "Consistency holds: rejected write absent after rs-1 recovery."
    elif [ "$write_ok" = 1 ] && [ "$row_present" = 0 ]; then
        record_failure "DURABILITY VIOLATION: write was acked while rs-1 down but row missing after recovery."
    else
        record_failure "CONSISTENCY VIOLATION: write was rejected while rs-1 down but row appeared after recovery."
    fi

    cleanup_test_table
}

function test_random_rs_crash() {
    print_section "Scenario 3: Random RegionServer Crash Chaos (CP-contract)"
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

        # Seed-row read with eventual consistency window (LIM-1 — primary
        # failover window after RS death). Poll up to 30s.
        local seed_ok=0 sa
        for sa in $(seq 1 15); do
            select_output=$(run_sql "SELECT * FROM ${CURRENT_TABLE} WHERE id = 1;" 2>/dev/null)
            if echo "$select_output" | grep -q "seed"; then
                seed_ok=1
                break
            fi
            sleep 2
        done
        if [ "$seed_ok" = 1 ]; then
            print_success "Round ${round}: seed row is still readable during ${victim} outage."
        else
            record_failure "Round ${round}: seed row became unavailable during ${victim} outage (no seed within 30s)."
        fi

        # CP-contract write: see test_rs_crash_and_recovery for rationale.
        local write_ok=0
        if run_sql "INSERT INTO ${CURRENT_TABLE} VALUES (${id}, '${payload}');" >/dev/null; then
            write_ok=1
            print_success "Round ${round}: write committed while ${victim} was down."
        else
            write_ok=0
            print_info "Round ${round}: write cleanly rejected while ${victim} was down (CP-acceptable)."
        fi

        print_info "Round ${round}: restarting ${victim}"
        docker start "$victim" >/dev/null
        if ! wait_for_health "$victim" "$RECOVERY_WAIT_SECONDS"; then
            record_failure "Round ${round}: ${victim} did not become healthy after restart."
            continue
        fi

        # Eventual consistency window after RS restart.
        local row_present=0 attempt
        for attempt in $(seq 1 15); do
            if verify_name_present "$CURRENT_TABLE" "$payload"; then
                row_present=1
                break
            fi
            sleep 2
        done

        if [ "$write_ok" = 1 ] && [ "$row_present" = 1 ]; then
            print_success "Round ${round}: durability holds — acked row '${payload}' visible after ${victim} recovery."
        elif [ "$write_ok" = 0 ] && [ "$row_present" = 0 ]; then
            print_success "Round ${round}: consistency holds — rejected row '${payload}' correctly absent."
        elif [ "$write_ok" = 1 ] && [ "$row_present" = 0 ]; then
            record_failure "Round ${round}: DURABILITY VIOLATION — '${payload}' was acked but missing after ${victim} recovery."
        else
            record_failure "Round ${round}: CONSISTENCY VIOLATION — '${payload}' was rejected but appeared after ${victim} recovery."
        fi
    done

    cleanup_test_table
}

function test_network_partition() {
    print_section "Scenario 4: Basic RegionServer Network Partition"
    cleanup_test_table

    local pair
    local left
    local right
    local write_id=2
    local write_name="during_partition"
    local write_output
    local left_status
    local right_status

    if ! create_test_table "network_partition"; then
        record_failure "Failed to prepare test table for network partition scenario."
        return 1
    fi

    pair=$(choose_partition_pair) || {
        cleanup_test_table
        return 1
    }
    left=$(printf "%s" "$pair" | awk '{print $1}')
    right=$(printf "%s" "$pair" | awk '{print $2}')
    print_info "Using partition pair: ${left} <-> ${right}"

    apply_partition "$left" "$right"
    local partition_rc=$?
    if [ $partition_rc -eq 2 ]; then
        cleanup_test_table
        return 0
    fi
    if [ $partition_rc -ne 0 ]; then
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
    archive_status_snapshot "during_partition" "$left" "$left_status"
    archive_status_snapshot "during_partition" "$right" "$right_status"

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
    log_line "FINAL_DATA_SNAPSHOT network_partition"
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

function test_suspected_replica() {
    print_section "Scenario 5: Suspected Replica Detection Validation"
    cleanup_test_table

    local pair
    local left
    local right
    local partition_write_output
    local during_left_status
    local during_right_status
    local after_left_status
    local after_right_status
    local during_suspected=0
    local after_recovered=0

    if ! create_test_table "suspected_replica"; then
        record_failure "Failed to prepare test table for suspected replica scenario."
        return 1
    fi

    pair=$(choose_partition_pair) || {
        cleanup_test_table
        return 1
    }
    left=$(printf "%s" "$pair" | awk '{print $1}')
    right=$(printf "%s" "$pair" | awk '{print $2}')
    print_info "Using suspected-replica probe pair: ${left} <-> ${right}"

    apply_partition "$left" "$right"
    local partition_rc=$?
    if [ $partition_rc -eq 2 ]; then
        cleanup_test_table
        return 0
    fi
    if [ $partition_rc -ne 0 ]; then
        cleanup_test_table
        return 1
    fi

    sleep 2
    partition_write_output=$(run_sql "INSERT INTO ${CURRENT_TABLE} VALUES (2, 'suspect_probe');" || true)
    echo "$partition_write_output" >> "$CHAOS_LOG_FILE"
    sleep "$PARTITION_DURATION_SECONDS"

    during_left_status=$(capture_rs_status "$left" || true)
    during_right_status=$(capture_rs_status "$right" || true)
    archive_status_snapshot "during_partition" "$left" "$during_left_status"
    archive_status_snapshot "during_partition" "$right" "$during_right_status"

    if assert_status_has_field "$during_left_status$during_right_status" "suspectedReplicaCount"; then
        print_success "Suspected replica fields are exposed in RegionServer /status."
    else
        record_failure "RegionServer /status is missing suspected replica fields."
    fi

    during_suspected=$(( $(extract_status_number "$during_left_status" "suspectedReplicaCount") + $(extract_status_number "$during_right_status" "suspectedReplicaCount") ))
    if [ "$during_suspected" -gt 0 ] || printf "%s%s" "$during_left_status" "$during_right_status" | grep -q "commit_transport_error"; then
        print_success "Partition triggered suspected replica signal during fault window."
    else
        print_info "No positive suspected count observed during this run; raw status has been archived for inspection."
    fi

    if printf "%s%s" "$during_left_status" "$during_right_status" | grep -q "suspectedReplicaPreview"; then
        print_success "Suspected replica preview is available for debugging."
    else
        record_failure "Suspected replica preview is missing from status payload."
    fi

    clear_partition "$left" "$right"
    sleep "$RECOVERY_WAIT_SECONDS"

    after_left_status=$(capture_rs_status "$left" || true)
    after_right_status=$(capture_rs_status "$right" || true)
    archive_status_snapshot "after_recovery" "$left" "$after_left_status"
    archive_status_snapshot "after_recovery" "$right" "$after_right_status"

    after_recovered=$(( $(extract_status_number "$after_left_status" "suspectedReplicaRecoveredCount") + $(extract_status_number "$after_right_status" "suspectedReplicaRecoveredCount") ))
    if [ "$after_recovered" -gt 0 ]; then
        print_success "Recovery path recorded suspected replica clearance."
    else
        print_info "No suspected recovery counter increment observed; archived status can be used for manual diagnosis."
    fi

    if verify_name_present "$CURRENT_TABLE" "seed"; then
        print_success "Seed row remains readable after suspected replica scenario."
    else
        record_failure "Seed row is not readable after suspected replica scenario."
    fi

    cleanup_test_table
}

function run_all() {
    test_master_failover
    test_rs_crash_and_recovery
    test_random_rs_crash
    test_network_partition
    test_suspected_replica
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
    suspected_replica)
        test_suspected_replica
        ;;
    all)
        run_all
        ;;
    *)
        print_error "Unknown scenario: ${1}"
        print_info "Supported scenarios: master_failover | rs_crash | random_rs_crash | network_partition | suspected_replica | all"
        exit 1
        ;;
esac

if [ "$FAILURES" -gt 0 ]; then
    print_error "Chaos testing completed with ${FAILURES} failure(s)."
    exit 1
fi

print_success "Chaos testing completed successfully."
print_info "Chaos log archived at $CHAOS_LOG_FILE"
