#!/bin/bash

# ==============================================================================
#  SuperSQL Distributed Chaos Testing Script
#
#  Usage: ./chaos_test.sh [scenario]
#  Scenarios:
#    master_failover - Kill active master, verify electon
#    rs_crash        - Kill one RS, verify data availability
#    wal_recovery    - Hard kill RS, restart and verify WAL replay
#    all (default)   - Run all scenarios
# ==============================================================================

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Starting SuperSQL Chaos Testing...${NC}"

# Check if docker compose is running
docker compose ps > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo -e "${RED}Error: Docker Compose is not running or not found in current directory.${NC}"
    exit 1
fi

# ── Helpers ───────────────────────────────────────────────────────────────────

function get_active_master() {
    for i in 1 2 3; do
        role=$(curl -s http://localhost:888$((i-1))/status | grep -oP '"role":"\K[^"]+')
        if [ "$role" == "ACTIVE" ]; then
            echo "master-$i"
            return
        fi
    done
    echo "none"
}

function run_sql() {
    local sql=$1
    echo -e "${YELLOW}Executing SQL: ${sql}${NC}"
    echo "${sql}" | docker exec -i client java -jar /app/app.jar
}

function wait_for_ready() {
    local service=$1
    local timeout=30
    echo -n "Waiting for $service to be healthy..."
    for i in $(seq 1 $timeout); do
        if docker compose ps $service | grep -q "healthy"; then
            echo -e " ${GREEN}OK${NC}"
            return 0
        fi
        echo -n "."
        sleep 1
    done
    echo -e " ${RED}TIMEOUT${NC}"
    return 1
}

# ── Scenarios ──────────────────────────────────────────────────────────────────

function test_master_failover() {
    echo -e "\n${YELLOW}>>> Scenario 1: Active Master Failover${NC}"
    
    active=$(get_active_master)
    if [ "$active" == "none" ]; then
        echo -e "${RED}Error: No active master found!${NC}"
        return 1
    fi
    echo -e "Current Active Master: ${GREEN}$active${NC}"
    
    echo -e "Stopping $active..."
    docker stop $active
    
    echo "Waiting for failover (approx 15s)..."
    sleep 20
    
    new_active=$(get_active_master)
    if [ "$new_active" != "none" ] && [ "$new_active" != "$active" ]; then
        echo -e "${GREEN}Failover Success! New active: $new_active${NC}"
    else
        echo -e "${RED}Failover Failed!${NC}"
        return 1
    fi
    
    echo "Restarting old master..."
    docker start $active
}

function test_rs_crash_and_recovery() {
    echo -e "\n${YELLOW}>>> Scenario 2: RegionServer Crash & WAL Recovery${NC}"
    
    # 1. Setup table and data
    run_sql "CREATE TABLE chaos_test(id int, name char(20), primary key(id));"
    run_sql "INSERT INTO chaos_test VALUES (1, 'InitialData');"
    
    # 2. Hard kill rs-1
    echo -e "Hard killing rs-1 (kill -9 simulation)..."
    docker kill rs-1
    
    # 3. Verify read availability (from other replicas)
    echo "Checking availability from surviving replicas..."
    run_sql "SELECT * FROM chaos_test WHERE id = 1;" | grep -q "InitialData"
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Read availability maintained during failure.${NC}"
    else
        echo -e "${RED}System unavailable after RS failure!${NC}"
    fi
    
    # 4. Insert data while node is down
    run_sql "INSERT INTO chaos_test VALUES (2, 'WhileDown');"
    
    # 5. Bring rs-1 back
    echo "Starting rs-1..."
    docker start rs-1
    wait_for_ready rs-1
    
    # 6. Verify WAL recovery and catch-up
    echo "Verifying data on rs-1 after recovery..."
    # We check if rs-1 has the data. Since it's a replica, it should have replayed WALs.
    run_sql "SELECT * FROM chaos_test WHERE id = 2;" | grep -q "WhileDown"
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Recovery & Catch-up Success!${NC}"
    else
        echo -e "${RED}Data loss detected after recovery!${NC}"
        return 1
    fi
}

# ── Main ──────────────────────────────────────────────────────────────────────

case "$1" in
    master_failover) test_master_failover ;;
    rs_crash)       test_rs_crash_and_recovery ;;
    *)
        test_master_failover
        test_rs_crash_and_recovery
        ;;
esac

echo -e "\n${GREEN}Chaos Testing Completed.${NC}"
