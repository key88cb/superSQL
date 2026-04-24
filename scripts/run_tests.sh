#!/usr/bin/env bash
# =============================================================================
#  SuperSQL 统一测试入口
#
#  使用方式：
#     scripts/run_tests.sh <layer> <suite> [options...]
#     scripts/run_tests.sh all                # 按 docs/TEST_PLAN.md §5 顺序执行
#
#  layer × suite 列表参见 docs/TEST_STATUS.md §4 与 docs/TEST_PLAN.md §3。
#
#  每个套件产物：
#     artifacts/<suite>/<YYYYMMDD-HHMMSS>/stdout.log
#     artifacts/<suite>/<YYYYMMDD-HHMMSS>/stderr.log
#     artifacts/<suite>/<YYYYMMDD-HHMMSS>/summary.json
#
#  退出码：
#     0  套件 / 总体通过
#     1  套件失败（summary.json.result == FAIL）
#     2  前置条件缺失（docker 未启、pom 未 install）
#     3  参数错误
# =============================================================================

set -u
set -o pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

ARTIFACTS_ROOT="${ARTIFACTS_ROOT:-$REPO_ROOT/artifacts}"
TS="$(date +%Y%m%d-%H%M%S)"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info()  { printf "%b[INFO]%b %s\n" "$YELLOW" "$NC" "$*"; }
log_pass()  { printf "%b[PASS]%b %s\n" "$GREEN"  "$NC" "$*"; }
log_fail()  { printf "%b[FAIL]%b %s\n" "$RED"    "$NC" "$*"; }

_json_escape() {
    python3 -c 'import json,sys;print(json.dumps(sys.stdin.read()))' 2>/dev/null || {
        # fallback: 把换行替换成 \n，双引号转义
        awk 'BEGIN{ORS=""} {gsub(/\\/,"\\\\"); gsub(/"/,"\\\""); gsub(/\n/,"\\n"); print}' <<<"$1"
    }
}

write_summary() {
    # $1 suite  $2 result  $3 started_at  $4 finished_at  $5 notes
    local suite="$1" result="$2" started_at="$3" finished_at="$4" notes="$5"
    local dir="$ARTIFACTS_ROOT/$suite/$TS"
    mkdir -p "$dir"
    cat >"$dir/summary.json" <<EOF
{
  "suite": "$suite",
  "result": "$result",
  "startedAt": "$started_at",
  "finishedAt": "$finished_at",
  "notes": "$notes",
  "artifacts": {
    "stdout": "stdout.log",
    "stderr": "stderr.log"
  }
}
EOF
    echo "$dir"
}

run_and_capture() {
    # $1 suite name     $2 command (invoked via bash -c)
    local suite="$1" cmd="$2"
    local dir="$ARTIFACTS_ROOT/$suite/$TS"
    mkdir -p "$dir"
    local started finished rc
    started="$(date -Iseconds)"
    log_info "[$suite] start → $dir"
    bash -c "$cmd" >"$dir/stdout.log" 2>"$dir/stderr.log"
    rc=$?
    finished="$(date -Iseconds)"
    local result="PASS"
    [[ $rc -ne 0 ]] && result="FAIL"
    write_summary "$suite" "$result" "$started" "$finished" "exit=$rc" >/dev/null
    if [[ $rc -eq 0 ]]; then
        log_pass "[$suite] exit=0 → $dir"
    else
        log_fail "[$suite] exit=$rc → $dir"
    fi
    return $rc
}

# ---------- 前置检查 ----------
precheck_cpp() {
    command -v g++ >/dev/null 2>&1 || { log_fail "g++ not found"; exit 2; }
}

precheck_java() {
    command -v mvn >/dev/null 2>&1 || { log_fail "mvn not found"; exit 2; }
    # 保证所有模块已 install，避免跨模块解析失败
    if ! [ -d "$HOME/.m2/repository/edu/zju/supersql/java-regionserver" ]; then
        log_info "首次运行：mvn install -DskipTests"
        mvn -q -B -ntp install -DskipTests || { log_fail "mvn install failed"; exit 2; }
    fi
}

precheck_cluster() {
    command -v docker >/dev/null 2>&1 || { log_fail "docker not found"; exit 2; }
    docker compose ps >/dev/null 2>&1 || { log_fail "docker compose not usable"; exit 2; }
    local running
    running=$(docker compose ps --status running -q | wc -l | tr -d ' ')
    if [[ "$running" -lt 10 ]]; then
        log_fail "cluster not fully up (running=$running, expected 10). 先执行 docker compose up -d --build"
        exit 2
    fi
}

# ---------- C++ ----------
suite_cpp_unit() {
    precheck_cpp
    local cmd='cd minisql/cpp-core && make clear >/dev/null 2>&1; make -j test_basic test_buffer_manager test_catalog_manager test_index_manager test_record_manager test_api'
    run_and_capture cpp.unit "$cmd"
}

suite_cpp_wal() {
    precheck_cpp
    local cmd='cd minisql/cpp-core && make clear >/dev/null 2>&1; make test_wal test_wal_crash_recovery'
    run_and_capture cpp.wal "$cmd"
}

suite_cpp_stress() {
    precheck_cpp
    local cmd='cd minisql/cpp-core && make clear >/dev/null 2>&1; make test_exhaustive && g++ -o test_pin_count -Wall -O2 tests/test_pin_count.cc api.cc record_manager.cc index_manager.cc catalog_manager.cc buffer_manager.cc log_manager.cc basic.cc && ./test_pin_count'
    run_and_capture cpp.stress "$cmd"
}

suite_cpp_coverage() {
    precheck_cpp
    local cmd='cd minisql/cpp-core && make clear_coverage >/dev/null 2>&1; make coverage'
    run_and_capture cpp.coverage "$cmd"
    local outdir="$ARTIFACTS_ROOT/cpp.coverage/$TS"
    mkdir -p "$outdir/coverage"
    cp minisql/cpp-core/*.gcov "$outdir/coverage/" 2>/dev/null || true
}

# ---------- Java ----------
suite_java_unit() {
    precheck_java
    # 通过 test pattern 排除 *IntegrationTest
    local cmd='mvn -q -B -ntp test -Dtest="!*IntegrationTest" -Dsurefire.failIfNoSpecifiedTests=false'
    run_and_capture java.unit "$cmd"
    collect_surefire java.unit
}

suite_java_integration() {
    precheck_java
    local cmd='mvn -q -B -ntp test -Dtest="*IntegrationTest" -Dsurefire.failIfNoSpecifiedTests=false'
    run_and_capture java.integration "$cmd"
    collect_surefire java.integration
}

suite_java_full() {
    precheck_java
    local cmd='mvn -B -ntp test'
    run_and_capture java.full "$cmd"
    collect_surefire java.full
}

collect_surefire() {
    local suite="$1"
    local dst="$ARTIFACTS_ROOT/$suite/$TS/surefire-reports"
    mkdir -p "$dst"
    find . -type d -name surefire-reports -not -path "*/artifacts/*" 2>/dev/null | while read -r d; do
        local mod
        mod="$(basename "$(dirname "$(dirname "$d")")")"
        mkdir -p "$dst/$mod"
        cp -f "$d"/*.xml "$d"/*.txt "$dst/$mod/" 2>/dev/null || true
    done
}

# ---------- 集群 ----------
suite_cluster_smoke() {
    precheck_cluster
    local cmd='bash scripts/cluster/smoke.sh'
    run_and_capture cluster.smoke "$cmd"
}

suite_cluster_chaos() {
    precheck_cluster
    local cmd='bash scripts/chaos_test.sh all'
    run_and_capture cluster.chaos "$cmd"
    # 混沌后必须回跑一次 smoke
    suite_cluster_smoke || log_fail "chaos 后 smoke 未恢复"
}

suite_cluster_stress() {
    precheck_cluster
    local cmd='bash scripts/cluster/stress.sh'
    run_and_capture cluster.stress "$cmd"
}

suite_zk_topology() {
    precheck_cluster
    local cmd='bash scripts/zk/assert_topology.sh'
    run_and_capture zk.topology "$cmd"
}

suite_e2e_client() {
    precheck_cluster
    local cmd='bash scripts/e2e/run_sql_fixtures.sh'
    run_and_capture e2e.client "$cmd"
}

# ---------- 总入口 ----------
run_all() {
    local failed=0
    for fn in \
        suite_cpp_unit suite_cpp_wal suite_cpp_stress suite_cpp_coverage \
        suite_java_unit suite_java_integration \
        suite_cluster_smoke suite_zk_topology suite_cluster_stress \
        suite_cluster_chaos suite_e2e_client ; do
        $fn || failed=$((failed + 1))
    done
    log_info "total failed suites: $failed"
    return $failed
}

usage() {
    cat <<EOF
Usage: $0 <layer> <suite>
   or: $0 all

Layers / suites:
   cpp      unit | wal | stress | coverage
   java     unit | integration | full
   cluster  smoke | chaos | stress
   zk       topology
   e2e      client

Environment overrides:
   ARTIFACTS_ROOT   (default: ./artifacts)
EOF
}

main() {
    if [[ $# -lt 1 ]]; then usage; exit 3; fi
    case "$1" in
        all)
            run_all
            ;;
        cpp)
            case "${2:-}" in
                unit)     suite_cpp_unit ;;
                wal)      suite_cpp_wal ;;
                stress)   suite_cpp_stress ;;
                coverage) suite_cpp_coverage ;;
                *) usage; exit 3 ;;
            esac
            ;;
        java)
            case "${2:-}" in
                unit)        suite_java_unit ;;
                integration) suite_java_integration ;;
                full)        suite_java_full ;;
                *) usage; exit 3 ;;
            esac
            ;;
        cluster)
            case "${2:-}" in
                smoke)  suite_cluster_smoke ;;
                chaos)  suite_cluster_chaos ;;
                stress) suite_cluster_stress ;;
                *) usage; exit 3 ;;
            esac
            ;;
        zk)
            case "${2:-}" in
                topology) suite_zk_topology ;;
                *) usage; exit 3 ;;
            esac
            ;;
        e2e)
            case "${2:-}" in
                client) suite_e2e_client ;;
                *) usage; exit 3 ;;
            esac
            ;;
        -h|--help) usage; exit 0 ;;
        *) usage; exit 3 ;;
    esac
}

main "$@"
