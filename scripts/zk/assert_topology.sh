#!/usr/bin/env bash
# zk.topology — 断言 ZooKeeper 命名空间结构与 docs/CLAUDE.md 一致
#
# /supersql/masters              临时顺序节点，>=1（通常 3）
# /supersql/active-master        持久节点，JSON 含 epoch
# /supersql/region_servers       >=1（通常 3）
# /supersql/meta/tables          0..N（DDL 后会存在）
# /supersql/assignments          同 /supersql/meta/tables

set -u
set -o pipefail

ZK_CLI="${ZK_CLI:-docker exec zk1 zkCli.sh -server zk1:2181}"

fail() { echo "[FAIL] $*" >&2; exit 1; }
ok()   { echo "[PASS] $*"; }

zk_ls() {
    # $1 path — prints children count on success.
    # zkCli.sh in single-command mode emits log lines and a single listing
    # line starting with '[' that contains the children. Pick the FIRST
    # line that starts with '[' but is not an "[zk: ... (CONNECTED) N]" prompt.
    $ZK_CLI ls "$1" 2>/dev/null \
        | grep -E '^\[' \
        | grep -vE '^\[zk: ' \
        | head -n 1 \
        | tr -d '[]' | tr ',' '\n' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | sed '/^$/d' \
        | wc -l | tr -d ' '
}

zk_get() {
    # zkCli.sh 'get <path>' emits log lines, a WatchedEvent line, and the data
    # payload (and a stat block in older versions). Our payloads are always
    # JSON objects — take the first line that starts with '{'.
    $ZK_CLI get "$1" 2>/dev/null \
        | grep -E '^\{' \
        | head -n 1
}

# 1. /supersql/masters
masters_count=$(zk_ls /supersql/masters)
[[ "$masters_count" -ge 1 ]] || fail "/supersql/masters children=$masters_count"
ok "/supersql/masters children=$masters_count"

# 2. /supersql/active-master
active=$(zk_get /supersql/active-master)
[[ -n "$active" ]] || fail "/supersql/active-master empty"
echo "$active" | grep -q 'epoch' || fail "/supersql/active-master payload missing 'epoch': $active"
ok "/supersql/active-master payload=$active"

# 3. /supersql/region_servers
rs_count=$(zk_ls /supersql/region_servers)
[[ "$rs_count" -ge 1 ]] || fail "/supersql/region_servers children=$rs_count"
ok "/supersql/region_servers children=$rs_count"

# 4. /supersql/meta/tables — optional（无表时为空）
tables_count=$(zk_ls /supersql/meta/tables 2>/dev/null || echo 0)
ok "/supersql/meta/tables children=$tables_count (informational)"

# 5. /supersql/assignments — optional
assign_count=$(zk_ls /supersql/assignments 2>/dev/null || echo 0)
ok "/supersql/assignments children=$assign_count (informational)"

# 一致性：若 /supersql/meta/tables 非空，每张表应同时存在于 /supersql/assignments
if [[ "$tables_count" -gt 0 ]]; then
    $ZK_CLI ls /supersql/meta/tables 2>/dev/null | tail -n +1 | grep -oE '\[[^]]*\]' | tr -d '[]' | tr ',' '\n' | sed '/^$/d' | while read -r t; do
        t_trim=$(echo "$t" | tr -d ' ')
        [[ -z "$t_trim" ]] && continue
        $ZK_CLI get "/supersql/assignments/$t_trim" >/dev/null 2>&1 \
            || fail "/supersql/meta/tables/$t_trim exists but /supersql/assignments/$t_trim missing"
    done
    ok "meta ↔ assignments consistency"
fi

echo "[SUMMARY] zk.topology PASS"
